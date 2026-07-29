package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	// "strings"
	"time"
)

const wrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

type CommandHandler struct {
	store *Store
}

func (h CommandHandler) Handle(command Command) string {
	switch cmd := command.(type) {
	case Xread:
		return h.handleXread(cmd)
	case Ping:
		return "+PONG\r\n"
	case Echo:
		return BulkString(cmd.Message)
	case Get:
		return h.handleGet(cmd)
	case Type:
		return h.handleType(cmd)
	case Set:
		return h.handleSet(cmd)
	case RPush:
		return h.handleRPush(cmd)
	case LPush:
		return h.handleLPush(cmd)
	case LLen:
		return h.handleLLen(cmd)
	case LPop:
		return h.handleLPop(cmd)
	case RPop:
		return h.handleRPop(cmd)
	case LRange:
		return h.handleLRange(cmd)
	case BLPop:
		return h.handleBLPop(cmd)
	case Xadd:
		return h.handleXadd(cmd)
	case Xrange:
		return h.handleXrange(cmd)
	case Incr:
		return h.handleIncr(cmd)
	case Unknown:
		return fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd.Name)
	default:
		return "-ERR unknown command\r\n"
	}
}

func (h CommandHandler) handleGet(cmd Get) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists || (entry.Expiry != nil && entry.Expiry.Before(time.Now())) {
		if exists {
			delete(h.store.db, cmd.Key)
		}
		return "$-1\r\n"
	}
	if value, ok := entry.Value.(StringValue); ok {
		return BulkString(value.Value)
	}
	return wrongType
}

func (h CommandHandler) handleType(cmd Type) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists || (entry.Expiry != nil && entry.Expiry.Before(time.Now())) {
		if exists {
			delete(h.store.db, cmd.Key)
		}
		return "+none\r\n"
	}

	switch entry.Value.(type) {
	case StringValue:
		return "+string\r\n"
	case ListValue:
		return "+list\r\n"
	case StreamValue:
		return "+stream\r\n"
	default:
		return "+none\r\n"
	}
}

func (h CommandHandler) handleSet(cmd Set) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	h.store.db[cmd.Key] = Entry{Value: StringValue{Value: cmd.Value}, Expiry: cmd.Expiry}
	return "+OK\r\n"
}

func (h CommandHandler) handleRPush(cmd RPush) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		entry = Entry{Value: ListValue{}}
	}
	list, ok := entry.Value.(ListValue)
	if !ok {
		return wrongType
	}
	list.Values = append(list.Values, cmd.Values...)
	entry.Value = list
	h.store.db[cmd.Key] = entry
	h.notifyWaiters(cmd.Key)
	return fmt.Sprintf(":%d\r\n", len(list.Values))
}

func generateStreamID(requested string, last *StreamID) (StreamID, error) {

	if requested == "*" {
		now := time.Now().UnixMilli()

		if last != nil && now <= last.Millis {
			return StreamID{
				Millis: last.Millis,
				Seq:    last.Seq + 1,
			}, nil
		}

		return StreamID{
			Millis: now,
			Seq:    0,
		}, nil
	}

	parts := strings.SplitN(requested, "-", 2)
	if len(parts) != 2 {
		return StreamID{}, fmt.Errorf("invalid stream ID")
	}

	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid stream ID")
	}

	if parts[1] == "*" {
		if last != nil {
			if millis < last.Millis {
				return StreamID{}, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
			}

			if millis == last.Millis {
				return StreamID{
					Millis: millis,
					Seq:    last.Seq + 1,
				}, nil
			}
		}

		sequence := int64(0)
		if millis == 0 {
			sequence = 1
		}

		return StreamID{
			Millis: millis,
			Seq:    sequence,
		}, nil
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid stream ID")
	}

	id := StreamID{Millis: millis, Seq: seq}
	if id.Millis == 0 && id.Seq == 0 {
		return StreamID{}, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	if last != nil &&
		(id.Millis < last.Millis ||
			(id.Millis == last.Millis && id.Seq <= last.Seq)) {
		return StreamID{}, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return id, nil
}

func (h CommandHandler) handleXrange(cmd Xrange) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		return "*0\r\n"
	}

	stream, ok := entry.Value.(StreamValue)
	if !ok {
		return wrongType
	}

	start, err := parseRangeID(cmd.BegID)
	if err != nil {
		return "-ERR invalid stream ID\r\n"
	}

	end, err := parseRangeID(cmd.EndID)
	if err != nil {
		return "-ERR invalid stream ID\r\n"
	}

	var entries []StreamEntry

	for _, streamEntry := range stream.Entries {
		if compareStreamIDs(streamEntry.ID, start) < 0 {
			continue
		}
		if compareStreamIDs(streamEntry.ID, end) > 0 {
			break
		}

		entries = append(entries, streamEntry)
	}

	return buildXRangeResponse(entries)
}

func (h CommandHandler) handleXread(cmd Xread) string {
	h.store.mu.Lock()

	results := ""
	count := 0
	if !strings.EqualFold(cmd.Streams[0].ID, "$") {
		for _, request := range cmd.Streams {
			entry, exists := h.store.db[request.Key]
			if !exists {
				continue
			}

			stream, ok := entry.Value.(StreamValue)
			if !ok {
				h.store.mu.Unlock()
				return wrongType
			}

			id, err := parseRangeID(request.ID)
			if err != nil {
				h.store.mu.Unlock()
				return "-ERR invalid stream ID\r\n"
			}

			entries := make([]StreamEntry, 0)
			for _, streamEntry := range stream.Entries {
				if compareStreamIDs(streamEntry.ID, id) > 0 {
					entries = append(entries, streamEntry)
				}
			}
			if len(entries) == 0 {
				continue
			}

			results += "*2\r\n" + BulkString(request.Key) + buildXRangeResponse(entries)
			count++
		}
		if count > 0 {
			h.store.mu.Unlock()
			return fmt.Sprintf("*%d\r\n%s", count, results)
		}
	}
	if cmd.Block == -1 {
		h.store.mu.Unlock()
		return "*0\r\n"
	}

	wait := make(chan streamResult, 1)
	for _, request := range cmd.Streams {
		id, _ := parseRangeID(request.ID)
		streamWaiter := &streamWaiter{id: id, result: wait}
		h.store.streamWaiters[request.Key] = append(h.store.streamWaiters[request.Key], streamWaiter)
	}
	h.store.mu.Unlock()

	var result streamResult
	if cmd.Block == 0 {
		result = <-wait
	} else {
		select {
		case result = <-wait:
		case <-time.After(cmd.Block):
			return "*-1\r\n"
		}
	}

	return "*1\r\n*2\r\n" + BulkString(result.key) + buildXRangeResponse([]StreamEntry{result.entry})
}

func parseRangeID(value string) (StreamID, error) {
	if value == "-" {
		return StreamID{Millis: 0, Seq: 0}, nil
	}

	if value == "+" {
		return StreamID{
			Millis: math.MaxInt64,
			Seq:    math.MaxInt64,
		}, nil
	}

	parts := strings.SplitN(value, "-", 2)
	if len(parts) != 2 {
		return StreamID{}, fmt.Errorf("invalid stream ID")
	}

	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, err
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, err
	}

	return StreamID{Millis: millis, Seq: seq}, nil
}

func compareStreamIDs(left, right StreamID) int {
	if left.Millis < right.Millis {
		return -1
	}
	if left.Millis > right.Millis {
		return 1
	}
	if left.Seq < right.Seq {
		return -1
	}
	if left.Seq > right.Seq {
		return 1
	}
	return 0
}

func buildXRangeResponse(entries []StreamEntry) string {
	response := fmt.Sprintf("*%d\r\n", len(entries))

	for _, entry := range entries {
		id := fmt.Sprintf("%d-%d", entry.ID.Millis, entry.ID.Seq)

		response += "*2\r\n"
		response += BulkString(id)
		response += buildArray(entry.Fields)
	}

	return response
}

func (h CommandHandler) handleXadd(cmd Xadd) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		entry = Entry{Value: StreamValue{}}
	}

	stream, ok := entry.Value.(StreamValue)
	if !ok {
		return wrongType
	}

	var last *StreamID
	if len(stream.Entries) > 0 {
		last = &stream.Entries[len(stream.Entries)-1].ID
	}

	id, err := generateStreamID(cmd.ID, last)
	if err != nil {
		return fmt.Sprintf("-ERR %s\r\n", err)
	}

	newEntry := StreamEntry{
		ID:     id,
		Fields: cmd.Fields,
	}
	stream.Entries = append(stream.Entries, newEntry)

	entry.Value = stream
	h.store.db[cmd.Key] = entry
	h.notifyStreamWaiters(cmd.Key, newEntry)
	idString := fmt.Sprintf("%d-%d", id.Millis, id.Seq)
	return BulkString(idString)
}

func (h CommandHandler) handleIncr(cmd Incr) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	
	if !exists {
		entry = Entry{Value: StringValue{Value: "1"}, Expiry: nil}
		h.store.db[cmd.Key] = entry
		return ":1\r\n"
	}
	
	switch value := entry.Value.(type) {
	case StringValue:
		n, err := strconv.ParseInt(value.Value, 10, 64)
		if err != nil {
			return "-ERR value is not an integer or out of range\r\n"
		}
		n++
		entry.Value = StringValue{Value: strconv.FormatInt(n, 10)}
		h.store.db[cmd.Key] = entry
		return fmt.Sprintf(":%d\r\n", n)
	default:
		return "-ERR value is not an integer or out of range\r\n"
	}

}

func (h CommandHandler) handleLPush(cmd LPush) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		entry = Entry{Value: ListValue{}}
	}
	list, ok := entry.Value.(ListValue)
	if !ok {
		return wrongType
	}
	for _, value := range cmd.Values {
		list.Values = append([]string{value}, list.Values...)
	}
	entry.Value = list
	h.store.db[cmd.Key] = entry
	h.notifyWaiters(cmd.Key)
	return fmt.Sprintf(":%d\r\n", len(list.Values))
}

func (h CommandHandler) handleLLen(cmd LLen) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		return ":0\r\n"
	}
	if list, ok := entry.Value.(ListValue); ok {
		return fmt.Sprintf(":%d\r\n", len(list.Values))
	}
	return wrongType
}

func (h CommandHandler) handleLPop(cmd LPop) string { return h.handlePop(cmd.Key, cmd.Count, true) }

func (h CommandHandler) handleRPop(cmd RPop) string { return h.handlePop(cmd.Key, cmd.Count, false) }

func (h CommandHandler) handlePop(key string, count *int, fromLeft bool) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[key]
	if !exists {
		if count == nil {
			return "$-1\r\n"
		}
		return "*0\r\n"
	}
	list, ok := entry.Value.(ListValue)
	if !ok {
		return wrongType
	}
	if count == nil {
		if len(list.Values) == 0 {
			return "$-1\r\n"
		}
		var item string
		if fromLeft {
			item, list.Values = list.Values[0], list.Values[1:]
		} else {
			last := len(list.Values) - 1
			item, list.Values = list.Values[last], list.Values[:last]
		}
		entry.Value = list
		h.store.db[key] = entry
		return BulkString(item)
	}

	items := make([]string, 0, *count)
	for range *count {
		if len(list.Values) == 0 {
			break
		}
		if fromLeft {
			items = append(items, list.Values[0])
			list.Values = list.Values[1:]
		} else {
			last := len(list.Values) - 1
			items = append(items, list.Values[last])
			list.Values = list.Values[:last]
		}
	}
	entry.Value = list
	h.store.db[key] = entry
	return buildArray(items)
}

func (h CommandHandler) handleLRange(cmd LRange) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		return "*0\r\n"
	}
	list, ok := entry.Value.(ListValue)
	if !ok {
		return wrongType
	}
	size := len(list.Values)
	start, end := cmd.Start, cmd.End
	if start < 0 {
		start = max(size+start, 0)
	}
	if end < 0 {
		end = min(size+end, size-1)
	} else {
		end = min(end, size-1)
	}
	if size == 0 || start > end {
		return "*0\r\n"
	}
	return buildArray(list.Values[start : end+1])
}

func (h CommandHandler) handleBLPop(cmd BLPop) string {
	h.store.mu.Lock()
	if entry, exists := h.store.db[cmd.Key]; exists {
		if list, ok := entry.Value.(ListValue); ok && len(list.Values) > 0 {
			item := list.Values[0]
			list.Values = list.Values[1:]
			entry.Value = list
			h.store.db[cmd.Key] = entry
			h.store.mu.Unlock()
			return popResponse(cmd.Key, item)
		}
	}

	w := &waiter{result: make(chan popResult, 1), active: true}
	h.store.waiters[cmd.Key] = append(h.store.waiters[cmd.Key], w)
	h.store.mu.Unlock()

	if cmd.Timeout == 0 {
		result := <-w.result
		return popResponse(result.key, result.item)
	}

	select {
	case result := <-w.result:
		return popResponse(result.key, result.item)
	case <-time.After(time.Duration(cmd.Timeout * float64(time.Second))):
		h.removeWaiter(w)
		return "*-1\r\n"
	}
}

func (h CommandHandler) notifyWaiters(key string) {
	for len(h.store.waiters[key]) > 0 {
		entry, exists := h.store.db[key]
		if !exists {
			return
		}
		list, ok := entry.Value.(ListValue)
		if !ok || len(list.Values) == 0 {
			return
		}

		w := h.store.waiters[key][0]
		h.store.waiters[key] = h.store.waiters[key][1:]
		if !w.active {
			continue
		}
		w.active = false
		h.removeWaiterLocked(w)
		item := list.Values[0]
		list.Values = list.Values[1:]
		entry.Value = list
		h.store.db[key] = entry
		w.result <- popResult{key: key, item: item}
		return
	}
}

func (h CommandHandler) removeWaiter(w *waiter) {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if w.active {
		w.active = false
		h.removeWaiterLocked(w)
	}
}

func (h CommandHandler) removeWaiterLocked(w *waiter) {
	for key, waiters := range h.store.waiters {
		for i := 0; i < len(waiters); {
			if waiters[i] == w {
				waiters = append(waiters[:i], waiters[i+1:]...)
				continue
			}
			i++
		}
		if len(waiters) == 0 {
			delete(h.store.waiters, key)
		} else {
			h.store.waiters[key] = waiters
		}
	}
}

func buildArray(items []string) string {
	response := fmt.Sprintf("*%d\r\n", len(items))
	for _, item := range items {
		response += BulkString(item)
	}
	return response
}

func popResponse(key, item string) string { return buildArray([]string{key, item}) }
func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
func (h CommandHandler) notifyStreamWaiters(key string, entry StreamEntry) {
	for _, waiter := range h.store.streamWaiters[key] {
		select {
		case waiter.result <- streamResult{key: key, entry: entry}:
		default:
		}
	}
}
