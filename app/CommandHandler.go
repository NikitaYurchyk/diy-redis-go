package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Transaction struct {
	active bool
	queue  []Command
}
type CommandHandler struct {
	store   *Store
	tx      Transaction
	isExec  bool
	watched map[string]uint64
}

func (h *CommandHandler) Handle(command Command) string {
	switch cmd := command.(type) {
	case Multi:
		if h.tx.active {
			return errNestedMulti
		}

		h.tx.active = true
		h.tx.queue = nil
		return respOK

	case Exec:
		if !h.tx.active {
			return errExecNoMulti
		}
		return h.handleExec()

	case Discard:
		if !h.tx.active {
			return errDiscardNoMulti
		}

		h.tx.active = false
		h.tx.queue = nil
		clear(h.watched)
		return respOK

	case Watch:
		if h.tx.active {
			return errWatchInMulti
		}
		return h.handleWatched(cmd)

	case Unwatch:
		return h.handleUnwatch(cmd)
	}

	if h.tx.active {
		h.tx.queue = append(h.tx.queue, command)
		return respQueued
	}

	return h.exec(command)
}

func (h *CommandHandler) exec(command Command) string {
	switch cmd := command.(type) {
	case Xread:
		return h.handleXread(cmd)
	case Multi:
		return h.handleMulti(cmd)
	case Ping:
		return respPong
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
	case InfoCMD:
		return h.handleInfo(cmd)
	case Unknown:
		return fmt.Sprintf(errUnknownCommandFormat, cmd.Name)
	default:
		return errUnknownCommand
	}
}

func (h *CommandHandler) handleGet(cmd Get) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists || (entry.Expiry != nil && entry.Expiry.Before(time.Now())) {
		if exists {
			delete(h.store.db, cmd.Key)
		}
		return respNullBulkString
	}
	if value, ok := entry.Value.(StringValue); ok {
		return BulkString(value.Value)
	}
	return wrongType
}

func (h *CommandHandler) handleInfo(cmd InfoCMD) string {
	if cmd.Type == ReplicOpt {
		return BulkString("role:" + string(h.store.info.Replication.Role))
	}
	return respNullBulkString
}

func (h *CommandHandler) handleType(cmd Type) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists || (entry.Expiry != nil && entry.Expiry.Before(time.Now())) {
		if exists {
			delete(h.store.db, cmd.Key)
		}
		return respTypeNone
	}

	switch entry.Value.(type) {
	case StringValue:
		return respTypeString
	case ListValue:
		return respTypeList
	case StreamValue:
		return respTypeStream
	default:
		return respTypeNone
	}
}

func (h *CommandHandler) handleSet(cmd Set) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	h.store.db[cmd.Key] = Entry{Value: StringValue{Value: cmd.Value}, Expiry: cmd.Expiry}
	h.incrVersion(cmd.Key)
	return respOK
}

func (h *CommandHandler) handleMulti(cmd Multi) string {
	h.tx.active = true
	return respOK
}

func (h *CommandHandler) handleRPush(cmd RPush) string {
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
	h.incrVersion(cmd.Key)
	h.notifyWaiters(cmd.Key)
	return fmt.Sprintf(respIntegerFormat, len(list.Values))
}

func (h *CommandHandler) handleWatched(cmd Watch) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	for _, key := range cmd.Keys {
		h.watched[key] = h.store.versions[key]
	}
	return respOK
}

func (h *CommandHandler) handleUnwatch(cmd Unwatch) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	clear(h.watched)
	return respOK
}

func (h *CommandHandler) handleXrange(cmd Xrange) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		return respEmptyArray
	}

	stream, ok := entry.Value.(StreamValue)
	if !ok {
		return wrongType
	}

	start, err := ParseStreamID(cmd.BegID)
	if err != nil {
		return errInvalidStream
	}

	end, err := ParseStreamID(cmd.EndID)
	if err != nil {
		return errInvalidStream
	}

	return buildXRangeResponse(stream.Range(start, end))
}

func (h *CommandHandler) handleExec() string {
	queued := h.tx.queue
	h.tx.active = false
	h.tx.queue = nil
	defer clear(h.watched)

	for key, version := range h.watched {
		if h.store.versions[key] != version {
			return respNullArray
		}

	}

	replies := make([]string, 0, len(queued))
	for _, queuedCommand := range queued {
		replies = append(replies, h.Handle(queuedCommand))
	}

	return fmt.Sprintf(respArrayHeaderFormat+"%s", len(replies), strings.Join(replies, ""))
}

func (h *CommandHandler) handleXread(cmd Xread) string {
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

			id, err := ParseStreamID(request.ID)
			if err != nil {
				h.store.mu.Unlock()
				return errInvalidStream
			}

			entries := stream.After(id)
			if len(entries) == 0 {
				continue
			}

			results += respArrayOf2 + BulkString(request.Key) + buildXRangeResponse(entries)
			count++
		}
		if count > 0 {
			h.store.mu.Unlock()
			return fmt.Sprintf(respArrayHeaderFormat+"%s", count, results)
		}
	}
	if cmd.Block == -1 {
		h.store.mu.Unlock()
		return respEmptyArray
	}

	wait := make(chan streamResult, 1)
	for _, request := range cmd.Streams {
		id, _ := ParseStreamID(request.ID)
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
			return respNullArray
		}
	}

	return respArrayOf1 + respArrayOf2 + BulkString(result.key) + buildXRangeResponse([]StreamEntry{result.entry})
}

func (h *CommandHandler) handleXadd(cmd Xadd) string {
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

	id, err := stream.NextID(cmd.ID)
	if err != nil {
		return fmt.Sprintf(errFormat, err)
	}

	newEntry := StreamEntry{
		ID:     id,
		Fields: cmd.Fields,
	}
	stream.Entries = append(stream.Entries, newEntry)

	entry.Value = stream
	h.store.db[cmd.Key] = entry
	h.incrVersion(cmd.Key)
	h.notifyStreamWaiters(cmd.Key, newEntry)
	return BulkString(id.String())
}

func (h *CommandHandler) handleIncr(cmd Incr) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]

	if !exists {
		entry = Entry{Value: StringValue{Value: "1"}, Expiry: nil}
		h.store.db[cmd.Key] = entry
		h.incrVersion(cmd.Key)
		return respOne
	}

	switch value := entry.Value.(type) {
	case StringValue:
		n, err := strconv.ParseInt(value.Value, 10, 64)
		if err != nil {
			return errNotAnInteger
		}
		n++
		entry.Value = StringValue{Value: strconv.FormatInt(n, 10)}
		h.store.db[cmd.Key] = entry
		h.incrVersion(cmd.Key)
		return fmt.Sprintf(respIntegerFormat, n)
	default:
		return errNotAnInteger
	}

}

func (h *CommandHandler) handleLPush(cmd LPush) string {
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
	h.incrVersion(cmd.Key)
	h.notifyWaiters(cmd.Key)
	return fmt.Sprintf(respIntegerFormat, len(list.Values))
}

func (h *CommandHandler) handleLLen(cmd LLen) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		return respZero
	}
	if list, ok := entry.Value.(ListValue); ok {
		return fmt.Sprintf(respIntegerFormat, len(list.Values))
	}
	return wrongType
}

func (h *CommandHandler) handleLPop(cmd LPop) string { return h.handlePop(cmd.Key, cmd.Count, true) }

func (h *CommandHandler) handleRPop(cmd RPop) string { return h.handlePop(cmd.Key, cmd.Count, false) }

func (h *CommandHandler) handlePop(key string, count *int, fromLeft bool) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[key]
	if !exists {
		if count == nil {
			return respNullBulkString
		}
		return respEmptyArray
	}
	list, ok := entry.Value.(ListValue)
	if !ok {
		return wrongType
	}
	if count == nil {
		if len(list.Values) == 0 {
			return respNullBulkString
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
		h.incrVersion(key)
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
	if len(items) > 0 {
		h.incrVersion(key)
	}
	return buildArray(items)
}

func (h *CommandHandler) handleLRange(cmd LRange) string {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()

	entry, exists := h.store.db[cmd.Key]
	if !exists {
		return respEmptyArray
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
		return respEmptyArray
	}
	return buildArray(list.Values[start : end+1])
}

func (h *CommandHandler) handleBLPop(cmd BLPop) string {
	h.store.mu.Lock()
	if entry, exists := h.store.db[cmd.Key]; exists {
		if list, ok := entry.Value.(ListValue); ok && len(list.Values) > 0 {
			item := list.Values[0]
			list.Values = list.Values[1:]
			entry.Value = list
			h.store.db[cmd.Key] = entry
			h.incrVersion(cmd.Key)
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
		return respNullArray
	}
}

func (h *CommandHandler) notifyWaiters(key string) {
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
		h.incrVersion(key)
		w.result <- popResult{key: key, item: item}
		return
	}
}

func (h *CommandHandler) removeWaiter(w *waiter) {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	if w.active {
		w.active = false
		h.removeWaiterLocked(w)
	}
}

func (h *CommandHandler) removeWaiterLocked(w *waiter) {
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

func (h *CommandHandler) incrVersion(key string) {
	h.store.versions[key]++
}

func (h *CommandHandler) notifyStreamWaiters(key string, entry StreamEntry) {
	for _, waiter := range h.store.streamWaiters[key] {
		select {
		case waiter.result <- streamResult{key: key, entry: entry}:
		default:
		}
	}
}
