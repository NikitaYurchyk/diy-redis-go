package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidStreamID = fmt.Errorf("invalid stream ID")
	errIDNotIncreasing = fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
	errIDNotPositive   = fmt.Errorf("The ID specified in XADD must be greater than 0-0")
)

type StreamID struct {
	Millis int64
	Seq    int64
}

// String renders the wire form, "<millis>-<seq>".
func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.Millis, id.Seq)
}

// Compare orders by millisecond then sequence, returning -1, 0 or 1.
func (id StreamID) Compare(other StreamID) int {
	if id.Millis < other.Millis {
		return -1
	}
	if id.Millis > other.Millis {
		return 1
	}
	if id.Seq < other.Seq {
		return -1
	}
	if id.Seq > other.Seq {
		return 1
	}
	return 0
}

type StreamEntry struct {
	ID     StreamID
	Fields []string
}

// StreamValue holds entries in insertion order, which is also ID order
// because NextID rejects anything not greater than the current last entry.
type StreamValue struct {
	Entries []StreamEntry
}

func (StreamValue) isRedisValue() {}

// Last returns the highest ID in the stream, or nil when it is empty.
func (s StreamValue) Last() *StreamID {
	if len(s.Entries) == 0 {
		return nil
	}
	return &s.Entries[len(s.Entries)-1].ID
}

// Range returns the entries between start and end, both inclusive, as XRANGE
// reports them.
func (s StreamValue) Range(start, end StreamID) []StreamEntry {
	var entries []StreamEntry

	for _, entry := range s.Entries {
		if entry.ID.Compare(start) < 0 {
			continue
		}
		// Entries are ID-ordered, so the first one past end ends the scan.
		if entry.ID.Compare(end) > 0 {
			break
		}
		entries = append(entries, entry)
	}

	return entries
}

// After returns the entries strictly greater than id. XREAD bounds are
// exclusive, unlike XRANGE.
func (s StreamValue) After(id StreamID) []StreamEntry {
	entries := make([]StreamEntry, 0)

	for _, entry := range s.Entries {
		if entry.ID.Compare(id) > 0 {
			entries = append(entries, entry)
		}
	}

	return entries
}

// NextID resolves an XADD ID argument against the stream's last entry. It
// accepts the three forms Redis allows: "*" (fully auto), "<millis>-*" (auto
// sequence), and an explicit "<millis>-<seq>".
func (s StreamValue) NextID(requested string) (StreamID, error) {
	last := s.Last()

	if requested == "*" {
		return autoStreamID(last), nil
	}

	parts := strings.SplitN(requested, "-", 2)
	if len(parts) != 2 {
		return StreamID{}, errInvalidStreamID
	}

	millis, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, errInvalidStreamID
	}

	if parts[1] == "*" {
		return autoSeqStreamID(millis, last)
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, errInvalidStreamID
	}

	return explicitStreamID(StreamID{Millis: millis, Seq: seq}, last)
}

// autoStreamID builds the ID for "*", taking the current time unless the
// clock has not advanced past the last entry, in which case it reuses that
// millisecond and takes the next sequence number.
func autoStreamID(last *StreamID) StreamID {
	now := time.Now().UnixMilli()

	if last != nil && now <= last.Millis {
		return StreamID{Millis: last.Millis, Seq: last.Seq + 1}
	}

	return StreamID{Millis: now, Seq: 0}
}

// autoSeqStreamID builds the ID for "<millis>-*", picking the sequence that
// follows the last entry at that millisecond.
func autoSeqStreamID(millis int64, last *StreamID) (StreamID, error) {
	if last != nil {
		if millis < last.Millis {
			return StreamID{}, errIDNotIncreasing
		}
		if millis == last.Millis {
			return StreamID{Millis: millis, Seq: last.Seq + 1}, nil
		}
	}

	// 0-0 is reserved, so the first sequence at millisecond 0 is 1.
	if millis == 0 {
		return StreamID{Millis: 0, Seq: 1}, nil
	}

	return StreamID{Millis: millis, Seq: 0}, nil
}

// explicitStreamID validates a fully specified "<millis>-<seq>".
func explicitStreamID(id StreamID, last *StreamID) (StreamID, error) {
	if id.Millis == 0 && id.Seq == 0 {
		return StreamID{}, errIDNotPositive
	}

	if last != nil && id.Compare(*last) <= 0 {
		return StreamID{}, errIDNotIncreasing
	}

	return id, nil
}

func ParseStreamID(value string) (StreamID, error) {
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

func buildXRangeResponse(entries []StreamEntry) string {
	response := fmt.Sprintf(respArrayHeaderFormat, len(entries))

	for _, entry := range entries {
		response += respArrayOf2
		response += BulkString(entry.ID.String())
		response += buildArray(entry.Fields)
	}

	return response
}

func createArrOfStreams(args []string) []XreadStream {
	count := len(args) / 2
	streams := make([]XreadStream, count)

	for i := 0; i < count; i++ {
		streams[i] = XreadStream{
			Key: args[i],
			ID:  args[count+i],
		}
	}

	return streams
}
