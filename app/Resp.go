package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"strings"
)

// Every raw RESP byte sequence lives here.

// crlf terminates every RESP line, on the way in and on the way out.
const crlf = "\r\n"

// Fixed replies.
const (
	respOK     = "+OK\r\n"
	respPong   = "+PONG\r\n"
	respQueued = "+QUEUED\r\n"

	respNullBulkString = "$-1\r\n"
	respNullArray      = "*-1\r\n"
	respEmptyArray     = "*0\r\n"
	respZero           = ":0\r\n"
	respOne            = ":1\r\n"

	respArrayOf1 = "*1\r\n"
	respArrayOf2 = "*2\r\n"
)

// TYPE replies.
const (
	respTypeNone   = "+none\r\n"
	respTypeString = "+string\r\n"
	respTypeList   = "+list\r\n"
	respTypeStream = "+stream\r\n"
)

// Errors.
const (
	wrongType         = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	errNotAnInteger   = "-ERR value is not an integer or out of range\r\n"
	errInvalidStream  = "-ERR invalid stream ID\r\n"
	errUnknownCommand = "-ERR unknown command\r\n"
	errNestedMulti    = "-ERR MULTI calls can not be nested\r\n"
	errExecNoMulti    = "-ERR EXEC without MULTI\r\n"
	errDiscardNoMulti = "-ERR DISCARD without MULTI\r\n"
	errWatchInMulti   = "-ERR WATCH inside MULTI is not allowed\r\n"
)

// Format strings for replies whose contents vary.
const (
	respBulkStringFormat  = "$%d\r\n%s\r\n"
	respIntegerFormat     = ":%d\r\n"
	respArrayHeaderFormat = "*%d\r\n"

	errFormat               = "-ERR %s\r\n"
	errUnknownCommandFormat = "-ERR unknown command '%s'\r\n"
	respFullResyncFormat    = "+FULLRESYNC %s %d\r\n"
)

const emptyRDBHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000ff00f06e3bfec0ff5aa2"
func EmptyRDB() []byte {
	b, _ := hex.DecodeString(emptyRDBHex)
	return b
}

func buildArray(items []string) string {
	response := fmt.Sprintf(respArrayHeaderFormat, len(items))
	for _, item := range items {
		response += BulkString(item)
	}
	return response
}

func popResponse(key, item string) string { return buildArray([]string{key, item}) }

func FullResync(replID string, offset uint64) string {
	return fmt.Sprintf(respFullResyncFormat, replID, offset)
}

func BulkString(data string) string {
	return fmt.Sprintf(respBulkStringFormat, len(data), data)
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, crlf) {
		return "", fmt.Errorf("RESP line missing CRLF")
	}
	return strings.TrimSuffix(line, crlf), nil
}

func RDBFileMessage(data []byte) []byte {
    header := fmt.Sprintf("$%d\r\n", len(data))
    return append([]byte(header), data...)
}