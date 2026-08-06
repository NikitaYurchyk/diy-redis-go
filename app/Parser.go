package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func ParseArray(reader *bufio.Reader) ([]string, error) {
	line, err := readLine(reader)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array")
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}
	parts := make([]string, count)
	for i := range parts {
		parts[i], err = ParseBulkString(reader)
		if err != nil {
			return nil, err
		}
	}
	return parts, nil
}

func ParseBulkString(reader *bufio.Reader) (string, error) {
	line, err := readLine(reader)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(line, "$") {
		return "", fmt.Errorf("expected RESP bulk string")
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", fmt.Errorf("invalid bulk string length: %w", err)
	}
	if length < 0 {
		return "", nil
	}

	data := make([]byte, length+2)
	if _, err := io.ReadFull(reader, data); err != nil {
		return "", err
	}
	if string(data[length:]) != crlf {
		return "", fmt.Errorf("bulk string missing CRLF")
	}
	return string(data[:length]), nil
}

func parseExpiry(parts []string) *time.Time {
	for i := 3; i < len(parts); i++ {
		switch strings.ToUpper(parts[i]) {
		case "EX":
			expiry := time.Now().Add(time.Duration(parseInt64(parts[i+1])) * time.Second)
			return &expiry
		case "PX":
			expiry := time.Now().Add(time.Duration(parseInt64(parts[i+1])) * time.Millisecond)
			return &expiry
		}
	}
	return nil
}

func parseOptionalInt(parts []string, index int) *int {
	if len(parts) <= index {
		return nil
	}
	value := parseInt(parts[index])
	return &value
}

func parseInt(value string) int { result, _ := strconv.Atoi(value); return result }

func parseInt64(value string) int64 { result, _ := strconv.ParseInt(value, 10, 64); return result }
func parseUInt64(value string) uint64 { result, _ := strconv.ParseUint(value, 10, 64); return result }
func parseFloat(value string) float64 { result, _ := strconv.ParseFloat(value, 64); return result }
