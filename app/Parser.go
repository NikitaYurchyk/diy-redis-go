package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
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
	if string(data[length:]) != "\r\n" {
		return "", fmt.Errorf("bulk string missing CRLF")
	}
	return string(data[:length]), nil
}

func BulkString(data string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
}

func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("RESP line missing CRLF")
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}
