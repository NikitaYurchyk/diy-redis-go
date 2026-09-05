package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

func ParseCommand(parts []string) Command {
	switch strings.ToUpper(parts[0]) {
	case "INFO":
		return InfoCMD{
			Type: ReplicOpt,
		}
	case "REPLCONF":
		if strings.EqualFold(parts[1], "GETACK") {
			return Replconf{GetAck: true}
		}
		if strings.EqualFold(parts[1], "ACK") {
			return Replconf{Ack: true, AckOffset: parseUInt64(parts[2])}
		}
		if parts[1] == "capa" {
			return Replconf{}
		}
		return Replconf{
			Port: parseInt(parts[2]),
		}

	case "PSYNC":
		return Psync{
			ID:     parts[1],
			Offset: parseUInt64(parts[2]),
		}

	case "WAIT":
		return Wait{
			NumReplicas: parseInt(parts[1]),
			Timeout:     parseInt(parts[2]),
		}

	case "MULTI":
		return Multi{}

	case "EXEC":
		return Exec{}

	case "DISCARD":
		return Discard{}

	case "INCR":
		return Incr{Key: parts[1]}

	case "XADD":
		return Xadd{Key: parts[1], ID: parts[2], Fields: parts[3:]}

	case "XRANGE":
		return Xrange{Key: parts[1], BegID: parts[2], EndID: parts[3]}

	case "XREAD":
		if strings.EqualFold(parts[1], "BLOCK") {
			streams := createArrOfStreams(parts[4:])
			return Xread{Streams: streams, Block: time.Duration(parseInt64(parts[2])) * time.Millisecond}

		}
		streams := createArrOfStreams(parts[2:])
		return Xread{Streams: streams, Block: -1}

	case "UNWATCH":
		return Unwatch{}

	case "WATCH":
		return Watch{parts[1:]}

	case "PING":
		return Ping{}

	case "ECHO":
		return Echo{Message: parts[1]}

	case "GET":
		return Get{Key: parts[1]}

	case "TYPE":
		return Type{Key: parts[1]}

	case "SET":
		return Set{Key: parts[1], Value: parts[2], Expiry: parseExpiry(parts)}

	case "RPUSH":
		return RPush{Key: parts[1], Values: parts[2:]}

	case "LPUSH":
		return LPush{Key: parts[1], Values: parts[2:]}

	case "LLEN":
		return LLen{Key: parts[1]}

	case "LPOP":
		return LPop{Key: parts[1], Count: parseOptionalInt(parts, 2)}

	case "RPOP":
		return RPop{Key: parts[1], Count: parseOptionalInt(parts, 2)}

	case "LRANGE":
		return LRange{Key: parts[1], Start: parseInt(parts[2]), End: parseInt(parts[3])}

	case "BLPOP":
		return BLPop{Key: parts[1], Timeout: parseFloat(parts[len(parts)-1])}
	case "CONFIG":
		if len(parts) < 3 {
			return Unknown{Name: parts[0]}
		}
		return Config{commandType: ConfigType(strings.ToLower(parts[1])), param: strings.ToLower(parts[2])}

	default:
		return Unknown{Name: parts[0]}
	}
}

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

func parseInt(value string) int       { result, _ := strconv.Atoi(value); return result }
func parseInt64(value string) int64   { result, _ := strconv.ParseInt(value, 10, 64); return result }
func parseUInt64(value string) uint64 { result, _ := strconv.ParseUint(value, 10, 64); return result }
func parseFloat(value string) float64 { result, _ := strconv.ParseFloat(value, 64); return result }
