package main

import (
	"strconv"
	"strings"
	"time"
)

type Command interface {
	isCommand()
}

type Xadd struct {
	Key    string
	ID 	   string
	Fields []string
}

type Ping struct{}
type Echo struct{ Message string }
type Get struct{ Key string }
type Type struct{ Key string }
type Set struct {
	Key, Value string
	Expiry     *time.Time
}
type RPush struct {
	Key    string
	Values []string
}
type LPush struct {
	Key    string
	Values []string
}
type LLen struct{ Key string }
type LPop struct {
	Key   string
	Count *int
}
type RPop struct {
	Key   string
	Count *int
}
type LRange struct {
	Key        string
	Start, End int
}
type BLPop struct {
	Key     string
	Timeout float64
}
type Unknown struct{ Name string }


func (Xadd) isCommand()    {}
func (Ping) isCommand()    {}
func (Echo) isCommand()    {}
func (Get) isCommand()     {}
func (Type) isCommand()    {}
func (Set) isCommand()     {}
func (RPush) isCommand()   {}
func (LPush) isCommand()   {}
func (LLen) isCommand()    {}
func (LPop) isCommand()    {}
func (RPop) isCommand()    {}
func (LRange) isCommand()  {}
func (BLPop) isCommand()   {}
func (Unknown) isCommand() {}

func ParseCommand(parts []string) Command {
	switch strings.ToUpper(parts[0]) {
	case "XADD":
		return Xadd{Key: parts[1], ID: parts[2], Fields: parts[3:]}
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
	default:
		return Unknown{Name: parts[0]}
	}
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
func parseFloat(value string) float64 { result, _ := strconv.ParseFloat(value, 64); return result }
