package main

import (
	"strings"
	"time"
)

type InfoOption string

const (
	ReplicOpt InfoOption = "replication"
)

type Command interface {
	isCommand()
}

type Xadd struct {
	Key    string
	ID     string
	Fields []string
}
type Xrange struct {
	Key   string
	BegID string
	EndID string
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
type XreadStream struct {
	Key string
	ID  string
}
type Xread struct {
	Streams []XreadStream
	Block   time.Duration
}
type BLPop struct {
	Key     string
	Timeout float64
}
type InfoCMD struct {
	Type InfoOption
}

type Incr struct{ Key string }
type Multi struct{}
type Exec struct{}
type Discard struct{}
type Watch struct{ Keys []string }
type Unwatch struct{}
type Unknown struct{ Name string }

func (Discard) isCommand() {}
func (Watch) isCommand()   {}
func (Exec) isCommand()    {}
func (Incr) isCommand()    {}
func (Xrange) isCommand()  {}
func (Xread) isCommand()   {}
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
func (Multi) isCommand()   {}
func (Unwatch) isCommand() {}
func (InfoCMD) isCommand() {}

func ParseCommand(parts []string) Command {
	switch strings.ToUpper(parts[0]) {
	case "INFO":
		return InfoCMD{
			Type: ReplicOpt,
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

	default:
		return Unknown{Name: parts[0]}
	}
}
