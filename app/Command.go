package main

import (
	"time"
)

type InfoOption string
type ConfigType string

const (
	ReplicOpt InfoOption = "replication"
	GetType   ConfigType = "get"
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
type Replconf struct {
	Port      int
	GetAck    bool
	Ack       bool
	AckOffset uint64
}
type Psync struct {
	ID     string
	Offset uint64
}
type Wait struct {
	NumReplicas int
	Timeout     int
}
type Incr struct{ Key string }
type Multi struct{}
type Exec struct{}
type Discard struct{}
type Watch struct{ Keys []string }
type Unwatch struct{}
type Unknown struct{ Name string }
type Config struct {
	commandType ConfigType
	param       string
}

func (Config) isCommand()   {}
func (Discard) isCommand()  {}
func (Replconf) isCommand() {}
func (Watch) isCommand()    {}
func (Exec) isCommand()     {}
func (Incr) isCommand()     {}
func (Xrange) isCommand()   {}
func (Xread) isCommand()    {}
func (Xadd) isCommand()     {}
func (Ping) isCommand()     {}
func (Echo) isCommand()     {}
func (Get) isCommand()      {}
func (Type) isCommand()     {}
func (Set) isCommand()      {}
func (RPush) isCommand()    {}
func (LPush) isCommand()    {}
func (LLen) isCommand()     {}
func (LPop) isCommand()     {}
func (RPop) isCommand()     {}
func (LRange) isCommand()   {}
func (BLPop) isCommand()    {}
func (Unknown) isCommand()  {}
func (Multi) isCommand()    {}
func (Unwatch) isCommand()  {}
func (InfoCMD) isCommand()  {}
func (Wait) isCommand()     {}
func (Psync) isCommand()    {}
