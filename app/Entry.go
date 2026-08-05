package main

import (
	"sync"
	"time"
)

type RedisValue interface {
	isRedisValue()
}

type StringValue struct{ Value string }
type ListValue struct{ Values []string }

func (StringValue) isRedisValue() {}
func (ListValue) isRedisValue()   {}

type Entry struct {
	Value  RedisValue
	Expiry *time.Time
}

type popResult struct {
	key, item string
}

type waiter struct {
	result chan popResult
	active bool
}

type streamResult struct {
	key   string
	entry StreamEntry
}

type streamWaiter struct {
	id     StreamID
	result chan streamResult
}

type Store struct {
	mu            sync.Mutex
	db            map[string]Entry
	waiters       map[string][]*waiter
	streamWaiters map[string][]*streamWaiter
	versions      map[string]uint64
	info          *Info
}

func NewStore() *Store {
	return &Store{
		db:            make(map[string]Entry),
		waiters:       make(map[string][]*waiter),
		streamWaiters: make(map[string][]*streamWaiter),
		versions:      make(map[string]uint64),
	}
}
