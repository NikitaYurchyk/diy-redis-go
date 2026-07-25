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
type StreamValue struct{Value []string}
func (StringValue) isRedisValue() {}
func (ListValue) isRedisValue()   {}
func (StreamValue) isRedisValue() {}

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

type Store struct {
	mu      sync.Mutex
	db      map[string]Entry
	waiters map[string][]*waiter
}

func NewStore() *Store {
	return &Store{
		db:      make(map[string]Entry),
		waiters: make(map[string][]*waiter),
	}
}
