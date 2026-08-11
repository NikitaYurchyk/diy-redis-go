package main

import (
	"net"
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
	replicas      []*ReplicaState
	replicasMu    sync.Mutex
}

type ReplicaState struct {
	Conn   net.Conn
	Port   int
	Offset uint64
}

func (s *Store) AddReplica(conn net.Conn, port int) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	s.replicas = append(s.replicas, &ReplicaState{Conn: conn, Port: port})
}

func (s *Store) UpdateReplicaAck(conn net.Conn, offset uint64) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	for _, r := range s.replicas {
		if r.Conn == conn {
			r.Offset = offset
			return
		}
	}
}

func (s *Store) ReplicaCount() int {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	return len(s.replicas)
}

func (s *Store) CountAcked(target uint64) int {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	count := 0
	for _, r := range s.replicas {
		if r.Offset >= target {
			count++
		}
	}
	return count
}

func (s *Store) SendGetAck() {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	msg := []byte(buildArray([]string{"REPLCONF", "GETACK", "*"}))
	for _, r := range s.replicas {
		r.Conn.Write(msg)
	}
}


func NewStore() *Store {
	return &Store{
		db:            make(map[string]Entry),
		waiters:       make(map[string][]*waiter),
		streamWaiters: make(map[string][]*streamWaiter),
		versions:      make(map[string]uint64),
	}
}
