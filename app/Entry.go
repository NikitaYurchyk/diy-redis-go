package main

import (
	"net"
	"os"
	"sync"
	"sync/atomic"
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
	file          *os.File
	db            map[string]Entry
	waiters       map[string][]*waiter
	streamWaiters map[string][]*streamWaiter
	versions      map[string]uint64
	config        map[string]string
	info          *Info
	replicas      []*ReplicaState
	replicasMu    sync.Mutex
}

// replicaOutboxSize bounds how far a replica may fall behind before we give up
// on it. Sized so a replica that is merely slow keeps up, while one that has
// stopped reading is dropped instead of stalling the command handlers.
const replicaOutboxSize = 1024

type ReplicaState struct {
	Conn net.Conn
	Port int

	// Offset is the last offset the replica acknowledged. Guarded by
	// Store.replicasMu.
	Offset uint64

	// outbox hands every propagated write to pump, so no command handler ever
	// blocks on a replica's socket.
	outbox    chan []byte
	closed    atomic.Bool
	closeOnce sync.Once
}

func newReplicaState(conn net.Conn, port int) *ReplicaState {
	replica := &ReplicaState{
		Conn:   conn,
		Port:   port,
		outbox: make(chan []byte, replicaOutboxSize),
	}
	go replica.pump()
	return replica
}

// pump owns every write to the replica connection. It is the only writer, so
// messages reach the replica in the order send queued them.
func (r *ReplicaState) pump() {
	for msg := range r.outbox {
		if _, err := r.Conn.Write(msg); err != nil {
			r.close()
			return
		}
	}
}

// send queues msg without ever blocking, and reports whether the replica is
// still usable. A full outbox means the replica cannot keep up, so it is
// dropped rather than allowed to back-pressure the whole server.
func (r *ReplicaState) send(msg []byte) bool {
	if r.closed.Load() {
		return false
	}
	select {
	case r.outbox <- msg:
		return true
	default:
		r.close()
		return false
	}
}

func (r *ReplicaState) close() {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.Conn.Close()
	})
}

func (s *Store) AddReplica(conn net.Conn, port int) {
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	s.replicas = append(s.replicas, newReplicaState(conn, port))
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

// Propagate forwards a write command to every replica. Only the master
// propagates; on a replica it is a no-op.
func (s *Store) Propagate(args ...string) {
	if s.info.Replication.Role != RoleMaster {
		return
	}
	msg := []byte(buildArray(args))

	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	// Advance the offset under the same lock that orders the sends, so the
	// offset always matches the byte stream the replicas receive.
	s.info.Replication.MasterReplOffset.Add(uint64(len(msg)))
	s.broadcastLocked(msg)
}

// SendGetAck asks every replica for its offset. It travels the same outbox as
// propagated writes, so a replica cannot answer before it has seen them.
func (s *Store) SendGetAck() {
	msg := []byte(buildArray([]string{"REPLCONF", "GETACK", "*"}))

	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	s.broadcastLocked(msg)
}

// broadcastLocked queues msg to every live replica and prunes the dead ones.
// Callers must hold replicasMu; every send is non-blocking, so the lock is
// never held across a socket write.
func (s *Store) broadcastLocked(msg []byte) {
	alive := s.replicas[:0]
	for _, r := range s.replicas {
		if r.send(msg) {
			alive = append(alive, r)
		}
	}
	for i := len(alive); i < len(s.replicas); i++ {
		s.replicas[i] = nil
	}
	s.replicas = alive
}

func NewStore(config map[string]string) *Store {
	if config == nil {
		config = make(map[string]string)
	}
	return &Store{
		db:            make(map[string]Entry),
		waiters:       make(map[string][]*waiter),
		streamWaiters: make(map[string][]*streamWaiter),
		versions:      make(map[string]uint64),
		config:        config,
		info:          InitInfo(),
	}
}
