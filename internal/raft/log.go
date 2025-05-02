package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	bolt "go.etcd.io/bbolt"
)

type LogEntry struct {
	Term    int
	Command any
}

type StableLog interface {
	Append(entries ...LogEntry) int // returns last index appended (1‑based)
	At(index int) (LogEntry, bool)  // returns false if index < firstIndex or > lastIndex
	LastIndexTerm() (int, int)
	LastIndex() int
	FirstIndex() int

	TruncateSuffix(idx int) error
	TruncateBefore(index int)
}

// ---------------- util ---------------------
func u64ToKey(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}
func keyToU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// -------------- BoltLog --------------------
type boltLog struct {
	db   *bolt.DB
	mu   sync.Mutex
	base uint64 // first index = base
}

func NewBoltLog(db *bolt.DB) StableLog {
	var base uint64 = 1
	_ = db.Update(func(tx *bolt.Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("log"))
		meta, _ := tx.CreateBucketIfNotExists([]byte("meta"))
		if v := meta.Get([]byte("firstIndex")); v != nil {
			base = binary.BigEndian.Uint64(v)
		} else {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], base)
			_ = meta.Put([]byte("firstIndex"), b[:])
		}
		return nil
	})
	return &boltLog{db: db, base: base}
}

// --------------- StableLog interface -----------------------
func (l *boltLog) Append(entries ...LogEntry) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	var last uint64
	_ = l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		for _, e := range entries {
			last = uint64(l.LastIndex() + 1)
			var buf bytes.Buffer
			_ = gob.NewEncoder(&buf).Encode(e)
			_ = b.Put(u64ToKey(last), buf.Bytes())
		}
		return nil
	})
	return int(last)
}

func (l *boltLog) At(idx int) (LogEntry, bool) {
	var e LogEntry
	if idx < int(l.base) {
		return e, false
	}
	_ = l.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("log")).Get(u64ToKey(uint64(idx)))
		if v == nil {
			return nil
		}
		_ = gob.NewDecoder(bytes.NewReader(v)).Decode(&e)
		return nil
	})
	return e, e.Term != 0 || e.Command != nil
}

func (l *boltLog) LastIndexTerm() (int, int) {
	var idx, term int
	_ = l.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("log")).Cursor()
		k, v := c.Last()
		if k == nil {
			idx, term = int(l.base-1), 0
			return nil
		}
		idx = int(keyToU64(k))
		var e LogEntry
		_ = gob.NewDecoder(bytes.NewReader(v)).Decode(&e)
		term = e.Term
		return nil
	})
	return idx, term
}

func (l *boltLog) LastIndex() int {
	i, _ := l.LastIndexTerm()
	return i
}

func (l *boltLog) FirstIndex() int {
	return int(l.base)
}

// ------------ snapshot compaction ---------------
func (l *boltLog) TruncateBefore(index int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	_ = l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		c := b.Cursor()
		for k, _ := c.First(); k != nil && keyToU64(k) < uint64(index); k, _ = c.Next() {
			_ = c.Delete()
		}

		return nil
	})
}

func (l *boltLog) TruncateSuffix(idx int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("log"))
		c := b.Cursor()
		for k, _ := c.Seek(u64ToKey(uint64(idx))); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}
