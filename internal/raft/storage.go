package raft

// StableStore holds the Raft persistent metadata.

import (
	"encoding/binary"

	bolt "go.etcd.io/bbolt"
)

type StableStore interface {
	Term() int
	SetTerm(t int)
	VotedFor() string
	SetVotedFor(id string)
	LastApplied() int
	SetLastApplied(index int)
}

// ------------------------------------------------------------
// Bolt-backed implementation
// ------------------------------------------------------------
const (
	bMeta       = "meta" // bucket name
	kTerm       = "term"
	kVotedFor   = "voted_for"
	LastApplied = "last_applied"
)

type boltStore struct{ db *bolt.DB }

func NewBoltStore(db *bolt.DB) StableStore {
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bMeta))
		return err
	}); err != nil {
		panic(err)
	}
	return &boltStore{db: db}
}

func (s *boltStore) Term() int {
	var t uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(kTerm)); v != nil {
			t = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return int(t)
}

func (s *boltStore) SetTerm(term int) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(term))
	_ = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(kTerm), buf[:])
	})
}

func (s *boltStore) VotedFor() string {
	var id string
	_ = s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(kVotedFor)); v != nil {
			id = string(v)
		}
		return nil
	})
	return id
}

func (s *boltStore) SetVotedFor(id string) {
	_ = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(kVotedFor), []byte(id))
	})
}

func (s *boltStore) LastApplied() int {
	var last uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte(bMeta)).Get([]byte(LastApplied)); v != nil {
			last = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return int(last)
}

func (s *boltStore) SetLastApplied(index int) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	_ = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bMeta)).Put([]byte(LastApplied), buf[:])
	})
}
