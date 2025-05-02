package tests

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

// ---------------------------------------------------------
// leader crashes  →  follower takes over
// ---------------------------------------------------------
func TestLeaderFailover(t *testing.T) {
	nodes, stop := buildCluster(t, 3)
	defer stop()
	time.Sleep(1 * time.Second)

	ldrIdx, ldr := leaderOf(nodes)
	if ldr == nil {
		t.Fatalf("no leader elected")
	}

	killNode(ldr) // crash leader

	// restart old node (returns as follower)
	nodes[ldrIdx] = restartNode(t, nodes[ldrIdx])
	time.Sleep(1 * time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for {
		_, l := leaderOf(nodes)
		if l == nil {
			if time.Now().After(deadline) {
				t.Fatalf("no leader after fail-over")
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if _, ok := l.Propose(kv.SetCmd{Key: "foo", Value: "bar"}); ok {
			break // success
		}
	}
	waitApplyAll(t, nodes, 1)
}

// ---------------------------------------------------------
// follower crashes, later catches up
// ---------------------------------------------------------
func TestFollowerCatchUp(t *testing.T) {
	nodes, stop := buildCluster(t, 3)
	defer stop()
	time.Sleep(2 * time.Second)

	_, ldr := leaderOf(nodes)
	if ldr == nil {
		t.Fatalf("no leader")
	}

	fIdx, follower := firstFollower(nodes)
	killNode(follower) // take follower down

	// leader keeps working
	for i := 0; i < 10; i++ {
		_, _ = ldr.Propose(kv.SetCmd{Key: fmt.Sprintf("k%d", i), Value: "v"})
	}
	waitApplyAll(t, []*raft.Node{ldr}, 10)

	// follower returns and must catch up
	nodes[fIdx] = restartNode(t, nodes[fIdx])
	waitApplyAll(t, nodes, 10)
}

// ---------------------------------------------------------
// single-node restart tests persistence + leadership
// ---------------------------------------------------------
func TestLeaderRestartWithDisk(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "solo.bolt")
	db, _ := bolt.Open(dbPath, 0600, nil)

	applyCh := make(chan raft.ApplyMsg, 128)
	node := raft.NewNode("solo", nil, applyCh, db)
	go node.Start()

	// --- wait for self-election -----------------------
	waitLeader := func(n *raft.Node, d time.Duration) {
		deadline := time.Now().Add(d)
		for n.State() != raft.Leader {
			if time.Now().After(deadline) {
				t.Fatalf("node never became leader")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitLeader(node, 500*time.Millisecond)

	// --- now the proposal will be accepted ------------
	idx, ok := node.Propose(kv.SetCmd{Key: "x", Value: "y"})
	if !ok {
		t.Fatalf("propose rejected by leader")
	}

	for node.LastApplied() < idx {
		time.Sleep(10 * time.Millisecond)
	}
	node.Stop()
	db.Close()

	// ---------- restart -------------------------------
	db2, _ := bolt.Open(dbPath, 0600, nil)
	applyCh2 := make(chan raft.ApplyMsg, 128)
	node2 := raft.NewNode("solo", nil, applyCh2, db2)
	go node2.Start()

	waitLeader(node2, 500*time.Millisecond)

	if node2.LastApplied() < idx {
		t.Fatalf("persisted entry missing after restart")
	}
	db2.Close()
}

/*=========================================================
                helper functions
=========================================================*/

func leaderOf(ns []*raft.Node) (int, *raft.Node) {
	for i, n := range ns {
		if n == nil {
			continue
		} // skip nil nodes
		if n.State() == raft.Leader {
			return i, n
		}
	}
	return -1, nil
}
func firstFollower(ns []*raft.Node) (int, *raft.Node) {
	for i, n := range ns {
		if n.State() == raft.Follower {
			return i, n
		}
	}
	return -1, nil
}

func waitApplyAll(t *testing.T, ns []*raft.Node, want int) {
	t.Helper()
	dead := time.Now().Add(5 * time.Second)
	for _, n := range ns {
		for n.LastApplied() < want {
			if time.Now().After(dead) {
				t.Fatalf("node %s stuck at %d/%d",
					n.ID(), n.LastApplied(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func killNode(n *raft.Node) { n.Stop() }

func restartNode(t *testing.T, old *raft.Node) *raft.Node {
	t.Helper()

	// drain the old ApplyCh until the sender closes it.
	go func(ch <-chan raft.ApplyMsg) {
		for range ch {
		}
	}(old.ApplyCh())

	newCh := make(chan raft.ApplyMsg, 256)
	newN := raft.NewNode(old.ID(), old.PeersCopy(), newCh, old.GetDB())
	go newN.Start()
	return newN
}

// func restartAll(t *testing.T, ns []*raft.Node) {
// 	for i, n := range ns {
// 		ns[i] = restartNode(t, n)
// 	}
// }
