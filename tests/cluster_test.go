package tests

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

func pickNPorts(n int) ([]int, error) {
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, err
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		ports = append(ports, port)
	}
	return ports, nil
}

// buildCluster spins up N raft nodes each with its own BoltDB file in a temp dir.
func buildCluster(t *testing.T, n int) ([]*raft.Node, func()) {
	t.Helper()

	ports, err := pickNPorts(n)
	if err != nil {
		t.Fatalf("port pick: %v", err)
	}

	// pick free ports
	addrs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		addrs[fmt.Sprintf("node%d", i+1)] = fmt.Sprintf("localhost:%d", ports[i])
	}

	nodes := make([]*raft.Node, 0, n)
	dbs := make([]*bolt.DB, 0, n)

	for id, addr := range addrs {
		// peer map ----------------------------------------------------
		peers := make(map[string]string)
		for pid, paddr := range addrs {
			if pid != id {
				peers[pid] = paddr
			}
		}

		// bolt db -----------------------------------------------------
		dbPath := filepath.Join(t.TempDir(), id+".bolt")
		db, err := bolt.Open(dbPath, 0600, nil)
		if err != nil {
			t.Fatalf("open bolt: %v", err)
		}
		dbs = append(dbs, db)

		// raft node ---------------------------------------------------
		applyCh := make(chan raft.ApplyMsg, 128)
		node := raft.NewNode(id, peers, applyCh, db)

		// drain applyCh so it never blocks ---------------------------
		go func(ch <-chan raft.ApplyMsg) {
			for range ch { /* discard */
			}
		}(applyCh)

		// start HTTP listener ----------------------------------------
		go func(n *raft.Node, addr string) {
			n.Start()
			mux := http.NewServeMux()
			mux.Handle("/raft/", http.StripPrefix("/raft", node.Trans()))
			log.Fatal(http.ListenAndServe(addr, mux))
		}(node, addr)

		nodes = append(nodes, node)
	}

	// graceful shutdown ---------------------------------------------
	stop := func() {
		for _, n := range nodes {
			if n == nil {
				continue
			}
			n.Stop()
		}
		for _, db := range dbs {
			db.Close()
		}
	}

	return nodes, stop
}

// --------------------------------------------------
// Leader election + Replication
// --------------------------------------------------
func TestEndToEndReplication(t *testing.T) {
	nodes, stop := buildCluster(t, 5)
	defer stop()

	time.Sleep(4 * time.Second) // allow election
	var leader *raft.Node
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	idx, ok := leader.Propose(kv.SetCmd{Key: "foo", Value: "bar"})
	if !ok {
		t.Fatalf("propose failed")
	}

	// wait for all nodes to apply the entry
	for leader.LastApplied() < idx {
		time.Sleep(10 * time.Millisecond)
	}

	deadline := time.Now().Add(10 * time.Second)
	for _, n := range nodes {
		id := n.ID()
		for n.LastApplied() < idx {
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not apply idx=%d", id, idx)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// --------------------------------------------------
// Concurrency: many writes in parallel
// --------------------------------------------------
func TestConcurrentWrites(t *testing.T) {
	nodes, stop := buildCluster(t, 1)
	defer stop()
	time.Sleep(2 * time.Second)

	var leader *raft.Node
	for _, n := range nodes {
		if n.State() == raft.Leader {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected")
	}

	entry_count := 1000

	var wg sync.WaitGroup
	for i := 0; i < entry_count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, ok := leader.Propose(kv.SetCmd{Key: fmt.Sprintf("k%02d", i), Value: "x"}); !ok {
				t.Errorf("propose %d failed", i)
			}
		}(i)
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()

	// wait for all nodes to apply the entry
	deadline := time.Now().Add(20 * time.Second)
	for _, n := range nodes {
		id := n.ID()
		for n.LastApplied() < entry_count {
			if time.Now().After(deadline) {
				t.Fatalf("node %s did not apply all entries", id)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
