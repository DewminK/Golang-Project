package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/internal/kv"
	"mini_etcd/internal/raft"
)

func main() {
	id := flag.String("id", "node1", "Node ID")
	addr := flag.String("addr", ":9001", "Listen address")
	peersStr := flag.String("peers", "", "Comma list id=addr")
	flag.Parse()

	// ---------- cluster topology ----------
	peers := map[string]string{}
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.Split(p, "=")
			peers[parts[0]] = parts[1]
		}
	}

	// ---------- raft + kv ----------
	dbPath := filepath.Join(".temp", "raft_"+*id+".bolt")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 0})
	if err != nil {
		log.Fatalf("[ERROR] %s: %v", *id, err)
	}
	store := kv.New(db, 20)
	applyCh := make(chan raft.ApplyMsg, 64)
	node := raft.NewNode(*id, peers, applyCh, db)

	go func() { // apply committed commands
		for msg := range applyCh {
			if msg.CommandValid {
				store.Apply(msg.Command)
			}
		}
	}()
	node.Start() // ticker only

	// ---------- http mux (single listener) ----------
	mux := http.NewServeMux()

	// Raft RPCs under /raft/*
	mux.Handle("/raft/", http.StripPrefix("/raft", node.Trans()))

	// PUT
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key, Value string }
		_ = json.NewDecoder(r.Body).Decode(&body)

		idx, ok := node.Propose(kv.SetCmd{Key: body.Key, Value: body.Value})
		if !ok {
			http.Error(w, "not leader", http.StatusTemporaryRedirect)
			return
		}
		for node.LastApplied() < idx { // wait for commit
			time.Sleep(10 * time.Millisecond)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// GET
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		v := store.Get(body.Key)
		_ = json.NewEncoder(w).Encode(struct{ Value string }{Value: v})
	})

	// DEL
	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		_ = json.NewDecoder(r.Body).Decode(&body)

		idx, ok := node.Propose(kv.DelCmd{Key: body.Key})
		if !ok {
			http.Error(w, "not leader", http.StatusTemporaryRedirect)
			return
		}
		for node.LastApplied() < idx { // wait for commit
			time.Sleep(10 * time.Millisecond)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	log.Printf("[INFO] %s listening on %s", *id, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
