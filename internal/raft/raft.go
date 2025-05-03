package raft

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/config"
	"mini_etcd/internal/transport"
)

// ------------------------------------------------------------
// Raft node state & constructor
// ------------------------------------------------------------

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string { return [...]string{"Follower", "Candidate", "Leader"}[s] }

type Node struct {
	mu sync.RWMutex

	// identity & topology
	id    string
	peers map[string]string // peerID -> addr

	// persistent state and caching variables
	currentTerm int
	votedFor    string
	db          *bolt.DB
	log         StableLog
	store       StableStore

	// volatile state
	commitIndex int
	lastApplied int

	// leader-only state
	nextIndex  map[string]int
	matchIndex map[string]int

	// runtime plumbing
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	stopCh         chan struct{}

	// transport
	trans *transport.HTTPTransport
}

func NewNode(id string, peers map[string]string, applyCh chan ApplyMsg, db *bolt.DB) *Node {
	n := &Node{
		id:         id,
		peers:      peers,
		db:         db,
		log:        NewBoltLog(db),
		store:      NewBoltStore(db),
		state:      Follower,
		applyCh:    applyCh,
		stopCh:     make(chan struct{}),
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
	}

	n.currentTerm = n.store.Term()
	n.votedFor = n.store.VotedFor()
	n.lastApplied = n.store.LastApplied()

	n.resetElectionTimer()
	n.trans = transport.New(n.handleInbound)
	return n
}

// ------------------------------------------------------------
// Public API
// ------------------------------------------------------------

func (n *Node) Serve(addr string) error {
	svr := &http.Server{Addr: addr, Handler: n.trans}
	go n.ticker()
	return svr.ListenAndServe()
}

func (n *Node) Start() {
	go n.ticker()
}

func (n *Node) Stop() { close(n.stopCh) }

// Propose replicates a command **only the leader**.
func (n *Node) Propose(cmd any) (idx int, ok bool) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return -1, false
	}
	idx = n.log.Append(LogEntry{Term: n.currentTerm, Command: cmd})

	// ---------- single-node fast commit ----------------
	if len(n.peers) == 0 {
		n.commitIndex = idx
		n.applyCommitted() // apply locally
		n.maybePrune()

		n.mu.Unlock()

		return idx, true
	}
	// ---------------------------------------------------

	n.mu.Unlock()

	go n.broadcastAppendEntries()
	return idx, true
}

// ------------------------------------------------------------
// Ticker goroutine – drives elections & heart-beats
// ------------------------------------------------------------

func (n *Node) ticker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.RLock()
			isLeader := n.state == Leader
			n.mu.RUnlock()

			if !isLeader { // only followers / candidates start elections
				// run election logic in its *own* goroutine so the ticker
				// continues to service heart-beats and term updates.
				go n.startElection()
			}
		case <-n.heartbeatTimerC():
			n.mu.RLock()
			leader := n.state == Leader
			n.mu.RUnlock()
			if leader {
				n.broadcastAppendEntries()
			}
			n.heartbeatTimer.Reset(config.HeartbeatInterval)
		}
	}
}

func (n *Node) startHeartbeatTimer() {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
	} else {
		n.heartbeatTimer.Reset(config.HeartbeatInterval)
	}
}

func (n *Node) heartbeatTimerC() <-chan time.Time {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
		if !n.heartbeatTimer.Stop() {
			<-n.heartbeatTimer.C
		}
	}
	return n.heartbeatTimer.C
}

func (n *Node) resetElectionTimer() {
	d := time.Duration(rand.Intn(int(config.ElectionTimeoutMax-config.ElectionTimeoutMin))) + config.ElectionTimeoutMin
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(d)
	} else {
		n.electionTimer.Reset(d)
	}
}

// ------------------------------------------------------------
// Elections
// ------------------------------------------------------------

func (n *Node) startElection() {
	// step up to candidate & bump term
	n.mu.Lock()
	n.state = Candidate

	n.currentTerm++
	n.store.SetTerm(n.currentTerm)

	n.votedFor = n.id
	n.store.SetVotedFor(n.id)

	term := n.currentTerm
	lastIdx, lastTerm := n.log.LastIndexTerm()
	n.resetElectionTimer()

	// copy peers map so we can iterate after releasing the lock
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}
	n.mu.Unlock()

	var votes int32 = 1 // self-vote
	var wg sync.WaitGroup

	for pid, paddr := range peerAddrs {
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			args := RequestVoteArgs{Term: term, CandidateID: n.id, LastLogIndex: lastIdx, LastLogTerm: lastTerm}
			var reply RequestVoteReply
			if err := n.trans.Call(addr, transport.RPCRequestVote, &args, &reply); err != nil {
				return
			}
			if reply.Term > term {
				n.mu.Lock()
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				return
			}
			if reply.VoteGranted && reply.Term == term {
				if atomic.AddInt32(&votes, 1) > int32(len(n.peers)/2) {
					n.mu.Lock()
					if n.state == Candidate && n.currentTerm == term {
						n.becomeLeader()
					}
					n.mu.Unlock()
				}
			}
		}(pid, paddr)
	}
	wg.Wait()
}

// ------------------------------------------------------------
// Leader transition helpers
// ------------------------------------------------------------

func (n *Node) becomeFollower(term int) {
	n.state = Follower

	n.currentTerm = term
	n.store.SetTerm(term)

	n.votedFor = ""
	n.store.SetVotedFor("")

	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	n.state = Leader
	// init nextIndex/matchIndex
	lastIdx := n.log.LastIndex() + 1
	for id := range n.peers {
		if id == n.id {
			continue
		}
		n.nextIndex[id] = lastIdx
		n.matchIndex[id] = 0
	}
	// send initial heart-beat outside the lock
	go n.broadcastAppendEntries()
	n.startHeartbeatTimer()
}

func (n *Node) broadcastAppendEntries() {
	// capture a *snapshot* of leader’s state under read-lock
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	term := n.currentTerm
	commitIdx := n.commitIndex
	nextIdxSnap := make(map[string]int, len(n.nextIndex))
	for k, v := range n.nextIndex {
		nextIdxSnap[k] = v
	}
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}
	n.mu.RUnlock()

	for pid, addr := range peerAddrs {
		ni := nextIdxSnap[pid]
		go func(id, paddr string, next int) {
			prevIdx := next - 1
			prevTerm := 0
			if prevIdx > 0 {
				if e, ok := n.log.At(prevIdx); ok {
					prevTerm = e.Term
				}
			}
			// gather entries [next .. last]
			entries := make([]LogEntry, 0)
			for i := next; i <= n.log.LastIndex(); i++ {
				if e, ok := n.log.At(i); ok {
					entries = append(entries, e)
				}
			}
			args := AppendEntriesArgs{Term: term, LeaderID: n.id, PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: commitIdx}
			var reply AppendEntriesReply
			if err := n.trans.Call(paddr, transport.RPCAppendEntries, &args, &reply); err != nil {
				return // network error – ignore, follower will timeout
			}
			n.handleAppendEntriesReply(id, &reply)
		}(pid, addr, ni)
	}
}

func (n *Node) handleAppendEntriesReply(id string, reply *AppendEntriesReply) {
	if reply.Term > n.currentTerm {
		n.mu.Lock()
		n.becomeFollower(reply.Term)
		n.mu.Unlock()
	} else if reply.Success {
		n.mu.Lock()
		if n.state == Leader {
			n.matchIndex[id] = reply.MatchIndex
			n.nextIndex[id] = reply.MatchIndex + 1
			n.maybeCommit()
		}
		n.mu.Unlock()
	} else {
		n.mu.Lock()
		n.nextIndex[id] = max(1, n.nextIndex[id]-1)
		n.mu.Unlock()
	}
}

// ------------------------------------------------------------
// Commit & Apply
// ------------------------------------------------------------

func (n *Node) maybeCommit() {
	for idx := n.commitIndex + 1; idx <= n.log.LastIndex(); idx++ {
		if n.log[idx].Term == n.currentTerm {
			matchCount := 1 // self is a match
			for _, v := range n.matchIndex {
				if v >= idx {
					matchCount++
				}
			}
			if matchCount > len(n.peers)/2 {
				n.commitIndex = idx
				n.maybePrune()
				break
			}
		}
	}
}

func (n *Node) applyCommitted() {
	for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
		if e, ok := n.log.At(i); ok {
			n.lastApplied = i
			n.applyCh <- ApplyMsg{Index: i, Command: e.Command}
		}
	}
}

// ------------------------------------------------------------
// RPCs
// ------------------------------------------------------------

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	MatchIndex  int
}

type ApplyMsg struct {
	Index   int
	Command any
}

// LogEntry is a struct representing each log entry.
type LogEntry struct {
	Term    int
	Command any
}

type StableLog interface {
	Append(entry LogEntry) int
	LastIndexTerm() (int, int)
	LastIndex() int
	At(idx int) (LogEntry, bool)
}

func NewBoltLog(db *bolt.DB) StableLog {
	// Implementation here.
	return nil
}

type StableStore interface {
	Term() int
	VotedFor() string
	LastApplied() int
	SetTerm(int)
	SetVotedFor(string)
	SetLastApplied(int)
}

func NewBoltStore(db *bolt.DB) StableStore {
	// Implementation here.
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
