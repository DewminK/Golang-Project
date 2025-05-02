package raft

/*
RequestVote RPC
------------------------------------------------------------
- Term: The candidate’s current term. Helps identify outdated messages.
- CandidateID: Identifies the server requesting votes.
- LastLogIndex, LastLogTerm: Information about the candidate's log.
			These are used by the receiver to decide whether to grant the vote,
			based on which log is more up-to-date.
*/

type RequestVoteArgs struct {
	Term         int    // candidate’s term
	CandidateID  string // candidate requesting vote
	LastLogIndex int
	LastLogTerm  int
}

/*
- VoteGranted: Whether the recipient granted its vote.
- Term: The responder’s term—if it's higher, the candidate should step down because it’s outdated.
*/

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

/*
AppendEntries RPC — heartbeats
------------------------------------------------------------
- PrevLogIndex, PrevLogTerm: Used to enforce the log-matching property
			(i.e., if these entries don’t match the follower’s log, the follower rejects the request).
*/

type AppendEntriesArgs struct {
	Term         int    // leader’s term
	LeaderID     string // for redirects
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader’s commitIndex
}

/*
- Success: Whether the follower appended the entries successfully.
- Term: The follower’s current term, allowing the leader to detect if it is outdated.
*/

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prev params
}

// ApplyMsg delivers committed log entries to the state machine (KV store).
type ApplyMsg struct {
	CommandValid bool
	CommandIndex int
	Command      interface{}
}
