package config

import "time"

// Timing constants tuned for local tests.
const (
	ElectionTimeoutMin = 150 * time.Millisecond
	ElectionTimeoutMax = 300 * time.Millisecond
	HeartbeatInterval  = 30 * time.Millisecond

	RetainTail = 100
	PruneEvery = 2000
)
