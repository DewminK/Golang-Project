### `truncate_hint.md`

The log store has a public method

```
TruncateBefore(cutoffIndex int)
```

which is intended to **permanently discard** every entry whose index is _strictly less than_ `cutoffIndex` and to update all bookkeeping so the log now begins at that point. Two test-scenarios exercise this:

1. A test calls `TruncateBefore` directly on a very large log and then
   appends another command.
2. A single-node run appends more than  
   `config.PruneEvery` entries; the leader should compact
   automatically, keeping only `config.RetainTail` recent items.

The tests look for three externally-visible effects:

| Expected after compaction                                                                                                            | Why it matters                                                          |
| ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------- |
| `FirstIndex()` jumps forward to the cut-off (and stays there across a restart).                                                      | Followers and crash-recovery rely on this to know where the log begins. |
| Old entries are _gone_ for good—subsequent proposals do **not** replay or duplicate commands that were committed before the cut-off. | Prevents state-machine divergence.                                      |
| The underlying Bolt file stops growing (or shrinks).                                                                                 | Indicates stale keys were actually removed from the database.           |

During actual runs the test harness notices that **at least one** of the
above expectations is violated.  
