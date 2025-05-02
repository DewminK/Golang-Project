### `raft_hint.md`

The build you’ve been given **starts and listens**, but the cluster never
really comes to life.  
Below are the _visible_ symptoms you can reproduce with nothing more
than the provided `server` / `client` binaries and a couple of shells.

---

| Experiment                                                | What you’ll see                                                                                                                     |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Spin-up 3 nodes**                                       | Minutes go by and _no_ leader is ever elected. Every `put` you send comes back _“not leader”_.                                      |
| **Single-node cluster**                                   | The lone node also refuses to lead; it stays forever in “candidate” cycles.                                                         |
| **Patch things so a leader finally appears**              | Log replication works in the sense that followers’ `commitIndex` marches forward, but reads on followers always return the old value. |
| **Restart a follower that was down during heavy traffic** | After the restart it still thinks its log is up-to-date, yet it never serves the latest values.                                     |

---
