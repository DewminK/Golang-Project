### `store_hint.md`

The key-value layer is meant to behave like a tiny “etcd”: every `SET`
is durable, every `GET` is up-to-date, deletes really delete, and a tiny
in-memory cache should just make things faster.

The public test-runner focuses on three broad behaviours:

| Area the tests probe | What you might notice when **running** the binary                                                           |
| -------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Durability**       | After a clean restart, some previously written keys are unexpectedly empty.                                 |
| **Freshness**        | A read performed immediately after a write can return an older value, yet a later read returns the new one. |
| **Cache discipline** | With a very small cache limit, the store sometimes keeps the wrong keys hot (or forgets the wrong ones).    |

The harness also spawns many goroutines that hammer the store
concurrently; race detection is enabled.
