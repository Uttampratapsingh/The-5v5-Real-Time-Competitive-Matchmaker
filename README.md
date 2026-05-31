## Architecture Overview
The system is a single-process, in-memory matchmaker with a tiny-http ingestion layer, a sharded pool, and worker threads that form 5v5 matches. Players are inserted into buckets keyed by (skill_bracket, region), and each bucket stores a BTreeMap keyed by rating, enabling ordered range scans without a global lock. Each worker executes the Ranked Sweep loop: choose a pivot bucket using the oldest waiting player, scan the rating window, and atomically evict ten candidates once a match is found. The HTTP layer accepts POST/DELETE for queue operations and proxies metrics to match the external API requirements. Metrics are collected through atomics and a small rolling buffer, then served via a dedicated metrics thread that never blocks the match loop. Match output is emitted as JSON lines to stdout, which the simulator reads to compute per-player wait times and percentile statistics.

ASCII diagram:

+-----------------------+    +----------------------------+    +-----------------------+    +-------------------+
| HTTP Ingestion (8080) | -> | Sharded Pool (N shards)    | -> | Worker Threads (M)    | -> | Match Output       |
| POST/DELETE, /metrics |    | BTreeMap buckets per shard |    | Ranked Sweep loop     |    | JSON logs (stdout) |
+-----------------------+    +----------------------------+    +-----------------------+    +-------------------+
                                      ^
                                      |
                              +-------------------+
                              | Metrics (9090)    |
                              | Atomics + buffer  |
                              +-------------------+

## Engineering Challenges & Solutions
1. Latency vs Match Quality: The Ranked Sweep approach centers the initial window at the median rating of the bucket containing the oldest-waiting player, then searches for 10 candidates inside ±150 SR. This keeps early matches tight and avoids pulling in distant ratings while queues are healthy. If the base window cannot fill a lobby, the time-based expansion safely trades a small amount of match quality for forward progress so the oldest player does not starve.

2. Thread-Safe Atomic Eviction: Each player carries an atomic evicted flag and lives in exactly one shard bucket. When a worker commits to a 10-player match, it locks only the shards that contain those candidates, verifies presence, and then uses a compare-and-swap to flip each evicted flag before removing them in the same critical section. This prevents double-matching, even under concurrent scans, without relying on a global lock.

3. Time-Based Constraint Relaxation: The expansion rule is applied only when the base window cannot form a 10-player lobby. The expanded window is computed as window = 150 + (wait_secs * 50) and then capped at ±600 SR. This keeps the initial search strict for normal traffic while allowing the window to widen deterministically as the oldest player’s wait time grows.

4. Team Balance: After 10 candidates are evicted, they are sorted by skill rating and split using the snake draft indices [0,3,5,7,9] and [1,2,4,6,8]. The O(n log n) sort ensures deterministic, reproducible teams, and the snake split avoids the complexity of integer programming while still minimizing average SR delta. The match record includes team averages, delta, and a normalized quality score where 1.0 is perfectly balanced and 0.0 represents the worst permitted spread.

5. Low-Latency Metrics: total_matches_formed, total_players_queued, and queue_depth are atomic counters updated on the hot path. avg_wait_ms uses a 100-sample circular buffer protected by its own mutex, decoupled from the pool locks. The /metrics endpoint on port 9090 runs in a dedicated thread and reads only atomics plus that small buffer, keeping the match loop lock-free with respect to monitoring.

## Algorithmic Trade-offs
BTreeMap was chosen over HashMap because range scans are core to the Ranked Sweep. HashMap would require full scans or additional indices, while BTreeMap offers ordered iteration and efficient range queries with O(log n) access that map cleanly to the windowed search. The snake draft split is used instead of linear programming to keep the balance step fast and deterministic; LP could squeeze out a few points of balance but would add latency and code complexity on a hot path. Sharding the pool across independent mutexes reduces contention compared to a single RwLock, particularly under heavy ingress where workers can scan shards in parallel. The trade-off is that multi-shard eviction needs careful ordering and atomic flags, but that complexity is contained and deterministic.

## Time & Space Complexity
| Operation            | Time       | Space  |
|----------------------|------------|--------|
| Enqueue player       | O(log n)   | O(1)   |
| Window scan          | O(k)       | O(1)   |
| Atomic eviction (x10)| O(log n)   | O(1)   |
| Team balance         | O(n log n) | O(n)   |
| Metrics read         | O(1)       | O(1)   |

## Scaling Discussion
Beyond a single process, the same design can be lifted to a distributed system by consistent-hash sharding on (skill_bracket, region) into Redis Sorted Sets, preserving ordered range scans and allowing workers to query only the relevant shards. Match results can be published over Pub/Sub so ingestion services can notify clients or update external state. Horizontal worker pods can scale match formation, while ingress pods handle HTTP and push enqueued players into the shard store. Metrics can be aggregated per shard and rolled up into a time-series system for cluster-wide visibility. For fault tolerance, shard ownership can be assigned via a coordinator and rebalanced on node failure, and match logs can be persisted to an append-only stream for downstream analytics.

## How to Run
```bash
  cargo build --release
  ./target/release/matchmaker &
  python3 simulate.py
```
Optional: `python3 simulate.py --profile benchmark` skews regions; `--fast-cross-region` enables immediate cross-region (non-spec); use `--players`/`--concurrency` for bigger loads.
Optional modes:
- Fast cross-region: `MATCHMAKER_FAST_CROSS_REGION=1 ./target/release/matchmaker`
- Benchmark distribution: `python3 simulate.py --profile benchmark`
