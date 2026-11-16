# MiniRedis - Development Log

## Current Status
- **Current Phase**: Phase 8: Production Readiness
- **Sub-Phase**: Complete
- **State**: Completed all 8 phases of the MiniRedis roadmap. Successfully implemented configuration management with file and flag cascading, structured JSON/text logging via `log/slog`, atomic telemetry metrics, unified Server struct with graceful SIGINT/SIGTERM shutdown, HTTP liveness/readiness health probes, multi-stage unprivileged Docker containerization, Docker Compose cluster orchestration, automated GitHub Actions CI matrix, and 100% test coverage with zero data races (`go test -race ./...`).

---

## Completed Phases & Milestones

### Phase 1: TCP Server Fundamentals (Completed)
- [x] Initialized Go module (`miniredis`).
- [x] Implemented minimal TCP server listening on `127.0.0.1:6379`.
- [x] Implemented concurrent connection acceptance using Go goroutines.
- [x] Added unit tests using `net.Pipe` (in-memory) and ephemeral port TCP tests (`go test -race`).

### Phase 2: Protocol Implementation (Completed)
- [x] Created dedicated `protocol` package (`protocol/resp.go`).
- [x] Implemented parsing of RESP Arrays, Bulk Strings, Simple Strings, Integers, and Errors.
- [x] Implemented response encoding for Simple Strings (`+`), Errors (`-`), Integers (`:`), Bulk Strings (`$`), Null (`$-1\r\n`), and Arrays (`*`).
- [x] Handled TCP packet fragmentation and partial network reads via `io.ReadFull`.
- [x] Supported pipelined / sequential commands on a single connection.
- [x] Exhaustive table-driven unit tests (`protocol/resp_test.go`).

### Phase 3: In-Memory Storage Engine (Completed)
- [x] Created dedicated `storage` package (`storage/store.go`).
- [x] Implemented thread-safe methods: `NewStore()`, `Set()`, `Get()`, `Delete()`, `Exists()`.
- [x] Used `sync.RWMutex` with `RLock()`/`RUnlock()` for readers and `Lock()`/`Unlock()` for writers.
- [x] Integrated storage engine with network server in `main.go`.
- [x] Implemented command routing for `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`.

### Phase 4A: TTL Support (Completed)
- [x] Added `expires map[string]time.Time` to `storage.Store`.
- [x] Implemented lazy expiration and active background cleanup.
- [x] Added commands `EXPIRE`, `TTL`, `PERSIST`.
- [x] Ensured `SET` clears existing TTL.

### Phase 4B: LRU Eviction (Completed)
- [x] Added `evictList *list.List` (`container/list`) and `elements map[string]*list.Element` to `storage.Store`.
- [x] Configurable capacity via `maxKeys` (0 = unlimited, > 0 = fixed LRU capacity).
- [x] Implemented $O(1)$ recency tracking for `GET` and `SET`.
- [x] Seamless TTL integration: expired keys cleaned before evicting live keys.

### Phase 5A: Append-Only File (AOF) Persistence (Completed)
- [x] Created dedicated `aof` package (`aof/aof.go`).
- [x] Implemented AOF file creation and appending via `os.OpenFile(..., os.O_APPEND|os.O_CREATE|os.O_WRONLY)`.
- [x] Reused RESP protocol serialization for AOF command logging.
- [x] Logged state-changing commands (`SET`, `DEL`, `EXPIREAT`, `PERSIST`).
- [x] Preserved absolute expiration deadlines via `EXPIREAT <unix_timestamp>`.
- [x] Implemented 3 durability policies: `always`, `everysec`, and `no`.

### Phase 5B: AOF Recovery and Replay (Completed)
- [x] Implemented `aof.Recover(path, store)` to read and replay logs into memory before accepting clients.
- [x] Replayed `SET`, `DEL`, `EXPIREAT`, and `PERSIST` without duplicate logging or client writes.
- [x] Implemented absolute TTL reconstruction and LRU write-sequence replay.
- [x] Handled incomplete trailing records with safe truncation via `file.Truncate()`.
- [x] Handled corrupted records in the middle of the file with fatal error reporting.

### Phase 5C: Database Snapshots (Completed)
- [x] Created dedicated `snapshot` package (`snapshot/snapshot.go`).
- [x] Designed binary-safe format with Magic Header (`MINIREDB`), versioning, `AOFOffset`, and CRC32 checksum.
- [x] Implemented atomic file replacement via `.tmp` + `fsync` + `os.Rename` + parent directory sync.
- [x] Integrated snapshot loading + incremental `aof.RecoverWithOffset` into startup lifecycle.
- [x] Implemented `SAVE` command.

### Phase 5D: AOF Rewrite and Compaction (Completed)
- [x] Implemented `aof.Rewrite(store, snapshotPath)` to compact historical logs from live in-memory state.
- [x] Added incremental concurrent write buffering (`rewriting` flag + `rewriteBuf [][]byte`) so clients do not lose writes during unlocked disk I/O.
- [x] Emitted only live keys (`SET` + `EXPIREAT` for active TTLs); omitted deleted keys, expired keys, redundant updates, and LRU-evicted keys.
- [x] Preserved exact LRU recency order by iterating `evictList` from Back (LRU) to Front (MRU) in `store.SnapshotData()`.
- [x] Implemented atomic replacement via `.rewrite.tmp`, `Flush()`, `Sync()`, `Close()`, `os.Rename()`, directory `Sync()`, and safe active handle reopen.
- [x] Solved Snapshot + AOF Rewrite consistency by invalidating obsolete snapshots whose `AOFOffset` referenced the pre-compaction file timeline.
- [x] Updated `aof.Offset()` to use `Seek(0, io.SeekEnd)` so offsets remain accurate immediately after reopen/rewrite.
- [x] Added `BGREWRITEAOF` command to `main.go`.
- [x] Added comprehensive tests for empty DB, compaction, TTL preservation, deterministic concurrent interleaving, multi-goroutine stress testing, LRU preservation, failed/interrupted rewrites, and snapshot invalidation under `go test -race`.

### Phase 6A: Benchmarking and Baseline Performance (Completed)
- [x] Implemented storage microbenchmarks in `storage/store_bench_test.go` (`SET`, `GET` with/without LRU, parallel scaling, `DEL`, `EXISTS`, `TTL`, LRU eviction).
- [x] Implemented RESP protocol microbenchmarks in `protocol/resp_bench_test.go` (parsing `PING`, `SET`, encoding simple strings, bulk strings, arrays).
- [x] Implemented end-to-end TCP server benchmarks in `server_bench_test.go` on ephemeral port `127.0.0.1:0` with persistent client connections.
- [x] Measured payload size impacts (16B vs 1KB) and concurrency scaling (1, 10, 50 concurrent client connections).
- [x] Measured AOF durability sync policies on NVMe SSD (`NoAOF`, `SyncEverySecond`, `SyncNo`, `SyncAlways`).
- [x] Quantified write lock contention under LRU: `BenchmarkStoreGetLRUParallel` (304 ns) vs `BenchmarkStoreGetParallel` (80 ns) — nearly 4x slowdown due to exclusive write lock on `evictList.MoveToFront`.
- [x] Quantified fsync bottleneck: `AOF_SyncAlways` drops throughput by ~98.5% compared to `AOF_SyncEverySecond` (991 ops/sec vs 64,850 ops/sec) due to synchronous disk flushing.
- [x] Documented complete baseline numbers and measurement methodology.

### Phase 6B: Load Testing and Concurrency Analysis (Completed)
- [x] Built native, zero-dependency load testing engine in `loadtest/loadtest.go` supporting configurable clients, duration, warm-up, workloads (`GET`, `SET`, `MIXED`), and key universes.
- [x] Built standalone CLI load test runner in `cmd/loadtest/main.go` with configurable command-line flags.
- [x] Implemented automated test matrix in `loadtest_matrix_test.go` covering concurrency scaling (1 to 250 clients), workloads, payloads (16B to 4KB), and persistence impact.
- [x] Captured exact latency percentiles (p50, p95, p99, and Max) across millions of requests with per-worker non-contending sample aggregation.
- [x] Identified throughput saturation point at 25 clients (~523k r/s on 16 vCPUs); observed throughput plateau and latency queueing (p99 climbing from 28 µs to 2.71 ms at 250 clients).
- [x] Identified read-write lock switching overhead in MIXED workload (517k r/s) compared to pure GET (685k r/s) and pure SET (653k r/s).
- [x] Observed periodic AOF sync tail latency spikes (Max latency 20.27 ms in `SyncEverySecond` vs 5.01 ms in in-memory mode).
- [x] Verified system stability under sustained load: 15-second test with 50 concurrent clients processed 7,221,179 operations at 481,407 r/s with 0 errors (0.0000% error rate) and zero goroutine leaks.

### Phase 6C: Profiling and Bottleneck Analysis (Completed)
- [x] Built automated profiling test suite in `profiling_test.go` generating CPU, heap, allocation, mutex, block, and goroutine profiles in `profiles/`.
- [x] Added optional `-pprof-port` HTTP endpoint flag to `main.go` for live runtime profiling.
- [x] **CPU Profiling**: Identified that network syscalls (`Syscall6`, `Read`, `Write`) account for ~68% of flat CPU time over loopback, with `protocol.(*Reader).ReadValue` accounting for 25.4% cumulative CPU time.
- [x] **Memory Allocation Profiling**: Discovered that 98%+ of server heap allocations originate from RESP protocol framing:
  - `readArray` (`make([]Value, count)`): 89 MB (17.4%)
  - `readBulkString` (`make([]byte, length)` + `string(buf)`): 38.6 MB (7.5%)
  - `Value.ToCommand` (`make([]string, len)`): 22.2 MB (4.3%)
  - `Value.Encode` (`fmt.Sprintf` + `[]byte` copies): 19.3 MB (3.8%)
  - `readLine` (`ReadBytes('\n')`): 15.2 MB (3.0%)
- [x] **In-Use Heap vs Allocations**: Confirmed that live retained heap memory is only ~1 MB (pure key-value map storage in `Store.Set`), while total allocated space across the test reached 513 MB, causing significant GC pressure.
- [x] **Mutex Contention Profiling**: Measured 69.98 seconds of cumulative blocking delay across 50 clients on `Store` mutex lock/unlock during a 3s MIXED test, with 158,894 contentions in `Store.Set` and 24,384 in `Store.Get`.
- [x] **Store.Set Internal Contention**: Source-level annotation revealed that 61% of execution time in `Store.Set` is spent waiting for `s.mu.Lock()` (880ms) compared to only 40ms for the actual map mutation.
- [x] **AOF Lock Impact**: Mutex profiling confirmed 7.48s of delay in `AOF.Write` and 1.45s in `AOF.startPeriodicSync`.
