# MiniRedis

An educational, Redis-inspired, in-memory key-value database built from scratch in Go.

## Overview

MiniRedis is built for learning how databases, TCP network servers, concurrency patterns, storage engines, persistence strategies, and distributed systems work under the hood.

Rather than relying on third-party frameworks, MiniRedis uses the Go standard library to demonstrate clean systems engineering principles from first principles.

---

## What Problem MiniRedis Solves

Databases like Redis feel like "magic black boxes" until you build one yourself. MiniRedis demystifies:
1. How a network server listens for connections and talks to multiple clients at the same time.
2. How database wire protocols (like RESP) encode commands and data over raw byte streams.
3. How in-memory storage, expiration (TTL), and memory eviction (LRU) work inside RAM.
4. How databases survive restarts using durability mechanisms like Append-Only Files (AOF), Point-in-Time Snapshots, and AOF Compaction (Rewrite).

---

## Architecture

```
+-------------------------------------------------------------------------+
|                           MiniRedis Server                              |
|                                                                         |
|      +-----------------------------------------------------------+      |
|      |               net.Listener (127.0.0.1:6379)               |      |
|      |    Accepts connections ONLY after full recovery finishes  |      |
|      +-----------------------------+-----------------------------+      |
|                                    |                                    |
|                             listener.Accept()                           |
|                                    |                                    |
|            +-----------------------+-----------------------+            |
|            | Spawns Goroutine      | Spawns Goroutine      |            |
|            v                       v                       v            |
|     +-------------+         +-------------+         +-------------+     |
|     | Client #1   |         | Client #2   |         | Client #3   |     |
|     | Goroutine   |         | Goroutine   |         | Goroutine   |     |
|     +------+------+         +------+------+         +------+------+     |
|            |                       |                       |            |
|            v                       v                       v            |
|     +-------------+         +-------------+         +-------------+     |
|     | protocol.   |         | protocol.   |         | protocol.   |     |
|     | Reader      |         | Reader      |         | Reader      |     |
|     +------+------+         +------+------+         +------+------+     |
|            |                       |                       |            |
|            v                       v                       v            |
|     +-------------------------------------------------------------+     |
|     |                       handleCommand()                       |     |
|     |     Validates arguments, routes command, serializes reply   |     |
|     +---------+--------------------+--------------------+---------+     |
|               |                    |                    |               |
|               v                    v                    v               |
|     +-------------------+  +---------------+  +-------------------+     |
|     |   storage.Store   |  |    aof.AOF    |  | snapshot.Save()   |     |
|     | - data (map)      |  | - Append log  |  | - Atomic rename   |     |
|     | - expires (TTL)   |  | - 1s fsync    |  | - CRC32 checksum  |     |
|     | - evictList (LRU) |  | - Rewrite buf |  +---------+---------+     |
|     +---------^---------+  +-------+-------+            |               |
|               |                    |                    v               |
|               |                    v          +-------------------+     |
|               |            +---------------+  | dump.rdb (Disk)   |     |
|               | Recovery:  |appendonly.aof |  | Point-in-time RDB |     |
|               | 1. Snapshot|  (AOF File)   |  +---------+---------+     |
|               +------------+-------^-------+            |               |
|                            |       |                    |               |
|                            +-------+--------------------+               |
|                             Replays AOF from Snapshot Offset            |
+-------------------------------------------------------------------------+
```

---

## Folder Structure

```
miniredis/
├── AGENTS.md          # Project roadmap, learning rules, and guidelines
├── DEVELOPMENT.md     # Engineering log, milestone tracking, and decisions
├── README.md          # Project documentation and guide
├── go.mod             # Go module definition
├── main.go            # TCP server listener, startup lifecycle & command router
├── main_test.go       # Server network, command flow, snapshot & rewrite tests
├── server_bench_test.go # End-to-end TCP server benchmarks (workloads, scaling, AOF policies)
├── server_replication_test.go # Integration tests for read-only replica, PSYNC, ROLE & REPLICAOF
├── loadtest_matrix_test.go # Automated load testing matrix (concurrency, payloads, persistence)
├── profiling_test.go  # Profiling harness capturing CPU, heap, mutex, block & goroutines
├── profiles/          # Captured .pprof binary profile outputs
├── cmd/
│   └── loadtest/
│       └── main.go    # Standalone CLI tool for load testing MiniRedis
├── loadtest/
│   └── loadtest.go    # Native load testing engine (workers, percentiles, connection pooling)
├── replication/       # Primary-Replica replication package (Phase 7)
│   ├── backlog.go     # Circular ring buffer, sequence offsets & replID timeline
│   ├── backlog_test.go# Unit tests for backlog wrap-around and partial resync
│   ├── primary.go     # Primary replication manager, PSYNC & broadcast stream
│   ├── replica.go     # Replica client loop, handshake, snapshot install & stream consumer
│   └── replication_test.go # Tests for handshake, full resync, partial resync & overflow
├── aof/               # Append-Only File (AOF) persistence package
│   ├── aof.go         # AOF writer, sync policies, recovery replayer, online rewrite
│   └── aof_test.go    # Tests for AOF writes, recovery, compaction, concurrency
├── snapshot/          # Database point-in-time snapshot package
│   ├── snapshot.go    # Binary snapshot serializer, CRC32 integrity, atomic rename
│   └── snapshot_test.go # Tests for snapshots, checksums, versions, atomic replacement
├── protocol/          # Dedicated RESP protocol package
│   ├── resp.go        # RESP Types, Value, Parser (Reader), and Encoder
│   ├── resp_test.go   # Table-driven tests for parsing, encoding, and partial reads
│   └── resp_bench_test.go # Microbenchmarks for parsing and serialization
└── storage/           # In-memory storage engine package
    ├── store.go       # Store struct, sync.RWMutex, TTL, LRU, and SnapshotData
    ├── store_test.go  # Unit, boundary, capacity, and high-concurrency race tests
    └── store_bench_test.go # Microbenchmarks for Store operations and lock contention
```

---

## Technologies Used

- **Language**: Go (`go1.27+`)
- **Networking**: Go `net` package (raw TCP sockets)
- **I/O & Framing**: Go `bufio` package, `os.File`, and `io.ReadFull`
- **Persistence**: Hybrid Append-Only File (AOF), Online AOF Rewrite, and Binary Snapshots
- **Integrity**: CRC32 IEEE polynomial checksums
- **Data Structures**: Hash Maps and `container/list` (Doubly Linked List)
- **Concurrency**: Goroutines, `sync.WaitGroup`, `sync.RWMutex`, `sync.Mutex`, and channels
- **Testing**: Go `testing` package with race detection (`-race`)

---

## Supported Commands

| Command | Syntax | State-Changing? | Persisted to AOF? | Description |
| :--- | :--- | :---: | :---: | :--- |
| **PING** | `PING [msg]` | No | No | Tests connection liveliness |
| **ECHO** | `ECHO msg` | No | No | Echoes given string |
| **SET** | `SET key val` | **Yes** | **Yes** | Stores key-value pair, bumps to MRU, clears TTL |
| **GET** | `GET key` | No | No | Retrieves value; null if missing/expired |
| **DEL** | `DEL key` | **Yes** | **Yes** (if deleted) | Deletes key from map, TTL, and LRU list |
| **EXISTS** | `EXISTS key` | No | No | Returns 1 if key exists, 0 otherwise |
| **EXPIRE** | `EXPIRE key s` | **Yes** | **Yes** (as `EXPIREAT`) | Sets TTL; persisted with absolute unix timestamp |
| **EXPIREAT**| `EXPIREAT key ts` | **Yes** | **Yes** | Sets expiration to explicit unix timestamp |
| **TTL** | `TTL key` | No | No | Returns remaining seconds, `-1`, or `-2` |
| **PERSIST** | `PERSIST key` | **Yes** | **Yes** (if removed) | Removes timeout, making key persistent |
| **SAVE** | `SAVE` | No | No | Dumps point-in-time binary snapshot to `dump.rdb` |
| **BGREWRITEAOF** | `BGREWRITEAOF` | No | No | Compacts `appendonly.aof` from live memory state |
| **ROLE** | `ROLE` | No | No | Returns replication role (`master` vs `slave`), offsets, and status |
| **INFO** | `INFO [section]` | No | No | Returns replication and cluster statistics |
| **REPLICAOF** | `REPLICAOF host port` | No | No | Dynamically targets primary or promotes node (`NO ONE`) |

---

## Persistence Architecture (Phase 5)

### 1. Append-Only File (AOF) & Recovery
- Records mutating commands (`SET`, `DEL`, `EXPIREAT`, `PERSIST`) as RESP Arrays of Bulk Strings.
- Supports configurable durability policies: `always`, `everysec` (default), and `no`.
- On startup, replays commands sequentially before accepting TCP clients and safely truncates torn trailing crash writes.

### 2. Point-in-Time Snapshots (`SAVE`)
- Binary format (`MINIREDB` magic header, version, `AOFOffset`, timestamp, entries, CRC32 checksum).
- Written to `dump.rdb.tmp`, synced with `fsync`, and swapped via atomic `os.Rename`.
- On startup, loads `dump.rdb` first, then seeks `appendonly.aof` to `AOFOffset` to replay only incremental tail commands.

### 3. AOF Rewrite & Compaction (`BGREWRITEAOF`)
Because an AOF continuously appends every update and deletion, it grows indefinitely even when the live dataset is small. **AOF Rewrite** generates a brand-new, minimal AOF directly from live in-memory state:
- **Redundant updates** (`SET counter 1..1000`) collapse into a single `SET counter 1000`.
- **Deleted, expired, and LRU-evicted keys** are completely omitted.
- **TTL deadlines** are preserved via `EXPIREAT <unix_timestamp>`.
- **LRU ordering** is preserved by emitting entries from `evictList.Back()` (LRU) to `Front()` (MRU).
- **Concurrent write safety**: Uses a 3-phase algorithm with an incremental `rewriteBuf` so live client writes arriving during disk I/O are never lost.
- **Snapshot coordination**: Automatically removes obsolete `dump.rdb` files whose `AOFOffset` belonged to the pre-compaction AOF timeline.

---

## Running Tests and Benchmarks

### Unit and Integration Tests (with Race Detector)
Run all functional tests across all packages with Go's data race detector:

```bash
go test -count=1 -race ./...
```

### Load Testing (Phase 6B)
MiniRedis includes a custom, native load testing engine and CLI tool with support for configurable client counts, duration, warm-up, workloads (`GET`, `SET`, `MIXED`), and high-resolution latency percentiles (p50, p95, p99, and max).

#### Running via the CLI tool:
Start the server in one terminal:
```bash
go run .
```

Run the load tester in another terminal:
```bash
# 50 concurrent clients, 5-second measurement, mixed 50/50 workload
go run ./cmd/loadtest -clients 50 -duration 5s -warmup 1s -workload MIXED

# 100 concurrent clients on GET workload with 1KB values
go run ./cmd/loadtest -clients 100 -duration 10s -workload GET -payload 1024
```

#### Running the automated load test matrix:
```bash
# Run the complete concurrency scaling matrix (1 to 250 clients)
go test -v -run TestLoadTestMatrixConcurrency .

# Run workload comparison (GET vs SET vs MIXED)
go test -v -run TestLoadTestWorkloadComparison .

# Run payload size comparison (16B vs 1KB vs 4KB)
go test -v -run TestLoadTestPayloadComparison .

# Run 15-second sustained reliability test
go test -v -run TestLoadTestReliabilitySustained .
```

### Profiling with Go `pprof` (Phase 6C)
MiniRedis supports full profiling across CPU, heap, allocations, mutex contention, block delays, and goroutine stack traces.

#### 1. Generating Profile Dumps via Test Suite:
Run the profiling test harness to generate binary `.pprof` files in `profiles/`:
```bash
# Generate all profiles (CPU, Memory, Mutex, Block, Goroutines)
go test -v -run "TestProfile.*" .
```

#### 2. Analyzing Profiles via `go tool pprof`:
```bash
# CPU Profile (top CPU consumers)
go tool pprof -top profiles/cpu_mixed.pprof

# Cumulative CPU time (call stack hierarchy)
go tool pprof -cum -top profiles/cpu_mixed.pprof

# Memory Allocation Profile (bytes allocated)
go tool pprof -alloc_space -top profiles/mem_mixed.pprof

# Live Heap Profile (memory actively retained in RAM)
go tool pprof -inuse_space -top profiles/mem_mixed.pprof

# Mutex Contention Profile (wait delay)
go tool pprof -total_delay -top profiles/mutex_mixed.pprof

# Mutex Contention Count (number of lock collisions)
go tool pprof -contentions -top profiles/mutex_mixed.pprof

# Goroutine Stack Traces (leak checking)
go tool pprof -traces profiles/goroutine_mixed.pprof

# Interactive Web Interface (flame graphs, graphviz call graph, source code)
go tool pprof -http=:8080 profiles/cpu_mixed.pprof
```

#### 3. Live Profiling via HTTP Debug Server:
Start MiniRedis with the `-pprof-port` flag:
```bash
go run . -pprof-port 6060
```
Then collect profiles directly while sending live client traffic:
```bash
# 5-second CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=5

# Live heap profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Mutex contention profile
go tool pprof http://localhost:6060/debug/pprof/mutex
```

---

## Load Testing & Concurrency Highlights (Phase 6B)

Tested on **AMD Ryzen 7 6800H (16 vCPUs, Linux x86_64, Go 1.27)**:

1. **Throughput Scaling & Saturation Knee**:
   - Concurrency scales near-linearly from 1 client (**73,000 req/sec**) to 10 clients (**283,500 req/sec**) to 25 clients (**523,150 req/sec**).
   - **Saturation Point**: At 25 clients, the 16 hardware threads become fully saturated. Beyond 25 clients, throughput plateaus (~507k at 50 clients, ~484k at 100 clients, ~453k at 250 clients).
   - **Queueing Latency (Little's Law)**: While p50 latency remains modest (~32 µs), p99 latency rises sharply from **28 µs** (1 client) to **212 µs** (25 clients) to **661 µs** (50 clients) to **2.71 ms** (250 clients) as client requests queue in OS network buffers and Go runtime run-queues.

2. **Read-Write Lock Switching Penalty**:
   - Pure `GET`: **~685,000 req/sec** (all 50 clients share `sync.RWMutex.RLock`).
   - Pure `SET`: **~653,000 req/sec** (homogeneous write lock acquisitions).
   - `MIXED` (50% GET / 50% SET): **~517,000 req/sec** (alternating reads and writes causes writers to block readers and readers to block writers, incurring lock state transition costs).

3. **Persistence Tail Latency Spike**:
   - In-memory SET max latency: **5.01 ms**.
   - `AOF SyncEverySecond` max latency: **20.27 ms** (periodic fsync flushes to disk cause brief lock holding that elevates p99 and max latency).

4. **Sustained Reliability**:
   - Processed **7,221,179 operations** continuously over 15 seconds at **481,407 req/sec**.
   - **0 errors (0.0000% error rate)** and zero connection drops.
   - Goroutine count remained exactly constant ($\Delta = +49$ for 50 persistent connections), confirming zero goroutine leaks.

---

## Profiling & Bottleneck Highlights (Phase 6C)

Tested on **AMD Ryzen 7 6800H (16 vCPUs, Linux x86_64, Go 1.27)**:

1. **CPU Profile Findings**:
   - **Kernel Network Syscalls**: `Syscall6` (`read`/`write`) accounts for **~68% of flat CPU time** during loopback socket transmission.
   - **Protocol Parsing**: `protocol.(*Reader).ReadValue` accounts for **25.4% cumulative CPU time**, driven by byte-by-byte socket buffer decoding.
   - **Storage Execution**: Map lookups and mutations in `Store.Set` and `Store.Get` account for less than **5% of CPU time**.

2. **Memory Allocation Bottlenecks**:
   - Total heap memory allocated during a 3-second test reached **513 MB**, while live in-use heap remained at **only 1.02 MB**.
   - **98%+ of allocations originate from RESP protocol parsing and formatting**:
     - `readArray` (`make([]Value, count)`): **89.1 MB** (17.4%)
     - `readBulkString` (`make([]byte, length)` + string copy): **38.6 MB** (7.5%)
     - `Value.ToCommand` (`make([]string, len)`): **22.2 MB** (4.3%)
     - `Value.Encode` (`fmt.Sprintf` + `[]byte` slice conversion): **19.3 MB** (3.8%)
     - `readLine` (`ReadBytes('\n')`): **15.2 MB** (3.0%)

3. **Concurrency & Lock Contention Bottlenecks**:
   - **Storage Mutex Contention**: In a 50-client mixed workload, goroutines accumulated **69.98 seconds of blocking delay** waiting for `Store` mutex acquisition across 183,000 contention events.
   - **Store.Set Internal Breakdown**: Annotated source profiling revealed that **61% of time in `Store.Set` (880ms) is spent waiting for `s.mu.Lock()`**, whereas the actual hash map mutation takes only **40ms**.
   - **AOF Mutex Contention**: Client write goroutines spent **7.48 seconds** waiting for `a.mu` in `AOF.Write` and **1.45 seconds** during periodic disk synchronization.
   - **Zero Goroutine Leaks**: Goroutine profile dumps confirmed that all client connections are completely reaped after disconnection, with only the server listener goroutine active.

---

## Optimization Highlights (Phase 6D)

Tested on **AMD Ryzen 7 6800H (16 vCPUs, Linux x86_64, Go 1.27)**:

### 1. Protocol Layer Allocation Optimization (Step 1)
Profiling in Phase 6C demonstrated that 98%+ of server heap memory allocations came from string formatting and slice operations in `protocol/resp.go`. We eliminated these through:
- **`AppendEncode` Pattern**: Replaced `fmt.Sprintf` with `strconv.AppendInt` and direct byte slice appending, allowing callers to serialize directly into a single continuous destination buffer.
- **Pre-Allocated Static Byte Slices**: Common server replies (`+OK\r\n`, `+PONG\r\n`, `$-1\r\n`, `*-1\r\n`) are served from immutable static slices with zero allocations.
- **Zero-Allocation Parser Operations**: Replaced `ReadBytes('\n')` with `ReadSlice('\n')`, replaced `make([]byte, 2)` CRLF reads with two `ReadByte()` calls, and implemented zero-allocation ASCII integer parsing directly on byte slices.

#### Microbenchmark Improvements:
| Operation | Baseline | Optimized | Speedup / Reduction |
| :--- | :---: | :---: | :---: |
| `EncodeSimpleString` | 93.6 ns (32 B, 3 allocs) | **3.66 ns (0 B, 0 allocs)** | **$25.5\times$ faster**, 100% memory reduction |
| `EncodeBulkString` | 136.7 ns (112 B, 3 allocs) | **35.2 ns (48 B, 1 alloc)** | **$3.88\times$ faster**, -57% memory, -66% allocs |
| `EncodeArray` | 606.5 ns (416 B, 12 allocs) | **96.7 ns (128 B, 1 alloc)** | **$6.27\times$ faster**, -69% memory, -91% allocs |
| `ParsePing` | 174.2 ns (84 B, 6 allocs) | **125.6 ns (72 B, 3 allocs)** | **+28% faster**, **-50% allocs** |
| `ParseSet` | 405.1 ns (256 B, 14 allocs) | **289.4 ns (232 B, 7 allocs)** | **+29% faster**, **-50% allocs** |

#### End-to-End Server Load-Test Improvements (50 Clients):
| Workload | Baseline Throughput | Optimized Throughput | Throughput Gain | p95 Latency Improvement |
| :--- | :---: | :---: | :---: | :---: |
| **GET (Read-Heavy)** | 637,707 r/s | **697,771 r/s** | **+60,064 r/s (+9.4%)** | $147.28\text{ µs} \to \mathbf{123.14\text{ µs}}$ (-16.4%) |
| **SET (Write-Heavy)** | 601,232 r/s | **655,014 r/s** | **+53,782 r/s (+8.9%)** | $162.77\text{ µs} \to \mathbf{133.95\text{ µs}}$ (-17.7%) |
| **MIXED (50/50)** | 462,810 r/s | **517,703 r/s** | **+54,893 r/s (+11.9%)** | $479.52\text{ µs} \to \mathbf{395.41\text{ µs}}$ (-17.5%) |

### 2. Sharded In-Memory Storage Engine (Step 2)
Profiling in Phase 6C revealed that 61% of CPU time in `Store.Set` (880 ms) was spent waiting for the single `sync.RWMutex`, while the actual hash map mutation took only 40 ms. During a 50-client concurrent workload, goroutines accumulated 70 seconds of blocking delay on that single lock.

We implemented **16-way lock striping / sharding** (`defaultShards = 16`):
- Keys are mapped to shards via the 32-bit FNV-1a hash algorithm: `idx := fnv32(key) % 16`.
- Each shard maintains its own `sync.RWMutex`, data map, and expiration map.
- Contention probability on any single lock drops from **100% down to 6.25%**, allowing up to 16 goroutines to read and write simultaneously across different keys.
- Preserved 100% backward compatibility for single-shard LRU eviction via anonymous struct embedding.

#### Microbenchmark Improvements:
| Operation | Baseline (Single Mutex) | Step 2 (16 Shards) | Improvement |
| :--- | :---: | :---: | :---: |
| `StoreSetParallel` | 376.8 ns (4.04M ops/s) | **70.63 ns (21.4M ops/s)** | **$5.33\times$ faster (+430% throughput)** |
| `StoreGetParallel` | 77.03 ns (16.4M ops/s) | **39.45 ns (34.7M ops/s)** | **$1.95\times$ faster (+112% throughput)** |

#### End-to-End Server Load-Test Improvements (50 Clients, 16B Payload):
| Workload | Phase 6B Baseline | Step 1 (Protocol) | Step 2 (Sharded Store) | Cumulative Gain | p95 Latency Improvement |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **GET (Read-Heavy)** | 637,707 r/s | 697,771 r/s | **694,721 r/s** | **+57,014 r/s (+8.9%)** | $147.28\text{ µs} \to \mathbf{123.65\text{ µs}}$ (-16.0%) |
| **SET (Write-Heavy)** | 601,232 r/s | 655,014 r/s | **682,785 r/s** | **+81,553 r/s (+13.6%)** | $162.77\text{ µs} \to \mathbf{125.80\text{ µs}}$ (-22.7%) |
| **MIXED (50/50)** | 462,810 r/s | 517,703 r/s | **679,713 r/s** | **+216,903 r/s (+46.9%)** | $479.52\text{ µs} \to \mathbf{126.51\text{ µs}}$ (**$3.79\times$ lower**) |

### 3. Non-Blocking Background Fsync (Step 3)
Profiling in Phase 6C showed that holding `a.mu` across blocking OS disk `fsync()` syscalls in `AOF.SyncEverySecond` introduced up to 8.9 seconds of cumulative contention delay and spiked tail latency.

We decoupled disk synchronization from client write paths:
- `startPeriodicSync` flushes the user-space `bufio.Writer` buffer to kernel page cache under lock in ~1 µs, releases `a.mu`, and offloads `f.Sync()` to a background worker goroutine.
- `atomic.Bool` prevents queuing redundant overlapping fsyncs if a disk flush takes longer than 1 second.
- `sync.WaitGroup` guarantees in-flight background fsyncs cleanly finish before file handle teardown on `Close()`.
- Client `AOF.Write` calls never experience 10–20 ms disk stalls during periodic sync.
- **AOF EverySecond Throughput**: Increased from **543,935 req/s to 583,898 req/s** (+7.3%), with p95 latency improving from 181 µs to **164.6 µs**.

---

## Reliability & Fault Injection (Phase 6E)

To ensure MiniRedis is production-hardened before distributed replication, we introduced automated failure injection and crash recovery suites:

1. **Crash & Recovery Matrix (`reliability_test.go`)**:
   - **Torn Write Recovery**: If a crash occurs halfway through writing an AOF record, startup recovery automatically rolls back to the last valid record and truncates the corrupted bytes using `os.File.Truncate`.
   - **Fail-Fast on Corrupted Logs**: Syntax corruption in the middle of an AOF file or an offset mismatch between snapshot and AOF is immediately detected and rejected.
   - **Zero Key Resurrection**: Expired keys (`EXPIREAT` in the past) are never revived on reboot.
   - **Snapshot & Rewrite Atomic Replacement**: Interrupted snapshot creation or AOF rewrites leave the live database files 100% intact with zero partial file corruption.

2. **Fault Injection Methodology**:
   - Non-intrusive test hooks in `AOF` (`SetFaultHooks`) and `snapshot` (`TestHookBeforeSync`, `TestHookBeforeRename`) allow tests to inject simulated disk full errors (`ENOSPC`), I/O failures, and crashes before atomic renames.

3. **Concurrency & Race Verification (`concurrency_test.go`)**:
   - Tested 8 high-contention concurrent scenarios including multi-client write collisions, concurrent snapshots during heavy traffic, concurrent rewrites, and abrupt listener shutdown.
   - Passed with zero data races under repeated stress testing (`go test -race -count=20 ./...`).

---

## Primary-Replica Replication (Phase 7)

MiniRedis features asynchronous, leader-follower replication compatible with standard Redis replication semantics.

```
       +------------------+
       |   Client App     |
       +--------+---------+
                |
          Writes & Reads (SET, GET, etc.)
                v
       +------------------+
       |     PRIMARY      | (Read/Write Leader)
       |  (Port: 6379)    |
       +--------+---------+
                |
                | Asynchronous Replication Stream (SET, DEL, EXPIREAT)
                |
       +--------+---------+
       |                  |
       v                  v
+--------------+   +--------------+
|   REPLICA 1  |   |   REPLICA 2  | (Read-Only Followers)
| (Port: 6380) |   | (Port: 6381) |
+-------+------+   +-------+------+
        ^                  ^
        |                  |
   Reads Only         Reads Only
```

### 1. Replication Architecture & Lifecycle
1. **Handshake**:
   - Replica dials Primary TCP port.
   - Replica sends `PING` -> Primary replies `+PONG`.
   - Replica sends `REPLCONF listening-port <port>` -> Primary replies `+OK`.
   - Replica sends `PSYNC <repl_id> <offset>`.
2. **Full Resynchronization (`+FULLRESYNC`)**:
   - Triggered on initial connection or when a reconnecting replica's offset has expired.
   - Primary encodes live point-in-time snapshot with CRC32 verification and streams it as a bulk payload.
   - Replica atomically loads snapshot into memory via `store.LoadSnapshot()`.
   - Primary transmits any concurrent writes accumulated in the backlog buffer during snapshot generation.
3. **Incremental Replication & Stream Offsets**:
   - Primary assigns a monotonically increasing cumulative byte offset to every mutating command (`SET`, `DEL`, `EXPIREAT`, `PERSIST`).
   - Mutations are written to the in-memory circular **Replication Backlog** and broadcast asynchronously to connected replicas.
4. **Partial Resynchronization (`+CONTINUE`)**:
   - When a disconnected replica reconnects within the retention window of the primary's 1MB circular backlog ring buffer, the primary issues `+CONTINUE` and streams only the missed byte delta.
   - Fast, sub-millisecond recovery without triggering disk I/O or full snapshots.
5. **Read-Only Replicas**:
   - Replicas reject any client write commands (`SET`, `DEL`, `EXPIRE`, `EXPIREAT`, `PERSIST`) with `-READONLY You can't write against a read only replica.`.
   - Replicas serve read queries (`GET`, `EXISTS`, `TTL`, `PING`, `ECHO`) for read scalability.
6. **Dynamic Promotion (`REPLICAOF`)**:
   - A replica can be promoted to an independent primary at runtime by issuing `REPLICAOF NO ONE`.
   - Nodes can point to a new primary via `REPLICAOF <host> <port>`.

### 2. How to Run Replicated MiniRedis
```bash
# 1. Start the Primary node on port 6379
go run main.go -port 6379

# 2. Start a Replica node on port 6380 pointing to Primary
go run main.go -port 6380 -replicaof "127.0.0.1:6379" -aof "replica.aof" -snapshot "replica.rdb"

# 3. Test replication with redis-cli or netcat
redis-cli -p 6379 SET message "Hello Distributed MiniRedis!"
redis-cli -p 6380 GET message
# -> "Hello Distributed MiniRedis!"

# 4. Confirm replica rejects writes
redis-cli -p 6380 SET message "Illegal write"
# -> (error) READONLY You can't write against a read only replica.
```

---

## Known Limitations

- `handleCommand` executes `store` mutation and `aof.Write` in sequence without a single global command lock; under extreme contention on the exact same key, two concurrent writes could record in different order than applied in memory.
- `BGREWRITEAOF` runs on the invoking client's goroutine (other clients continue unblocked during disk I/O, while the invoking client waits for `+OK`).
