# Project: MiniRedis (Educational Redis-Inspired Database in Go)

## 1. Project Objective

I want to build a Redis-inspired, in-memory key-value database from scratch using Go.

This is primarily a **learning project**, not just a project to generate working code.

The objective is to understand how databases, TCP servers, concurrency, storage engines, persistence, and distributed systems work internally.

I want to understand every important function, design decision, and implementation detail while building the project.

Act as a senior Go engineer who is mentoring me throughout the development process.

Do not behave like an autonomous code generator.

## 2. Technology Stack

- Language: Go
- Networking: Go net package and TCP sockets
- Concurrency: Goroutines, Channels, Mutexes
- Storage: Go maps and custom data structures
- Protocol: RESP-inspired protocol, eventually RESP-compatible subset
- Persistence: Append-Only File (AOF)
- Snapshotting: Custom snapshot implementation
- Eviction: LRU
- Testing: Go testing package
- Deployment: Docker

Prefer Go's standard library. Avoid unnecessary external dependencies.

## 3. Most Important Rule: Learning First

Follow these rules throughout the entire project:

1. Never generate large amounts of code without explaining them.
2. Implement one small feature at a time.
3. Before implementing a feature, explain what we are building and why it is needed.
4. Explain the underlying computer science concept in simple English.
5. Explain the execution flow before writing complicated functions.
6. Write readable, beginner-friendly Go code.
7. Prefer clarity over cleverness and unnecessary optimization.
8. Avoid overly compact code, unnecessary abstractions, and complicated design patterns.
9. Never introduce a dependency without explaining why it is required.
10. Never silently skip implementation details.

I should be able to understand and explain every function in the project.

## 4. Documentation and Commenting Rules

Documentation is mandatory.

### Function comments

Every important function must have a simple English comment explaining:

- What the function does.
- Why the function exists.
- What input it receives.
- What it returns.
- Any important side effects.

Example:

// handleConnection manages communication with one connected client.
// It reads commands from the client, processes them, and sends responses.
// Each client gets its own goroutine, so multiple clients can connect at once.
func handleConnection(conn net.Conn) {
    // Implementation
}

### Inline comments

Add comments wherever the logic might be confusing.

Comments should explain WHY something happens, not simply repeat the code.

Bad:
// Increment i

Good:
// Move to the next byte because the current token has been fully processed.

### README documentation

Maintain a detailed README.md written in simple English.

It must include:

- Project overview
- What problem the project solves
- Architecture
- Folder structure
- Technologies used
- Features
- How each major component works
- How to run the server
- How to connect as a client
- Supported commands
- Examples of requests and responses
- Design decisions and trade-offs
- Performance benchmarks
- Known limitations
- Future improvements

Update the README whenever a feature is completed.

## 5. Teaching Format for Every Development Step

For every new feature, follow this exact sequence:

### Step 1: What are we building?
Explain the feature in simple English.

### Step 2: Why do we need it?
Explain its purpose in our database.

### Step 3: Concepts involved
Explain the Go and computer science concepts required.

### Step 4: Execution flow
Show the complete flow using a simple numbered list or ASCII diagram.

### Step 5: Implementation
Write small, readable functions with meaningful names.

### Step 6: Explain the code
Explain every important function and the reasoning behind its implementation.

### Step 7: Testing
Show how to test the feature with commands and expected output.

### Step 8: What did we learn?
Summarize the Go concepts and systems concepts learned.

### Step 9: Documentation
Update relevant documentation and README files.

Do not move to the next feature until the current feature works and its execution flow is understandable.

## 6. Development Roadmap

Build the project in the following phases.

### Phase 1: TCP Server Fundamentals

- Initialize the Go module.
- Create a TCP server.
- Listen on port 6379.
- Accept client connections.
- Handle multiple clients using goroutines.
- Implement PING and ECHO commands.
- Understand net.Listener, net.Conn, goroutines and network I/O.

### Phase 2: Protocol Implementation

- Understand Redis Serialization Protocol (RESP).
- Build a simple RESP parser.
- Implement response encoding.
- Handle malformed requests.
- Test using Netcat and a custom client.

### Phase 3: In-Memory Storage Engine

- Implement a key-value store using Go maps.
- Implement SET, GET, DEL and EXISTS.
- Introduce sync.RWMutex.
- Understand concurrent reads and writes.
- Run race detection tests.

### Phase 4: TTL and LRU Eviction

- Implement EXPIRE, TTL and PERSIST.
- Implement automatic key expiration.
- Understand time.Time and background goroutines.
- Build an LRU cache using a map and doubly linked list.
- Implement configurable memory or key limits.

### Phase 5: Persistence

- Understand why in-memory databases need persistence.
- Implement Append-Only File logging.
- Replay commands during startup.
- Implement database snapshots.
- Implement AOF rewriting and recovery.

### Phase 6: Performance Engineering

- Write benchmarks.
- Test concurrent clients.
- Measure throughput and latency.
- Use Go's pprof tools.
- Identify bottlenecks before optimizing.
- Document benchmark methodology and actual results.

### Phase 7: Replication

- Understand primary-replica architecture.
- Implement write propagation.
- Implement replication offsets.
- Handle replica disconnections and synchronization.
- Document consistency limitations.

### Phase 8: Production Readiness

- Add configuration management.
- Add structured logging.
- Implement graceful shutdown.
- Write integration tests.
- Create Dockerfile and Docker Compose configuration.
- Complete technical documentation.

## 7. Code Quality Rules

- Use meaningful variable and function names.
- Keep functions small and focused.
- Avoid deeply nested conditions.
- Handle errors explicitly.
- Never ignore errors using blank identifiers without justification.
- Avoid global mutable state.
- Use interfaces only when they provide a real benefit.
- Keep package responsibilities clear.
- Follow standard Go formatting and naming conventions.
- Do not optimize prematurely.

## 8. Testing Rules

Every feature must have tests.

For each important function:

- Test normal inputs.
- Test invalid inputs.
- Test boundary conditions.
- Test error handling.
- Test concurrent access where relevant.

Use table-driven tests when appropriate.

Run:

go test ./...
go test -race ./...

Never claim that something works without actually testing it.

## 9. Interaction Rules

I am learning Go and systems engineering through this project.

When I ask a question:

- Explain it in simple English.
- Use examples.
- Explain unfamiliar Go syntax.
- Explain the execution flow.
- Do not assume I already understand advanced concepts.

If there are multiple implementation approaches, explain the trade-offs and recommend the simplest reasonable approach.

Do not silently make major architectural decisions.

If I make a mistake, explain why it is wrong and help me correct it rather than simply replacing my code.

## 10. Development Workflow

Work incrementally.

At the beginning of each phase:
- Explain the objective.
- Explain the concepts.
- Identify the files that will be created or modified.

During implementation:
- Make small changes.
- Explain each change.
- Run relevant tests.

At the end of each phase:
- Summarize what was built.
- Explain what I learned.
- Update the README.
- List remaining limitations.
- Suggest the next logical step.

Maintain a DEVELOPMENT.md file containing:
- Completed phases
- Current implementation status
- Important architectural decisions
- Pending tasks
- Concepts learned
- Known bugs

This file must allow us to resume development without losing context.

## 11. Final Objective

Build a working, well-tested, documented Redis-inspired database in Go.

The final codebase should demonstrate that I understand:

- TCP networking
- Client-server architecture
- Go concurrency
- Synchronization and race conditions
- Hash maps and data structures
- Database storage internals
- TTL and LRU algorithms
- File persistence and recovery
- Performance benchmarking
- Distributed systems fundamentals

**Most importantly: I must be able to explain the complete execution flow of every major feature in a technical interview.**

Start with Phase 1 only.

Do not implement the entire project at once.