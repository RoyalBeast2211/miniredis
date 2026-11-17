package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"miniredis/protocol"
	"miniredis/storage"
)

// SyncPolicy defines how frequently data is flushed and synced to physical storage.
type SyncPolicy string

const (
	// SyncAlways forces an fsync to disk on every single write.
	// Maximum durability, lowest throughput.
	SyncAlways SyncPolicy = "always"

	// SyncEverySecond flushes and fsyncs data in the background once every second.
	// Balanced durability (at most 1 second of data loss on power crash) and high throughput.
	// Standard Redis default.
	SyncEverySecond SyncPolicy = "everysec"

	// SyncNo relies entirely on the operating system kernel to flush dirty pages.
	// Maximum throughput, lowest durability (can lose 30+ seconds of data on power crash).
	SyncNo SyncPolicy = "no"
)

// AOF manages writing state-changing commands to an Append-Only File
// and supports safe online compaction via Rewrite.
type AOF struct {
	path       string
	file       *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	policy     SyncPolicy
	stopSync   chan struct{}
	closed     bool
	rewriting  bool
	rewriteBuf [][]byte

	// syncing indicates whether a background disk fsync is currently running.
	syncing atomic.Bool
	// syncWG tracks in-flight background periodic fsync workers during shutdown.
	syncWG sync.WaitGroup

	// Test hooks for fault injection (nil in production).
	onBeforeWrite         func() error
	onBeforeSync          func() error
	onBeforeRewriteRename func(tempPath string) error

	// onRewriteBeforeSwap is an optional internal hook used in tests to inject
	// deterministic concurrent writes while a rewrite is in progress.
	onRewriteBeforeSwap func()
}

// SetFaultHooks configures test-only hooks to simulate failures during persistence operations.
func (a *AOF) SetFaultHooks(onWrite, onSync func() error, onRewriteRename func(string) error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.onBeforeWrite = onWrite
	a.onBeforeSync = onSync
	a.onBeforeRewriteRename = onRewriteRename
}

// encodeCommand serializes a command slice into a RESP Array of Bulk Strings.
// For example: ["SET", "key", "val"] -> "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n"
func encodeCommand(args []string) []byte {
	elements := make([]protocol.Value, len(args))
	for i, arg := range args {
		elements[i] = protocol.NewBulkString(arg)
	}
	return protocol.NewArray(elements).Encode()
}

// Open creates or opens an AOF file in append-only write mode with the given sync policy.
func Open(path string, policy SyncPolicy) (*AOF, error) {
	// Open file with flags:
	// - os.O_CREATE: create the file if it does not already exist
	// - os.O_WRONLY: open for writing only
	// - os.O_APPEND: all writes automatically append to the end of the file
	// - 0644: standard read/write permissions for owner, read-only for others
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	a := &AOF{
		path:     path,
		file:     file,
		writer:   bufio.NewWriter(file),
		policy:   policy,
		stopSync: make(chan struct{}),
	}

	// Start periodic background fsync worker if policy is SyncEverySecond
	if policy == SyncEverySecond {
		go a.startPeriodicSync(1 * time.Second)
	}

	return a, nil
}

// Write serializes a command and its arguments into RESP Array format and appends it to the file.
// It is thread-safe and can be called concurrently by multiple client goroutines.
// If an AOF rewrite is currently running, the command is ALSO captured in rewriteBuf
// so it will be appended to the new compacted AOF before atomic replacement.
func (a *AOF) Write(args []string) error {
	if len(args) == 0 {
		return errors.New("aof: empty command")
	}

	encoded := encodeCommand(args)

	// Lock to prevent interleaved writes from concurrent client goroutines.
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.onBeforeWrite != nil {
		if err := a.onBeforeWrite(); err != nil {
			return err
		}
	}

	if a.closed {
		return errors.New("aof: file is closed")
	}

	// Write encoded RESP bytes to the active AOF user-space buffer.
	if _, err := a.writer.Write(encoded); err != nil {
		return err
	}

	// If a rewrite is currently running, record a copy in the incremental rewrite buffer.
	// Because we hold a.mu, the order in rewriteBuf strictly matches the order written to disk.
	if a.rewriting {
		cmdCopy := make([]byte, len(encoded))
		copy(cmdCopy, encoded)
		a.rewriteBuf = append(a.rewriteBuf, cmdCopy)
	}

	// Apply durability policy to the active file.
	switch a.policy {
	case SyncAlways:
		if err := a.writer.Flush(); err != nil {
			return err
		}
		return a.file.Sync()

	case SyncNo:
		return a.writer.Flush()

	case SyncEverySecond:
		// Leave bytes in bufio.Writer; the background worker flushes and syncs once per second.
		return nil

	default:
		return a.writer.Flush()
	}
}

// Sync flushes buffered data to the OS page cache and issues an fsync syscall.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.onBeforeSync != nil {
		if err := a.onBeforeSync(); err != nil {
			return err
		}
	}

	if a.closed {
		return errors.New("aof: file is closed")
	}

	if err := a.writer.Flush(); err != nil {
		return err
	}
	return a.file.Sync()
}

// Offset flushes user-space buffers and returns the exact current end-of-file byte offset.
// Used when taking a snapshot to record the exact AOF position corresponding to that snapshot.
func (a *AOF) Offset() (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return 0, errors.New("aof: file is closed")
	}

	if err := a.writer.Flush(); err != nil {
		return 0, err
	}

	return a.file.Seek(0, io.SeekEnd)
}

// RewriteResult summarizes the outcome of an AOF compaction rewrite.
type RewriteResult struct {
	KeysWritten         int   // Number of live keys written from the snapshot
	IncrementalCommands int   // Number of concurrent commands drained from rewriteBuf
	NewFileSize         int64 // Final size of the compacted AOF file in bytes
}

// Rewrite compacts the AOF file so it represents only the minimal set of commands
// needed to reconstruct the current database state (plus any concurrent writes).
//
// Execution flow:
//  1. Brief Lock: Enable incremental buffering (rewriting=true) and capture a consistent
//     point-in-time snapshot of live keys via store.SnapshotData().
//  2. Unlocked Disk I/O: Write SET and EXPIREAT commands for live keys to a temporary file
//     (path + ".rewrite.tmp"), flush, and fsync. Live clients continue writing without blocking.
//  3. Brief Lock & Atomic Swap: Drain all commands accumulated in rewriteBuf into the temp file,
//     flush, fsync, close temp file, remove obsolete snapshot file (if provided), atomically
//     rename temp file over the live AOF, sync parent directory, and reopen the active AOF handle.
func (a *AOF) Rewrite(store *storage.Store, snapshotPath string) (RewriteResult, error) {
	if store == nil {
		return RewriteResult{}, errors.New("aof: store cannot be nil")
	}

	// Phase 1: Capture point-in-time snapshot and start incremental rewrite buffer.
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return RewriteResult{}, errors.New("aof: file is closed")
	}
	if a.rewriting {
		a.mu.Unlock()
		return RewriteResult{}, errors.New("aof: rewrite already in progress")
	}

	a.rewriting = true
	a.rewriteBuf = nil
	entries := store.SnapshotData()
	hook := a.onRewriteBeforeSwap
	a.mu.Unlock()

	// Helper to reset rewrite state cleanly if an error occurs before atomic swap.
	abortRewrite := func(tempPath string, tempFile *os.File) {
		if tempFile != nil {
			_ = tempFile.Close()
		}
		if tempPath != "" {
			_ = os.Remove(tempPath)
		}
		a.mu.Lock()
		a.rewriting = false
		a.rewriteBuf = nil
		a.mu.Unlock()
	}

	// Phase 2 (Unlocked): Write compacted baseline state to temporary file.
	tempPath := a.path + ".rewrite.tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		abortRewrite("", nil)
		return RewriteResult{}, fmt.Errorf("aof: failed to create rewrite temp file: %w", err)
	}

	tempWriter := bufio.NewWriter(tempFile)
	now := time.Now()
	keysWritten := 0

	for _, entry := range entries {
		// Skip keys that expired between SnapshotData() and writing.
		if entry.HasExpire && now.After(entry.ExpireAt) {
			continue
		}

		// Write SET command for the key's current value.
		if _, err := tempWriter.Write(encodeCommand([]string{"SET", entry.Key, entry.Value})); err != nil {
			abortRewrite(tempPath, tempFile)
			return RewriteResult{}, fmt.Errorf("aof: failed writing SET to rewrite file: %w", err)
		}

		// If the key has a TTL, preserve its exact absolute deadline via EXPIREAT.
		// Persistent keys (including keys that had PERSIST run on them) have HasExpire == false,
		// so a single SET reconstructs them cleanly.
		if entry.HasExpire {
			deadlineStr := strconv.FormatInt(entry.ExpireAt.Unix(), 10)
			if _, err := tempWriter.Write(encodeCommand([]string{"EXPIREAT", entry.Key, deadlineStr})); err != nil {
				abortRewrite(tempPath, tempFile)
				return RewriteResult{}, fmt.Errorf("aof: failed writing EXPIREAT to rewrite file: %w", err)
			}
		}

		keysWritten++
	}

	// Flush and fsync the baseline temp file before taking the lock again,
	// keeping the final critical section as short as possible.
	if err := tempWriter.Flush(); err != nil {
		abortRewrite(tempPath, tempFile)
		return RewriteResult{}, fmt.Errorf("aof: failed flushing rewrite temp file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		abortRewrite(tempPath, tempFile)
		return RewriteResult{}, fmt.Errorf("aof: failed syncing rewrite temp file: %w", err)
	}

	// Optional hook used in unit tests to simulate concurrent writes arriving right before swap.
	if hook != nil {
		hook()
	}

	// Phase 3 (Locked): Drain incremental concurrent writes and atomically swap files.
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, errors.New("aof: file closed during rewrite")
	}

	// Append all concurrent writes that arrived during Phase 2 in exact arrival order.
	incrementalCount := len(a.rewriteBuf)
	for _, cmdBytes := range a.rewriteBuf {
		if _, err := tempWriter.Write(cmdBytes); err != nil {
			_ = tempFile.Close()
			_ = os.Remove(tempPath)
			a.rewriting = false
			a.rewriteBuf = nil
			return RewriteResult{}, fmt.Errorf("aof: failed writing incremental buffer: %w", err)
		}
	}

	// Final flush and fsync to guarantee every byte is durable on physical media before rename.
	if err := tempWriter.Flush(); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, fmt.Errorf("aof: failed final flush of rewrite file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, fmt.Errorf("aof: failed final sync of rewrite file: %w", err)
	}

	tempInfo, err := tempFile.Stat()
	if err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, fmt.Errorf("aof: failed stat on rewrite file: %w", err)
	}
	newFileSize := tempInfo.Size()

	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, fmt.Errorf("aof: failed closing rewrite file: %w", err)
	}

	// Ensure old AOF buffer is flushed before checking its size.
	_ = a.writer.Flush()
	oldInfo, _ := a.file.Stat()
	oldHasData := oldInfo != nil && oldInfo.Size() > 0

	// Snapshot consistency:
	// An existing snapshot's AOFOffset refers to byte positions in the OLD AOF file.
	// If the old AOF is non-empty, remove the obsolete snapshot BEFORE renaming so that
	// even a crash between Remove and Rename safely recovers from the complete old AOF.
	if snapshotPath != "" && oldHasData {
		if err := os.Remove(snapshotPath); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(tempPath)
			a.rewriting = false
			a.rewriteBuf = nil
			return RewriteResult{}, fmt.Errorf("aof: failed removing obsolete snapshot: %w", err)
		}
	}

	// Atomically replace the live AOF path with the compacted temporary file.
	if a.onBeforeRewriteRename != nil {
		if err := a.onBeforeRewriteRename(tempPath); err != nil {
			_ = os.Remove(tempPath)
			a.rewriting = false
			a.rewriteBuf = nil
			return RewriteResult{}, err
		}
	}
	if err := os.Rename(tempPath, a.path); err != nil {
		_ = os.Remove(tempPath)
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, fmt.Errorf("aof: atomic rename failed: %w", err)
	}

	// If the old AOF was empty (e.g. server booted purely from snapshot), remove the old
	// snapshot AFTER the new compacted AOF is safely in place.
	if snapshotPath != "" && !oldHasData {
		_ = os.Remove(snapshotPath)
	}

	// Sync parent directory to persist the directory entry rename on POSIX filesystems.
	if dirFile, err := os.Open(filepath.Dir(a.path)); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}

	// Reopen the newly replaced AOF path for future client appends and close the old handle.
	newFile, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		a.rewriting = false
		a.rewriteBuf = nil
		return RewriteResult{}, fmt.Errorf("aof: failed to reopen rewritten AOF: %w", err)
	}

	oldFile := a.file
	a.file = newFile
	a.writer = bufio.NewWriter(newFile)
	a.rewriting = false
	a.rewriteBuf = nil
	_ = oldFile.Close()

	return RewriteResult{
		KeysWritten:         keysWritten,
		IncrementalCommands: incrementalCount,
		NewFileSize:         newFileSize,
	}, nil
}

// Close flushes all remaining buffered writes, performs a final fsync, and closes the file.
func (a *AOF) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true

	// Stop background sync goroutine if running
	if a.policy == SyncEverySecond {
		close(a.stopSync)
	}

	// Flush any pending bytes in user-space buffer to OS page cache
	flushErr := a.writer.Flush()
	fileToClose := a.file
	a.mu.Unlock()

	// Wait for any in-flight background periodic fsync to finish
	a.syncWG.Wait()

	a.mu.Lock()
	defer a.mu.Unlock()

	// Force OS to flush kernel buffers to physical disk
	syncErr := fileToClose.Sync()

	// Close the operating system file handle
	closeErr := fileToClose.Close()

	if flushErr != nil {
		return flushErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// startPeriodicSync runs a background ticker that periodically flushes and syncs the AOF file.
// To avoid blocking client write goroutines during long-running OS disk fsync calls,
// the buffer flush is executed under lock, but the disk fsync is offloaded to a background worker.
func (a *AOF) startPeriodicSync(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopSync:
			return
		case <-ticker.C:
			// 1. Flush user-space bufio.Writer to OS kernel page cache under lock (~1 microsecond)
			a.mu.Lock()
			if a.closed {
				a.mu.Unlock()
				return
			}
			_ = a.writer.Flush()
			fileToSync := a.file
			a.mu.Unlock()

			// 2. Perform blocking disk fsync in background without holding a.mu.
			// If a previous disk fsync is still in flight, skip queuing a duplicate.
			if a.syncing.CompareAndSwap(false, true) {
				a.syncWG.Add(1)
				go func(f *os.File) {
					defer a.syncWG.Done()
					defer a.syncing.Store(false)
					_ = f.Sync()
				}(fileToSync)
			}
		}
	}
}

// RecoveryResult summarizes the outcome of replaying an AOF file.
type RecoveryResult struct {
	CommandsReplayed int
	TruncatedBytes   int64 // > 0 if an incomplete trailing record was safely truncated
}

// applyReplayCommand executes a state-changing command directly against the storage engine.
// It bypasses network sockets and does NOT write back to the AOF.
func applyReplayCommand(store *storage.Store, args []string) error {
	if len(args) == 0 {
		return errors.New("empty command in aof")
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "SET":
		if len(args) != 3 {
			return fmt.Errorf("malformed SET command in AOF: expected 3 args, got %d", len(args))
		}
		store.Set(args[1], args[2])

	case "DEL":
		if len(args) != 2 {
			return fmt.Errorf("malformed DEL command in AOF: expected 2 args, got %d", len(args))
		}
		store.Delete(args[1])

	case "EXPIREAT":
		if len(args) != 3 {
			return fmt.Errorf("malformed EXPIREAT command in AOF: expected 3 args, got %d", len(args))
		}
		unixSec, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid timestamp in EXPIREAT: %w", err)
		}
		store.ExpireAt(args[1], unixSec)

	case "PERSIST":
		if len(args) != 2 {
			return fmt.Errorf("malformed PERSIST command in AOF: expected 2 args, got %d", len(args))
		}
		store.Persist(args[1])

	default:
		return fmt.Errorf("unknown command %q in AOF", args[0])
	}

	return nil
}

// Recover opens an existing AOF file and replays all commands from the beginning.
func Recover(path string, store *storage.Store) (RecoveryResult, error) {
	return RecoverWithOffset(path, store, 0)
}

// RecoverWithOffset opens an existing AOF file, seeks to startOffset (obtained from a snapshot),
// and replays only the incremental commands recorded after that position.
//
// Startup cases:
// 1. Missing file: Returns (RecoveryResult{}, nil) representing a fresh first-time boot.
// 2. Empty file: Returns (RecoveryResult{}, nil) with 0 commands replayed.
// 3. AOF shorter than startOffset: Returns an error (consistency violation).
// 4. Valid file: Replays commands sequentially starting from startOffset.
// 5. Incomplete final record: Safely truncates the trailing partial record.
// 6. Corrupted record: Returns a descriptive error without skipping.
func RecoverWithOffset(path string, store *storage.Store, startOffset int64) (RecoveryResult, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return RecoveryResult{}, nil
		}
		return RecoveryResult{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return RecoveryResult{}, err
	}
	if info.Size() == 0 {
		return RecoveryResult{}, nil
	}

	if startOffset > 0 {
		if info.Size() < startOffset {
			return RecoveryResult{}, fmt.Errorf("aof: file size (%d) is shorter than snapshot AOF offset (%d)", info.Size(), startOffset)
		}
		if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
			return RecoveryResult{}, fmt.Errorf("aof: failed to seek to offset %d: %w", startOffset, err)
		}
	}

	reader := protocol.NewReader(file)
	var lastValidOffset int64 = startOffset
	commandsReplayed := 0

	for {
		currentFilePos, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return RecoveryResult{}, err
		}
		commandStartOffset := currentFilePos - int64(reader.Buffered())

		val, err := reader.ReadValue()
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Reached clean EOF
				break
			}

			// Check for partial/truncated record at the very end of the file
			if errors.Is(err, io.ErrUnexpectedEOF) || err.Error() == "protocol error: expected CRLF after bulk string data" {
				if commandStartOffset < info.Size() {
					if truncErr := file.Truncate(lastValidOffset); truncErr != nil {
						return RecoveryResult{}, fmt.Errorf("aof: failed to truncate partial record: %w", truncErr)
					}
					truncatedBytes := info.Size() - lastValidOffset
					return RecoveryResult{
						CommandsReplayed: commandsReplayed,
						TruncatedBytes:   truncatedBytes,
					}, nil
				}
			}

			// Any other syntax error or corruption in the middle of the file is fatal
			return RecoveryResult{}, fmt.Errorf("aof: corruption at byte offset %d: %w", commandStartOffset, err)
		}

		args, err := val.ToCommand()
		if err != nil {
			return RecoveryResult{}, fmt.Errorf("aof: invalid command structure at byte offset %d: %w", commandStartOffset, err)
		}

		if err := applyReplayCommand(store, args); err != nil {
			return RecoveryResult{}, fmt.Errorf("aof: replay failed at byte offset %d: %w", commandStartOffset, err)
		}

		commandsReplayed++

		currentFilePos, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			return RecoveryResult{}, err
		}
		lastValidOffset = currentFilePos - int64(reader.Buffered())
	}

	return RecoveryResult{
		CommandsReplayed: commandsReplayed,
		TruncatedBytes:   0,
	}, nil
}
