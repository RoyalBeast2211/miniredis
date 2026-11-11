package aof

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"miniredis/protocol"
)

type SyncPolicy string

const (
	SyncAlways      SyncPolicy = "always"
	SyncEverySecond SyncPolicy = "everysec"
	SyncNo          SyncPolicy = "no"
)

type AOF struct {
	policy SyncPolicy

	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

func encodeCommand(args []string) []byte {
	return protocol.EncodeCommand(args...)
}

func Open(path string, policy SyncPolicy) (*AOF, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open aof file: %w", err)
	}
	return &AOF{
		file:   file,
		writer: bufio.NewWriter(file),
		policy: policy,
	}, nil
}

func (a *AOF) Write(args []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	data := encodeCommand(args)
	if _, err := a.writer.Write(data); err != nil {
		return err
	}
	return a.writer.Flush()
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.writer != nil {
		_ = a.writer.Flush()
	}
	if a.file != nil {
		return a.file.Close()
	}
	return nil
}

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
