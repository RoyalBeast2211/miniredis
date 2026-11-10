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
