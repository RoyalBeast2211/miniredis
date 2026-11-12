package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"miniredis/storage"
)

const (
	// MagicHeader identifies a MiniRedis snapshot file (8 bytes).
	MagicHeader = "MINIREDB"

	// CurrentVersion defines the supported snapshot file format version.
	CurrentVersion uint32 = 1
)

// Header contains metadata stored at the start of every snapshot file.
type Header struct {
	Version   uint32
	AOFOffset int64
	CreatedAt int64
	KeyCount  uint32
}

// Test hooks for fault injection (nil in production).
var (
	// TestHookBeforeSync is called before file.Sync() in Save.
	TestHookBeforeSync func(file *os.File) error
	// TestHookBeforeRename is called before os.Rename() in Save.
	TestHookBeforeRename func(tempPath string) error
)

// Encode serializes in-memory snapshot entries along with the AOF offset
// into a binary byte slice including header and trailing CRC32 checksum.
// This format is used both for on-disk snapshots and over-the-network replication.
func Encode(aofOffset int64, entries []storage.SnapshotEntry) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 1. Write Magic Header (8 bytes)
	if _, err := buf.Write([]byte(MagicHeader)); err != nil {
		return nil, err
	}

	// 2. Write Version (4 bytes, BigEndian)
	if err := binary.Write(buf, binary.BigEndian, CurrentVersion); err != nil {
		return nil, err
	}

	// 3. Write AOFOffset (8 bytes, BigEndian)
	if err := binary.Write(buf, binary.BigEndian, aofOffset); err != nil {
		return nil, err
	}

	// 4. Write CreatedAt Unix timestamp (8 bytes, BigEndian)
	createdAt := time.Now().Unix()
	if err := binary.Write(buf, binary.BigEndian, createdAt); err != nil {
		return nil, err
	}

	// 5. Write KeyCount (4 bytes, BigEndian)
	keyCount := uint32(len(entries))
	if err := binary.Write(buf, binary.BigEndian, keyCount); err != nil {
		return nil, err
	}

	// 6. Write each key-value entry
	for _, entry := range entries {
		var hasExpire byte = 0
		if entry.HasExpire {
			hasExpire = 1
		}
		if err := binary.Write(buf, binary.BigEndian, hasExpire); err != nil {
			return nil, err
		}

		if entry.HasExpire {
			expireUnix := entry.ExpireAt.Unix()
			if err := binary.Write(buf, binary.BigEndian, expireUnix); err != nil {
				return nil, err
			}
		}

		// Key: length (4 bytes) + bytes
		keyBytes := []byte(entry.Key)
		if err := binary.Write(buf, binary.BigEndian, uint32(len(keyBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(keyBytes); err != nil {
			return nil, err
		}

		// Value: length (4 bytes) + bytes
		valBytes := []byte(entry.Value)
		if err := binary.Write(buf, binary.BigEndian, uint32(len(valBytes))); err != nil {
			return nil, err
		}
		if _, err := buf.Write(valBytes); err != nil {
			return nil, err
		}
	}

	// 7. Calculate and append CRC32 Checksum (4 bytes)
	checksum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.BigEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode reads, verifies CRC32 integrity, and deserializes binary snapshot data.
func Decode(data []byte) (Header, []storage.SnapshotEntry, error) {
	// Minimum size: 8 (magic) + 4 (version) + 8 (offset) + 8 (created) + 4 (count) + 4 (checksum) = 36 bytes
	if len(data) < 36 {
		return Header{}, nil, errors.New("snapshot: data too small or truncated")
	}

	payload := data[:len(data)-4]
	storedChecksum := binary.BigEndian.Uint32(data[len(data)-4:])

	// 1. Verify CRC32 Checksum
	computedChecksum := crc32.ChecksumIEEE(payload)
	if computedChecksum != storedChecksum {
		return Header{}, nil, fmt.Errorf("snapshot: checksum mismatch: computed %x, stored %x (file corrupted)",
			computedChecksum, storedChecksum)
	}

	reader := bytes.NewReader(payload)

	// Verify Magic Header
	magicBuf := make([]byte, len(MagicHeader))
	if _, err := io.ReadFull(reader, magicBuf); err != nil {
		return Header{}, nil, err
	}
	if string(magicBuf) != MagicHeader {
		return Header{}, nil, fmt.Errorf("snapshot: invalid magic header %q", string(magicBuf))
	}

	// Read Version
	var version uint32
	if err := binary.Read(reader, binary.BigEndian, &version); err != nil {
		return Header{}, nil, err
	}
	if version != CurrentVersion {
		return Header{}, nil, fmt.Errorf("snapshot: unsupported format version %d", version)
	}

	// Read AOFOffset
	var aofOffset int64
	if err := binary.Read(reader, binary.BigEndian, &aofOffset); err != nil {
		return Header{}, nil, err
	}

	// Read CreatedAt
	var createdAt int64
	if err := binary.Read(reader, binary.BigEndian, &createdAt); err != nil {
		return Header{}, nil, err
	}

	// Read KeyCount
	var keyCount uint32
	if err := binary.Read(reader, binary.BigEndian, &keyCount); err != nil {
		return Header{}, nil, err
	}

	header := Header{
		Version:   version,
		AOFOffset: aofOffset,
		CreatedAt: createdAt,
		KeyCount:  keyCount,
	}

	// Read Entries
	entries := make([]storage.SnapshotEntry, 0, keyCount)
	for i := uint32(0); i < keyCount; i++ {
		var hasExpire byte
		if err := binary.Read(reader, binary.BigEndian, &hasExpire); err != nil {
			return Header{}, nil, err
		}

		var expireAt time.Time
		if hasExpire == 1 {
			var expireUnix int64
			if err := binary.Read(reader, binary.BigEndian, &expireUnix); err != nil {
				return Header{}, nil, err
			}
			expireAt = time.Unix(expireUnix, 0)
		}

		// Key
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return Header{}, nil, err
		}
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuf); err != nil {
			return Header{}, nil, err
		}

		// Value
		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return Header{}, nil, err
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBuf); err != nil {
			return Header{}, nil, err
		}

		entries = append(entries, storage.SnapshotEntry{
			Key:       string(keyBuf),
			Value:     string(valBuf),
			HasExpire: hasExpire == 1,
			ExpireAt:  expireAt,
		})
	}

	return header, entries, nil
}

// Save creates a new snapshot file at the specified path atomically:
// 1. Serializes entries using Encode()
// 2. Writes to temporary file (path + ".tmp")
// 3. Flushes and calls fsync() on the file
// 4. Atomically renames temporary file to path
func Save(path string, aofOffset int64, entries []storage.SnapshotEntry) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := Encode(aofOffset, entries)
	if err != nil {
		return err
	}

	tempPath := path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		file.Close()
		_ = os.Remove(tempPath)
	}()

	if _, err := file.Write(data); err != nil {
		return err
	}

	// Force OS kernel to commit disk buffers (fsync)
	if TestHookBeforeSync != nil {
		if err := TestHookBeforeSync(file); err != nil {
			return err
		}
	}
	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	// Atomically rename temp file to target path
	if TestHookBeforeRename != nil {
		if err := TestHookBeforeRename(tempPath); err != nil {
			return err
		}
	}
	if err := os.Rename(tempPath, path); err != nil {
		return err
	}

	// Sync parent directory to persist directory entry change on POSIX filesystems
	dirFile, err := os.Open(dir)
	if err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}

	return nil
}

// Load reads, verifies, and deserializes a snapshot file.
// If the file does not exist, it returns os.ErrNotExist.
// If the checksum fails, magic header is invalid, or version is unsupported, it returns an error.
func Load(path string) (Header, []storage.SnapshotEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Header{}, nil, err
	}
	return Decode(data)
}
