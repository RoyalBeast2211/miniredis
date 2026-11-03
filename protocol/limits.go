package protocol

import "errors"

const (
	// MaxBulkStringLength defines the hard ceiling on individual bulk strings (512 MB).
	// This matches the Redis specification and prevents client requests from triggering
	// out-of-memory crashes via malicious length headers.
	MaxBulkStringLength = 512 * 1024 * 1024

	// MaxArrayElements defines the maximum number of elements permitted in a RESP array (1 million).
	MaxArrayElements = 1024 * 1024
)

var (
	// ErrBulkStringTooLarge is returned when a client attempts to send a bulk string exceeding MaxBulkStringLength.
	ErrBulkStringTooLarge = errors.New("protocol error: bulk string exceeds maximum allowed size (512 MB)")

	// ErrArrayTooLarge is returned when an array element count exceeds MaxArrayElements.
	ErrArrayTooLarge = errors.New("protocol error: array length exceeds maximum allowed elements")
)
