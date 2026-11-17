package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Type represents the RESP data type identifier byte.
type Type byte

const (
	// TypeSimpleString represents simple strings: "+OK\r\n"
	TypeSimpleString Type = '+'
	// TypeError represents error messages: "-ERR message\r\n"
	TypeError Type = '-'
	// TypeInteger represents signed 64-bit integers: ":1000\r\n"
	TypeInteger Type = ':'
	// TypeBulkString represents binary-safe strings with explicit length: "$5\r\nhello\r\n"
	TypeBulkString Type = '$'
	// TypeArray represents an ordered list of RESP values: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
	TypeArray Type = '*'
)

// Value represents any data value supported by the RESP protocol.
// It can store a simple string, an error, an integer, a bulk string, or an array.
type Value struct {
	Type   Type    // The RESP type (+, -, :, $, *)
	Str    string  // Used for SimpleString, Error, and BulkString
	Num    int64   // Used for Integer
	Array  []Value // Used for Array
	IsNull bool    // True if this represents a null value (e.g., "$-1\r\n" or "*-1\r\n")
}

// NewSimpleString creates a RESP Simple String value.
func NewSimpleString(s string) Value {
	return Value{Type: TypeSimpleString, Str: s}
}

// NewError creates a RESP Error value.
func NewError(msg string) Value {
	return Value{Type: TypeError, Str: msg}
}

// NewInteger creates a RESP Integer value.
func NewInteger(n int64) Value {
	return Value{Type: TypeInteger, Num: n}
}

// NewBulkString creates a RESP Bulk String value.
func NewBulkString(s string) Value {
	return Value{Type: TypeBulkString, Str: s}
}

// NewNullBulkString creates a RESP Null Bulk String ("$-1\r\n").
func NewNullBulkString() Value {
	return Value{Type: TypeBulkString, IsNull: true}
}

// NewArray creates a RESP Array value containing the given elements.
func NewArray(elems []Value) Value {
	return Value{Type: TypeArray, Array: elems}
}

// ToCommand converts a RESP Array of Bulk Strings into a slice of command strings.
// In Redis, all commands sent from clients are encoded as an Array of Bulk Strings.
// For example: ["ECHO", "hello world"]
func (v Value) ToCommand() ([]string, error) {
	if v.Type != TypeArray {
		return nil, errors.New("command must be a RESP array")
	}

	args := make([]string, len(v.Array))
	for i, elem := range v.Array {
		if elem.Type != TypeBulkString {
			return nil, fmt.Errorf("command argument at index %d must be a bulk string", i)
		}
		args[i] = elem.Str
	}
	return args, nil
}

// EncodeCommand encodes a command name and its string arguments into a standard RESP Array of Bulk Strings.
// For example: ["SET", "k", "v"] -> "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".
func EncodeCommand(args ...string) []byte {
	elems := make([]Value, len(args))
	for i, arg := range args {
		elems[i] = NewBulkString(arg)
	}
	return NewArray(elems).Encode()
}

// Pre-allocated static byte slices for common RESP responses to avoid heap allocations.
var (
	respOK        = []byte("+OK\r\n")
	respPong      = []byte("+PONG\r\n")
	respNullBulk  = []byte("$-1\r\n")
	respNullArray = []byte("*-1\r\n")
)

// estimateSize estimates the serialized byte length to allocate an appropriately sized buffer.
func (v Value) estimateSize() int {
	switch v.Type {
	case TypeSimpleString, TypeError:
		// prefix(1) + str + CRLF(2)
		return len(v.Str) + 3
	case TypeInteger:
		// prefix(1) + digits(~20) + CRLF(2)
		return 23
	case TypeBulkString:
		if v.IsNull {
			return 5 // $-1\r\n
		}
		// prefix(1) + digits(len) + CRLF(2) + str + CRLF(2)
		return len(v.Str) + 15
	case TypeArray:
		if v.IsNull {
			return 5 // *-1\r\n
		}
		size := 15
		for i := range v.Array {
			size += v.Array[i].estimateSize()
		}
		return size
	default:
		return 32
	}
}

// AppendEncode serializes a RESP Value by appending its raw bytes to dst.
// This allows callers to reuse an existing buffer and avoid intermediate heap allocations.
func (v Value) AppendEncode(dst []byte) []byte {
	switch v.Type {
	case TypeSimpleString:
		dst = append(dst, '+')
		dst = append(dst, v.Str...)
		return append(dst, '\r', '\n')

	case TypeError:
		dst = append(dst, '-')
		dst = append(dst, v.Str...)
		return append(dst, '\r', '\n')

	case TypeInteger:
		dst = append(dst, ':')
		dst = strconv.AppendInt(dst, v.Num, 10)
		return append(dst, '\r', '\n')

	case TypeBulkString:
		if v.IsNull {
			return append(dst, '$', '-', '1', '\r', '\n')
		}
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(v.Str)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, v.Str...)
		return append(dst, '\r', '\n')

	case TypeArray:
		if v.IsNull {
			return append(dst, '*', '-', '1', '\r', '\n')
		}
		dst = append(dst, '*')
		dst = strconv.AppendInt(dst, int64(len(v.Array)), 10)
		dst = append(dst, '\r', '\n')
		for i := range v.Array {
			dst = v.Array[i].AppendEncode(dst)
		}
		return dst

	default:
		return dst
	}
}

// Encode serializes a RESP Value into its raw byte representation ready to be sent over the wire.
func (v Value) Encode() []byte {
	// Fast path: return static pre-allocated slices for the most common constant responses.
	switch v.Type {
	case TypeSimpleString:
		if v.Str == "OK" {
			return respOK
		}
		if v.Str == "PONG" {
			return respPong
		}
	case TypeBulkString:
		if v.IsNull {
			return respNullBulk
		}
	case TypeArray:
		if v.IsNull {
			return respNullArray
		}
	}

	// Dynamic path: pre-size the buffer based on estimated size and append directly into it.
	return v.AppendEncode(make([]byte, 0, v.estimateSize()))
}

// Reader wraps a bufio.Reader to provide convenient methods for parsing incoming RESP streams.
type Reader struct {
	reader *bufio.Reader
}

// NewReader constructs a new Reader from any io.Reader (such as a net.Conn).
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		reader: bufio.NewReader(rd),
	}
}

// Buffered returns the number of bytes currently buffered in memory.
func (r *Reader) Buffered() int {
	return r.reader.Buffered()
}

// parseSignedInt parses an ASCII signed integer from a byte slice without heap allocation.
func parseSignedInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errors.New("protocol error: empty integer")
	}

	var neg bool
	if b[0] == '-' {
		neg = true
		b = b[1:]
	} else if b[0] == '+' {
		b = b[1:]
	}

	if len(b) == 0 {
		return 0, errors.New("protocol error: missing digits")
	}

	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, errors.New("protocol error: invalid integer character")
		}
		digit := int64(c - '0')
		// Check for potential overflow: n*10 + digit > maxInt64
		if n > (1<<63-1-digit)/10 {
			return 0, errors.New("protocol error: integer overflow")
		}
		n = n*10 + digit
	}

	if neg {
		return -n, nil
	}
	return n, nil
}

// readLine reads bytes until the next '\n' and verifies that the preceding byte is '\r'.
// It returns the line without the trailing "\r\n".
// The returned byte slice points into bufio.Reader's internal buffer when possible,
// eliminating heap allocations for typical line reads.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.reader.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			// Line is longer than the internal buffer; allocate and accumulate across chunks.
			fullLine := append([]byte(nil), line...)
			for errors.Is(err, bufio.ErrBufferFull) {
				var part []byte
				part, err = r.reader.ReadSlice('\n')
				fullLine = append(fullLine, part...)
			}
			line = fullLine
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}

	// A valid RESP line must end with "\r\n", meaning at least 2 bytes.
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errors.New("protocol error: line must end with CRLF (\\r\\n)")
	}

	// Slice off the trailing "\r\n".
	return line[:len(line)-2], nil
}

// readSimpleString parses a Simple String ("+OK\r\n").
func (r *Reader) readSimpleString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return NewSimpleString(string(line)), nil
}

// readError parses an Error string ("-ERR unknown command\r\n").
func (r *Reader) readError() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return NewError(string(line)), nil
}

// readInteger parses an Integer (":1000\r\n").
func (r *Reader) readInteger() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	num, err := parseSignedInt(line)
	if err != nil {
		return Value{}, fmt.Errorf("protocol error: invalid integer %q", string(line))
	}
	return NewInteger(num), nil
}

// readBulkString parses a Bulk String ("$5\r\nhello\r\n" or "$-1\r\n").
func (r *Reader) readBulkString() (Value, error) {
	// 1. Read the length line: "$<len>\r\n"
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	length, err := parseSignedInt(line)
	if err != nil {
		return Value{}, fmt.Errorf("protocol error: invalid bulk string length %q", string(line))
	}

	// In RESP, "$-1\r\n" denotes a Null Bulk String.
	if length == -1 {
		return NewNullBulkString(), nil
	}

	if length < -1 {
		return Value{}, fmt.Errorf("protocol error: negative bulk string length %d", length)
	}

	if length > MaxBulkStringLength {
		return Value{}, ErrBulkStringTooLarge
	}

	// 2. Read exactly `length` bytes into a buffer.
	buf := make([]byte, length)
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		return Value{}, err
	}

	// 3. Read the trailing "\r\n" following the payload data without heap allocation.
	b1, err := r.reader.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Value{}, io.ErrUnexpectedEOF
		}
		return Value{}, err
	}
	b2, err := r.reader.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return Value{}, io.ErrUnexpectedEOF
		}
		return Value{}, err
	}
	if b1 != '\r' || b2 != '\n' {
		return Value{}, errors.New("protocol error: expected CRLF after bulk string data")
	}

	return NewBulkString(string(buf)), nil
}

// readArray parses a RESP Array ("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n" or "*-1\r\n").
func (r *Reader) readArray() (Value, error) {
	// 1. Read the element count line: "*<count>\r\n"
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	count, err := parseSignedInt(line)
	if err != nil {
		return Value{}, fmt.Errorf("protocol error: invalid array count %q", string(line))
	}

	// In RESP, "*-1\r\n" denotes a Null Array.
	if count == -1 {
		return Value{Type: TypeArray, IsNull: true}, nil
	}

	if count < -1 {
		return Value{}, fmt.Errorf("protocol error: negative array length %d", count)
	}

	if count > MaxArrayElements {
		return Value{}, ErrArrayTooLarge
	}

	// 2. Parse each element in the array recursively.
	elements := make([]Value, count)
	for i := 0; i < int(count); i++ {
		elem, err := r.ReadValue()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Value{}, io.ErrUnexpectedEOF
			}
			return Value{}, err
		}
		elements[i] = elem
	}

	return NewArray(elements), nil
}

// ReadValue reads the next complete RESP Value from the incoming stream.
// It examines the first byte (type indicator) and delegates to the appropriate helper.
func (r *Reader) ReadValue() (Value, error) {
	// Read the single prefix byte identifying the RESP data type.
	prefix, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(prefix) {
	case TypeSimpleString:
		return r.readSimpleString()
	case TypeError:
		return r.readError()
	case TypeInteger:
		return r.readInteger()
	case TypeBulkString:
		return r.readBulkString()
	case TypeArray:
		return r.readArray()
	default:
		return Value{}, fmt.Errorf("protocol error: unknown prefix byte %q", prefix)
	}
}
