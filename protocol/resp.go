package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

type Value struct {
	Type   Type
	Str    string
	Num    int64
	Array  []Value
	IsNull bool
}

func NewSimpleString(s string) Value {
	return Value{Type: TypeSimpleString, Str: s}
}

func NewError(msg string) Value {
	return Value{Type: TypeError, Str: msg}
}

func NewInteger(n int64) Value {
	return Value{Type: TypeInteger, Num: n}
}

func NewBulkString(s string) Value {
	return Value{Type: TypeBulkString, Str: s}
}

func NewNullBulkString() Value {
	return Value{Type: TypeBulkString, IsNull: true}
}

func NewArray(elems []Value) Value {
	return Value{Type: TypeArray, Array: elems}
}

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

func (v Value) Encode() []byte {
	switch v.Type {
	case TypeSimpleString:
		return []byte(fmt.Sprintf("+%s\r\n", v.Str))
	case TypeError:
		return []byte(fmt.Sprintf("-%s\r\n", v.Str))
	case TypeInteger:
		return []byte(fmt.Sprintf(":%d\r\n", v.Num))
	case TypeBulkString:
		if v.IsNull {
			return []byte("$-1\r\n")
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str))
	case TypeArray:
		if v.IsNull {
			return []byte("*-1\r\n")
		}
		var buf []byte
		buf = append(buf, []byte(fmt.Sprintf("*%d\r\n", len(v.Array)))...)
		for _, elem := range v.Array {
			buf = append(buf, elem.Encode()...)
		}
		return buf
	default:
		return []byte("-ERR unknown type\r\n")
	}
}

func EncodeCommand(args ...string) []byte {
	elems := make([]Value, len(args))
	for i, arg := range args {
		elems[i] = NewBulkString(arg)
	}
	return NewArray(elems).Encode()
}

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
