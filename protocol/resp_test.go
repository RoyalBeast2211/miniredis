package protocol

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
)

// chunkedReader is a helper that splits reads into tiny chunks (e.g. 1 byte)
// to simulate TCP packet fragmentation and partial network reads.
type chunkedReader struct {
	data      []byte
	chunkSize int
	offset    int
}

func (c *chunkedReader) Read(p []byte) (int, error) {
	if c.offset >= len(c.data) {
		return 0, io.EOF
	}
	remaining := len(c.data) - c.offset
	toRead := c.chunkSize
	if toRead > remaining {
		toRead = remaining
	}
	if toRead > len(p) {
		toRead = len(p)
	}
	copy(p, c.data[c.offset:c.offset+toRead])
	c.offset += toRead
	return toRead, nil
}

// TestParserValidArraysAndBulkStrings tests typical Redis commands encoded as RESP Arrays.
func TestParserValidArraysAndBulkStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
	}{
		{
			name:  "PING command",
			input: "*1\r\n$4\r\nPING\r\n",
			expected: Value{
				Type: TypeArray,
				Array: []Value{
					{Type: TypeBulkString, Str: "PING"},
				},
			},
		},
		{
			name:  "ECHO hello command",
			input: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
			expected: Value{
				Type: TypeArray,
				Array: []Value{
					{Type: TypeBulkString, Str: "ECHO"},
					{Type: TypeBulkString, Str: "hello"},
				},
			},
		},
		{
			name:  "Empty bulk string",
			input: "$0\r\n\r\n",
			expected: Value{
				Type: TypeBulkString,
				Str:  "",
			},
		},
		{
			name:  "Null bulk string",
			input: "$-1\r\n",
			expected: Value{
				Type:   TypeBulkString,
				IsNull: true,
			},
		},
		{
			name:  "Null array",
			input: "*-1\r\n",
			expected: Value{
				Type:   TypeArray,
				IsNull: true,
			},
		},
		{
			name:  "Bulk string containing newlines and spaces",
			input: "$12\r\nhello\n world\r\n",
			expected: Value{
				Type: TypeBulkString,
				Str:  "hello\n world",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(strings.NewReader(tt.input))
			val, err := reader.ReadValue()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(val, tt.expected) {
				t.Errorf("Expected %+v, got %+v", tt.expected, val)
			}
		})
	}
}

// TestParserSimpleStringErrorAndInteger tests simple strings, errors, and integers.
func TestParserSimpleStringErrorAndInteger(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
	}{
		{
			name:     "Simple String OK",
			input:    "+OK\r\n",
			expected: Value{Type: TypeSimpleString, Str: "OK"},
		},
		{
			name:     "Simple String PONG",
			input:    "+PONG\r\n",
			expected: Value{Type: TypeSimpleString, Str: "PONG"},
		},
		{
			name:     "Error message",
			input:    "-ERR unknown command 'FOO'\r\n",
			expected: Value{Type: TypeError, Str: "ERR unknown command 'FOO'"},
		},
		{
			name:     "Positive integer",
			input:    ":1000\r\n",
			expected: Value{Type: TypeInteger, Num: 1000},
		},
		{
			name:     "Negative integer",
			input:    ":-42\r\n",
			expected: Value{Type: TypeInteger, Num: -42},
		},
		{
			name:     "Zero",
			input:    ":0\r\n",
			expected: Value{Type: TypeInteger, Num: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(strings.NewReader(tt.input))
			val, err := reader.ReadValue()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(val, tt.expected) {
				t.Errorf("Expected %+v, got %+v", tt.expected, val)
			}
		})
	}
}

// TestParserMalformedInputs tests that invalid or corrupted RESP data returns descriptive errors.
func TestParserMalformedInputs(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Unknown prefix byte",
			input: "?unknown\r\n",
		},
		{
			name:  "Bulk string invalid length (non-number)",
			input: "$abc\r\nhello\r\n",
		},
		{
			name:  "Bulk string negative length below -1",
			input: "$-2\r\n",
		},
		{
			name:  "Bulk string missing trailing CRLF",
			input: "$5\r\nhellono\r\n",
		},
		{
			name:  "Bulk string truncated before length reached",
			input: "$10\r\nshort\r\n",
		},
		{
			name:  "Array invalid count (non-number)",
			input: "*xyz\r\n",
		},
		{
			name:  "Array negative count below -1",
			input: "*-5\r\n",
		},
		{
			name:  "Integer invalid format",
			input: ":not_a_number\r\n",
		},
		{
			name:  "Missing \\r before \\n",
			input: "+OK\n",
		},
		{
			name:  "Bulk string exceeds max size (512MB)",
			input: "$600000000\r\n",
		},
		{
			name:  "Array count exceeds max elements (1M)",
			input: "*2000000\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewReader(strings.NewReader(tt.input))
			_, err := reader.ReadValue()
			if err == nil {
				t.Errorf("Expected error for malformed input %q, but got nil", tt.input)
			}
		})
	}
}

// TestMultipleSequentialCommands tests that multiple back-to-back commands
// over the same connection (pipelining) are correctly parsed one after another.
func TestMultipleSequentialCommands(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n*1\r\n$4\r\nPING\r\n"
	reader := NewReader(strings.NewReader(input))

	// First command: PING
	cmd1, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read command 1: %v", err)
	}
	args1, err := cmd1.ToCommand()
	if err != nil || len(args1) != 1 || args1[0] != "PING" {
		t.Errorf("Expected [PING], got %v (err: %v)", args1, err)
	}

	// Second command: ECHO hello
	cmd2, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read command 2: %v", err)
	}
	args2, err := cmd2.ToCommand()
	if err != nil || len(args2) != 2 || args2[0] != "ECHO" || args2[1] != "hello" {
		t.Errorf("Expected [ECHO hello], got %v (err: %v)", args2, err)
	}

	// Third command: PING
	cmd3, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read command 3: %v", err)
	}
	args3, err := cmd3.ToCommand()
	if err != nil || len(args3) != 1 || args3[0] != "PING" {
		t.Errorf("Expected [PING], got %v (err: %v)", args3, err)
	}

	// End of stream
	_, err = reader.ReadValue()
	if err != io.EOF {
		t.Errorf("Expected io.EOF at end of stream, got %v", err)
	}
}

// TestPartialNetworkReads verifies that when bytes arrive in tiny fragments
// (simulating TCP packet fragmentation), the parser correctly waits and reassembles them.
func TestPartialNetworkReads(t *testing.T) {
	raw := "*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n"
	// Feed the parser 1 byte at a time!
	chunked := &chunkedReader{
		data:      []byte(raw),
		chunkSize: 1,
	}

	reader := NewReader(chunked)
	val, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("Failed to read chunked input: %v", err)
	}

	args, err := val.ToCommand()
	if err != nil {
		t.Fatalf("Failed to convert to command: %v", err)
	}

	expected := []string{"ECHO", "hello world"}
	if !reflect.DeepEqual(args, expected) {
		t.Errorf("Expected %v, got %v", expected, args)
	}
}

// TestEncoding tests encoding of all RESP types back to byte slices.
func TestEncoding(t *testing.T) {
	tests := []struct {
		name     string
		val      Value
		expected string
	}{
		{
			name:     "Simple String",
			val:      NewSimpleString("OK"),
			expected: "+OK\r\n",
		},
		{
			name:     "Error",
			val:      NewError("ERR unknown command"),
			expected: "-ERR unknown command\r\n",
		},
		{
			name:     "Integer",
			val:      NewInteger(42),
			expected: ":42\r\n",
		},
		{
			name:     "Bulk String",
			val:      NewBulkString("hello"),
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "Empty Bulk String",
			val:      NewBulkString(""),
			expected: "$0\r\n\r\n",
		},
		{
			name:     "Null Bulk String",
			val:      NewNullBulkString(),
			expected: "$-1\r\n",
		},
		{
			name: "Array",
			val: NewArray([]Value{
				NewBulkString("ECHO"),
				NewBulkString("hello"),
			}),
			expected: "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
		},
		{
			name:     "Null Array",
			val:      Value{Type: TypeArray, IsNull: true},
			expected: "*-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.val.Encode()
			if !bytes.Equal(encoded, []byte(tt.expected)) {
				t.Errorf("Expected %q, got %q", tt.expected, string(encoded))
			}
		})
	}
}
