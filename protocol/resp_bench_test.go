package protocol

import (
	"bytes"
	"testing"
)

var (
	benchParsedVal Value
	benchEncoded   []byte
	benchCmdArgs   []string
)

// BenchmarkProtocolParsePing measures parsing of the minimal RESP array command: *1\r\n$4\r\nPING\r\n.
func BenchmarkProtocolParsePing(b *testing.B) {
	raw := []byte("*1\r\n$4\r\nPING\r\n")
	// Pre-create a large buffer with repeated commands to avoid measuring bytes.Reader creation overhead
	buf := bytes.Repeat(raw, b.N)
	reader := NewReader(bytes.NewReader(buf))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		val, err := reader.ReadValue()
		if err != nil {
			b.Fatalf("ReadValue failed: %v", err)
		}
		benchParsedVal = val
	}
}

// BenchmarkProtocolParseSet measures parsing of a 3-element RESP command: *3\r\n$3\r\nSET\r\n$7\r\nmykey01\r\n$7\r\nmyval01\r\n.
func BenchmarkProtocolParseSet(b *testing.B) {
	raw := []byte("*3\r\n$3\r\nSET\r\n$7\r\nmykey01\r\n$7\r\nmyval01\r\n")
	buf := bytes.Repeat(raw, b.N)
	reader := NewReader(bytes.NewReader(buf))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		val, err := reader.ReadValue()
		if err != nil {
			b.Fatalf("ReadValue failed: %v", err)
		}
		benchParsedVal = val
	}
}

// BenchmarkProtocolEncodeSimpleString measures serialization of a RESP Simple String (+OK\r\n).
func BenchmarkProtocolEncodeSimpleString(b *testing.B) {
	val := NewSimpleString("OK")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchEncoded = val.Encode()
	}
}

// BenchmarkProtocolEncodeBulkString measures serialization of a standard RESP Bulk String ($7\r\nmyval01\r\n).
func BenchmarkProtocolEncodeBulkString(b *testing.B) {
	val := NewBulkString("benchmark_value_string_sample")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchEncoded = val.Encode()
	}
}

// BenchmarkProtocolEncodeArray measures serialization of a 3-element RESP array (command serialization).
func BenchmarkProtocolEncodeArray(b *testing.B) {
	val := NewArray([]Value{
		NewBulkString("SET"),
		NewBulkString("user:1000:session"),
		NewBulkString("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"),
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchEncoded = val.Encode()
	}
}

// BenchmarkProtocolToCommand measures converting a parsed RESP array Value into a []string slice.
func BenchmarkProtocolToCommand(b *testing.B) {
	val := NewArray([]Value{
		NewBulkString("SET"),
		NewBulkString("user:1000:session"),
		NewBulkString("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"),
	})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		args, err := val.ToCommand()
		if err != nil {
			b.Fatalf("ToCommand failed: %v", err)
		}
		benchCmdArgs = args
	}
}
