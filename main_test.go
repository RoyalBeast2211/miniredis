package main

import (
	"net"
	"testing"

	"miniredis/protocol"
	"miniredis/storage"
)

func sendCommand(conn net.Conn, args ...string) (protocol.Value, error) {
	elements := make([]protocol.Value, len(args))
	for i, arg := range args {
		elements[i] = protocol.NewBulkString(arg)
	}
	cmd := protocol.NewArray(elements)

	if _, err := conn.Write(cmd.Encode()); err != nil {
		return protocol.Value{}, err
	}

	reader := protocol.NewReader(conn)
	return reader.ReadValue()
}

func TestServer_KeyValueCommands(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	store := storage.NewStore()
	defer store.Close()
	go handleConnection(serverConn, store)

	// SET foo bar
	resp, err := sendCommand(clientConn, "SET", "foo", "bar")
	if err != nil || resp.Str != "OK" {
		t.Fatalf("expected OK, got %+v (err: %v)", resp, err)
	}

	// GET foo
	resp, err = sendCommand(clientConn, "GET", "foo")
	if err != nil || resp.Str != "bar" {
		t.Fatalf("expected bar, got %+v (err: %v)", resp, err)
	}

	// EXISTS foo
	resp, err = sendCommand(clientConn, "EXISTS", "foo")
	if err != nil || resp.Num != 1 {
		t.Fatalf("expected 1, got %+v (err: %v)", resp, err)
	}

	// DEL foo
	resp, err = sendCommand(clientConn, "DEL", "foo")
	if err != nil || resp.Num != 1 {
		t.Fatalf("expected 1, got %+v (err: %v)", resp, err)
	}

	// GET foo (missing)
	resp, err = sendCommand(clientConn, "GET", "foo")
	if err != nil || !resp.IsNull {
		t.Fatalf("expected null bulk string, got %+v (err: %v)", resp, err)
	}
}
