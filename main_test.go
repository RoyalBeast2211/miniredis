package main

import (
	"net"
	"testing"

	"miniredis/protocol"
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

func TestServer_Ping(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go handleConnection(serverConn)

	resp, err := sendCommand(clientConn, "PING")
	if err != nil {
		t.Fatalf("failed to send command: %v", err)
	}
	if resp.Type != protocol.TypeSimpleString || resp.Str != "PONG" {
		t.Fatalf("expected +PONG, got %+v", resp)
	}
}

func TestServer_Echo(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go handleConnection(serverConn)

	resp, err := sendCommand(clientConn, "ECHO", "hello world")
	if err != nil {
		t.Fatalf("failed to send command: %v", err)
	}
	if resp.Type != protocol.TypeBulkString || resp.Str != "hello world" {
		t.Fatalf("expected $11\r\nhello world, got %+v", resp)
	}
}

func TestServer_Pipelining(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go handleConnection(serverConn)

	cmd1 := protocol.NewArray([]protocol.Value{protocol.NewBulkString("PING")}).Encode()
	cmd2 := protocol.NewArray([]protocol.Value{protocol.NewBulkString("ECHO"), protocol.NewBulkString("pipelined")}).Encode()

	if _, err := clientConn.Write(append(cmd1, cmd2...)); err != nil {
		t.Fatalf("failed to write pipelined commands: %v", err)
	}

	reader := protocol.NewReader(clientConn)
	r1, err := reader.ReadValue()
	if err != nil || r1.Str != "PONG" {
		t.Fatalf("unexpected reply 1: %+v (err: %v)", r1, err)
	}

	r2, err := reader.ReadValue()
	if err != nil || r2.Str != "pipelined" {
		t.Fatalf("unexpected reply 2: %+v (err: %v)", r2, err)
	}
}
