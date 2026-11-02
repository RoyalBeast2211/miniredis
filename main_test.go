package main

import (
	"bufio"
	"net"
	"testing"
)

func TestServer_Ping(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	go handleConnection(serverConn)

	_, err := clientConn.Write([]byte("PING\n"))
	if err != nil {
		t.Fatalf("failed to write to connection: %v", err)
	}

	reader := bufio.NewReader(clientConn)
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	expected := "+PONG\r\n"
	if response != expected {
		t.Fatalf("expected %q, got %q", expected, response)
	}
}
