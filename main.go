package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"miniredis/protocol"
)

func handleCommand(args []string) protocol.Value {
	if len(args) == 0 {
		return protocol.NewError("ERR empty command")
	}

	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		if len(args) == 1 {
			return protocol.NewSimpleString("PONG")
		} else if len(args) == 2 {
			return protocol.NewBulkString(args[1])
		}
		return protocol.NewError("ERR wrong number of arguments for 'ping' command")
	case "ECHO":
		if len(args) == 2 {
			return protocol.NewBulkString(args[1])
		}
		return protocol.NewError("ERR wrong number of arguments for 'echo' command")
	default:
		return protocol.NewError(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := protocol.NewReader(conn)
	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Protocol read error: %v\n", err)
			}
			break
		}

		args, err := val.ToCommand()
		if err != nil {
			conn.Write(protocol.NewError(err.Error()).Encode())
			continue
		}

		reply := handleCommand(args)
		if _, err := conn.Write(reply.Encode()); err != nil {
			break
		}
	}
}

func runServer(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go handleConnection(conn)
	}
}

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to bind to port 6379: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("MiniRedis server listening on 127.0.0.1:6379...")
	if err := runServer(listener); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
