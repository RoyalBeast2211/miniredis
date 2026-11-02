package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Connection read error: %v\n", err)
			}
			break
		}
		cmd := strings.TrimSpace(line)
		parts := strings.Fields(cmd)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(parts) > 1 {
				conn.Write([]byte("+" + strings.Join(parts[1:], " ") + "\r\n"))
			} else {
				conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
			}
		default:
			conn.Write([]byte("-ERR unknown command '" + parts[0] + "'\r\n"))
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
