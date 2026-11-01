package main

import (
	"bufio"
	"fmt"
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
			break
		}
		cmd := strings.TrimSpace(line)
		if strings.ToUpper(cmd) == "PING" {
			conn.Write([]byte("+PONG\r\n"))
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
