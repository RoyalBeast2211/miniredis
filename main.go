package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"miniredis/aof"
	"miniredis/protocol"
	"miniredis/storage"
)

func handleCommand(store *storage.Store, aofWriter *aof.AOF, args []string) protocol.Value {
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
	case "SET":
		if len(args) != 3 {
			return protocol.NewError("ERR wrong number of arguments for 'set' command")
		}
		store.Set(args[1], args[2])
		if aofWriter != nil {
			_ = aofWriter.Write(args)
		}
		return protocol.NewSimpleString("OK")
	case "GET":
		if len(args) != 2 {
			return protocol.NewError("ERR wrong number of arguments for 'get' command")
		}
		val, exists := store.Get(args[1])
		if !exists {
			return protocol.NewNullBulkString()
		}
		return protocol.NewBulkString(val)
	case "DEL":
		if len(args) < 2 {
			return protocol.NewError("ERR wrong number of arguments for 'del' command")
		}
		deletedCount := int64(0)
		for _, key := range args[1:] {
			if store.Delete(key) {
				deletedCount++
				if aofWriter != nil {
					_ = aofWriter.Write([]string{"DEL", key})
				}
			}
		}
		return protocol.NewInteger(deletedCount)
	case "EXISTS":
		if len(args) < 2 {
			return protocol.NewError("ERR wrong number of arguments for 'exists' command")
		}
		existCount := int64(0)
		for _, key := range args[1:] {
			if store.Exists(key) {
				existCount++
			}
		}
		return protocol.NewInteger(existCount)
	default:
		return protocol.NewError(fmt.Sprintf("ERR unknown command '%s'", args[0]))
	}
}

func handleConnection(conn net.Conn, store *storage.Store, aofWriter *aof.AOF) {
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

		reply := handleCommand(store, aofWriter, args)
		if _, err := conn.Write(reply.Encode()); err != nil {
			break
		}
	}
}

func runServer(listener net.Listener, store *storage.Store, aofWriter *aof.AOF) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go handleConnection(conn, store, aofWriter)
	}
}

func main() {
	store := storage.NewStore()
	defer store.Close()

	aofWriter, err := aof.Open("appendonly.aof", aof.SyncEverySecond)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open AOF: %v\n", err)
		os.Exit(1)
	}
	defer aofWriter.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to bind to port 6379: %v\n", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Println("MiniRedis server listening on 127.0.0.1:6379...")
	if err := runServer(listener, store, aofWriter); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
