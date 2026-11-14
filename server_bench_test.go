package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"miniredis/aof"
	"miniredis/protocol"
	"miniredis/storage"
)

var (
	serverBenchVal protocol.Value
)

// benchClient represents a persistent TCP client connection to MiniRedis.
// It maintains a dedicated protocol.Reader so buffered bytes are preserved across requests.
type benchClient struct {
	conn   net.Conn
	reader *protocol.Reader
}

func newBenchClient(addr string) (*benchClient, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &benchClient{
		conn:   conn,
		reader: protocol.NewReader(conn),
	}, nil
}

func (c *benchClient) execute(cmd []byte) (protocol.Value, error) {
	if _, err := c.conn.Write(cmd); err != nil {
		return protocol.Value{}, err
	}
	return c.reader.ReadValue()
}

func (c *benchClient) Close() error {
	return c.conn.Close()
}

// startBenchServer boots a MiniRedis server on an ephemeral loopback port ("127.0.0.1:0").
// It returns the server address and a cleanup function to tear down listener and background tasks.
func startBenchServer(b *testing.B, store *storage.Store, aofWriter *aof.AOF) (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("failed to start benchmark listener: %v", err)
	}

	go runServer(listener, store, aofWriter)

	cleanup := func() {
		_ = listener.Close()
		if aofWriter != nil {
			_ = aofWriter.Close()
		}
		if store != nil {
			store.Close()
		}
	}

	return listener.Addr().String(), cleanup
}

// BenchmarkServerPing measures raw ping-pong round-trip over a persistent TCP socket.
func BenchmarkServerPing(b *testing.B) {
	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startBenchServer(b, store, nil)
	defer cleanup()

	client, err := newBenchClient(addr)
	if err != nil {
		b.Fatalf("client dial failed: %v", err)
	}
	defer client.Close()

	pingCmd := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("PING"),
	}).Encode()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.execute(pingCmd)
		if err != nil {
			b.Fatalf("PING command failed: %v", err)
		}
		serverBenchVal = resp
	}
	b.StopTimer()
}

// BenchmarkServerSet measures sequential SET commands over a persistent TCP socket (in-memory only).
func BenchmarkServerSet(b *testing.B) {
	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startBenchServer(b, store, nil)
	defer cleanup()

	client, err := newBenchClient(addr)
	if err != nil {
		b.Fatalf("client dial failed: %v", err)
	}
	defer client.Close()

	setCmd := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("SET"),
		protocol.NewBulkString("bench_key"),
		protocol.NewBulkString("bench_val"),
	}).Encode()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.execute(setCmd)
		if err != nil {
			b.Fatalf("SET command failed: %v", err)
		}
		serverBenchVal = resp
	}
	b.StopTimer()
}

// BenchmarkServerGet measures sequential GET commands over a persistent TCP socket (in-memory only).
func BenchmarkServerGet(b *testing.B) {
	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startBenchServer(b, store, nil)
	defer cleanup()

	client, err := newBenchClient(addr)
	if err != nil {
		b.Fatalf("client dial failed: %v", err)
	}
	defer client.Close()

	// Pre-populate key
	store.Set("bench_key", "bench_val")

	getCmd := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("GET"),
		protocol.NewBulkString("bench_key"),
	}).Encode()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.execute(getCmd)
		if err != nil {
			b.Fatalf("GET command failed: %v", err)
		}
		serverBenchVal = resp
	}
	b.StopTimer()
}

// BenchmarkServerMixedWorkload measures a realistic 80% GET / 20% SET workload.
func BenchmarkServerMixedWorkload(b *testing.B) {
	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startBenchServer(b, store, nil)
	defer cleanup()

	client, err := newBenchClient(addr)
	if err != nil {
		b.Fatalf("client dial failed: %v", err)
	}
	defer client.Close()

	store.Set("bench_key", "bench_val")

	getCmd := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("GET"),
		protocol.NewBulkString("bench_key"),
	}).Encode()

	setCmd := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("SET"),
		protocol.NewBulkString("bench_key"),
		protocol.NewBulkString("bench_val"),
	}).Encode()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var cmd []byte
		if i%5 == 0 {
			cmd = setCmd // 20% SET
		} else {
			cmd = getCmd // 80% GET
		}

		resp, err := client.execute(cmd)
		if err != nil {
			b.Fatalf("command failed: %v", err)
		}
		serverBenchVal = resp
	}
	b.StopTimer()
}

// BenchmarkServerPayloadSizes compares throughput with small (16B) versus medium (1KB) values.
func BenchmarkServerPayloadSizes(b *testing.B) {
	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startBenchServer(b, store, nil)
	defer cleanup()

	client, err := newBenchClient(addr)
	if err != nil {
		b.Fatalf("client dial failed: %v", err)
	}
	defer client.Close()

	val16B := strings.Repeat("A", 16)
	cmd16B := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("SET"),
		protocol.NewBulkString("k16"),
		protocol.NewBulkString(val16B),
	}).Encode()

	val1KB := strings.Repeat("B", 1024)
	cmd1KB := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("SET"),
		protocol.NewBulkString("k1k"),
		protocol.NewBulkString(val1KB),
	}).Encode()

	b.Run("Payload_16B", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.execute(cmd16B)
			if err != nil {
				b.Fatalf("execute failed: %v", err)
			}
			serverBenchVal = resp
		}
	})

	b.Run("Payload_1KB", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.execute(cmd1KB)
			if err != nil {
				b.Fatalf("execute failed: %v", err)
			}
			serverBenchVal = resp
		}
	})
}

// BenchmarkServerConcurrentClients measures throughput scaling across 1, 10, and 50 concurrent client connections.
func BenchmarkServerConcurrentClients(b *testing.B) {
	clientCounts := []int{1, 10, 50}

	for _, numClients := range clientCounts {
		b.Run(fmt.Sprintf("%d_clients", numClients), func(b *testing.B) {
			store := storage.NewStoreWithInterval(0)
			addr, cleanup := startBenchServer(b, store, nil)
			defer cleanup()

			clients := make([]*benchClient, numClients)
			for c := 0; c < numClients; c++ {
				cli, err := newBenchClient(addr)
				if err != nil {
					b.Fatalf("failed to dial client %d: %v", c, err)
				}
				defer cli.Close()
				clients[c] = cli
			}

			cmd := protocol.NewArray([]protocol.Value{
				protocol.NewBulkString("PING"),
			}).Encode()

			b.ReportAllocs()
			b.ResetTimer()

			var wg sync.WaitGroup
			opsPerClient := b.N / numClients
			if opsPerClient == 0 {
				opsPerClient = 1
			}

			for c := 0; c < numClients; c++ {
				wg.Add(1)
				go func(cli *benchClient) {
					defer wg.Done()
					for i := 0; i < opsPerClient; i++ {
						_, err := cli.execute(cmd)
						if err != nil {
							return
						}
					}
				}(clients[c])
			}
			wg.Wait()
			b.StopTimer()
		})
	}
}

// BenchmarkServerAOFSyncPolicies measures the throughput impact of different AOF durability modes.
func BenchmarkServerAOFSyncPolicies(b *testing.B) {
	tempDir, err := os.MkdirTemp(".", "miniredis_bench_aof_*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	setCmd := protocol.NewArray([]protocol.Value{
		protocol.NewBulkString("SET"),
		protocol.NewBulkString("aof_key"),
		protocol.NewBulkString("aof_val"),
	}).Encode()

	// Sub-benchmark 1: No AOF (In-memory baseline)
	b.Run("AOF_Disabled", func(b *testing.B) {
		store := storage.NewStoreWithInterval(0)
		addr, cleanup := startBenchServer(b, store, nil)
		defer cleanup()

		client, err := newBenchClient(addr)
		if err != nil {
			b.Fatalf("client dial failed: %v", err)
		}
		defer client.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.execute(setCmd)
			if err != nil {
				b.Fatalf("command failed: %v", err)
			}
			serverBenchVal = resp
		}
	})

	// Sub-benchmark 2: AOF with SyncNo (OS buffer flush)
	b.Run("AOF_SyncNo", func(b *testing.B) {
		store := storage.NewStoreWithInterval(0)
		aofPath := filepath.Join(tempDir, "bench_sync_no.aof")
		aofWriter, err := aof.Open(aofPath, aof.SyncNo)
		if err != nil {
			b.Fatalf("failed to open aof: %v", err)
		}

		addr, cleanup := startBenchServer(b, store, aofWriter)
		defer cleanup()

		client, err := newBenchClient(addr)
		if err != nil {
			b.Fatalf("client dial failed: %v", err)
		}
		defer client.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.execute(setCmd)
			if err != nil {
				b.Fatalf("command failed: %v", err)
			}
			serverBenchVal = resp
		}
	})

	// Sub-benchmark 3: AOF with SyncEverySecond (Redis default)
	b.Run("AOF_SyncEverySecond", func(b *testing.B) {
		store := storage.NewStoreWithInterval(0)
		aofPath := filepath.Join(tempDir, "bench_sync_everysec.aof")
		aofWriter, err := aof.Open(aofPath, aof.SyncEverySecond)
		if err != nil {
			b.Fatalf("failed to open aof: %v", err)
		}

		addr, cleanup := startBenchServer(b, store, aofWriter)
		defer cleanup()

		client, err := newBenchClient(addr)
		if err != nil {
			b.Fatalf("client dial failed: %v", err)
		}
		defer client.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.execute(setCmd)
			if err != nil {
				b.Fatalf("command failed: %v", err)
			}
			serverBenchVal = resp
		}
	})

	// Sub-benchmark 4: AOF with SyncAlways (fsync per write - highest durability)
	b.Run("AOF_SyncAlways", func(b *testing.B) {
		store := storage.NewStoreWithInterval(0)
		aofPath := filepath.Join(tempDir, "bench_sync_always.aof")
		aofWriter, err := aof.Open(aofPath, aof.SyncAlways)
		if err != nil {
			b.Fatalf("failed to open aof: %v", err)
		}

		addr, cleanup := startBenchServer(b, store, aofWriter)
		defer cleanup()

		client, err := newBenchClient(addr)
		if err != nil {
			b.Fatalf("client dial failed: %v", err)
		}
		defer client.Close()

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			resp, err := client.execute(setCmd)
			if err != nil {
				b.Fatalf("command failed: %v", err)
			}
			serverBenchVal = resp
		}
	})
}
