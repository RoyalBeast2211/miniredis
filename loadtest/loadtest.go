package loadtest

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"miniredis/protocol"
)

// WorkloadType specifies the type of command workload to generate.
type WorkloadType string

const (
	// WorkloadGet executes only GET commands on pre-populated keys.
	WorkloadGet WorkloadType = "GET"

	// WorkloadSet executes only SET commands.
	WorkloadSet WorkloadType = "SET"

	// WorkloadMixed executes 50% GET and 50% SET commands.
	WorkloadMixed WorkloadType = "MIXED"
)

// Config defines the parameters for a load testing session.
type Config struct {
	// TargetAddr is the host:port of the MiniRedis server (e.g., "127.0.0.1:6379").
	TargetAddr string

	// Clients is the number of concurrent persistent TCP client connections to maintain.
	Clients int

	// Duration is the duration of the measurement phase.
	Duration time.Duration

	// WarmupDuration is the duration of the warm-up phase (metrics during warm-up are discarded).
	WarmupDuration time.Duration

	// Workload is the operation type: GET, SET, or MIXED.
	Workload WorkloadType

	// PayloadSize is the byte size of string values in SET commands.
	PayloadSize int

	// KeyCount is the size of the key universe (e.g. 10000 means keys key:0 to key:9999).
	KeyCount int

	// MaxOps limits total operations across all clients (0 = unlimited, duration-bound).
	MaxOps int64
}

// Result contains aggregate and percentile metrics collected during a load test run.
type Result struct {
	Config          Config
	TotalOps        int64
	SuccessfulOps   int64
	ErrorOps        int64
	Duration        time.Duration
	OpsPerSec       float64
	MinLatency      time.Duration
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	MaxLatency      time.Duration
	AvgLatency      time.Duration
	StartGoroutines int
	EndGoroutines   int
	StartAllocBytes uint64
	EndAllocBytes   uint64
}

// generateValue creates a predictable string of the specified byte length.
func generateValue(size int) string {
	if size <= 0 {
		return "val"
	}
	// Fill with a repeating pattern to avoid allocation in hot loops where possible
	return strings.Repeat("A", size)
}

// Client represents a single persistent TCP connection to MiniRedis.
type Client struct {
	conn   net.Conn
	reader *protocol.Reader
}

// NewClient dials the target server and sets up a dedicated RESP Reader.
func NewClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	// Disable Nagle's algorithm for low-latency request-response round trips
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	return &Client{
		conn:   conn,
		reader: protocol.NewReader(conn),
	}, nil
}

// Execute sends an encoded command slice and reads the server's RESP response.
func (c *Client) Execute(cmd []byte) (protocol.Value, error) {
	if _, err := c.conn.Write(cmd); err != nil {
		return protocol.Value{}, err
	}
	return c.reader.ReadValue()
}

// Close gracefully closes the underlying TCP connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// PrePopulateKeys inserts keys key:0 through key:(keyCount-1) into the server
// so GET and MIXED workloads have real keys to query.
func PrePopulateKeys(addr string, keyCount int, payloadSize int) error {
	client, err := NewClient(addr)
	if err != nil {
		return fmt.Errorf("pre-populate failed to connect: %w", err)
	}
	defer client.Close()

	val := generateValue(payloadSize)
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("bench_key:%d", i)
		cmd := protocol.NewArray([]protocol.Value{
			protocol.NewBulkString("SET"),
			protocol.NewBulkString(key),
			protocol.NewBulkString(val),
		}).Encode()

		resp, err := client.Execute(cmd)
		if err != nil {
			return fmt.Errorf("pre-populate SET %s failed: %w", key, err)
		}
		if resp.Type == protocol.TypeError {
			return fmt.Errorf("pre-populate server error: %s", resp.Str)
		}
	}
	return nil
}

// Run executes a complete load testing session according to the provided Config.
func Run(cfg Config) (*Result, error) {
	if cfg.Clients <= 0 {
		cfg.Clients = 1
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 5 * time.Second
	}
	if cfg.KeyCount <= 0 {
		cfg.KeyCount = 10000
	}
	if cfg.PayloadSize <= 0 {
		cfg.PayloadSize = 16
	}

	// Snapshot baseline runtime resources
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	startGoroutines := runtime.NumGoroutine()

	// Establish persistent client connections
	clients := make([]*Client, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		cli, err := NewClient(cfg.TargetAddr)
		if err != nil {
			// Clean up already established connections
			for j := 0; j < i; j++ {
				_ = clients[j].Close()
			}
			return nil, fmt.Errorf("client %d failed to connect: %w", i, err)
		}
		clients[i] = cli
	}
	defer func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}()

	payloadVal := generateValue(cfg.PayloadSize)

	// Pre-encode templates to minimize client-side load generator CPU overhead
	// We use key indices modulo KeyCount for deterministic access patterns
	getCmds := make([][]byte, cfg.KeyCount)
	setCmds := make([][]byte, cfg.KeyCount)
	for i := 0; i < cfg.KeyCount; i++ {
		k := fmt.Sprintf("bench_key:%d", i)
		getCmds[i] = protocol.NewArray([]protocol.Value{
			protocol.NewBulkString("GET"),
			protocol.NewBulkString(k),
		}).Encode()
		setCmds[i] = protocol.NewArray([]protocol.Value{
			protocol.NewBulkString("SET"),
			protocol.NewBulkString(k),
			protocol.NewBulkString(payloadVal),
		}).Encode()
	}

	// Phase 1: Warm-up period (traffic runs, but latencies & counts are discarded)
	if cfg.WarmupDuration > 0 {
		warmupDeadline := time.Now().Add(cfg.WarmupDuration)
		var warmupWg sync.WaitGroup
		for i := 0; i < cfg.Clients; i++ {
			warmupWg.Add(1)
			go func(c *Client, workerID int) {
				defer warmupWg.Done()
				idx := workerID % cfg.KeyCount
				for time.Now().Before(warmupDeadline) {
					var cmd []byte
					if cfg.Workload == WorkloadGet {
						cmd = getCmds[idx]
					} else {
						cmd = setCmds[idx]
					}
					_, _ = c.Execute(cmd)
					idx = (idx + 1) % cfg.KeyCount
				}
			}(clients[i], i)
		}
		warmupWg.Wait()
	}

	// Phase 2: Active Measurement Phase
	var totalSuccess int64
	var totalErrors int64
	workerLatencies := make([][]time.Duration, cfg.Clients)

	// Pre-allocate slice space per worker to prevent allocation pauses during test
	estimatedOpsPerWorker := 100000
	for i := 0; i < cfg.Clients; i++ {
		workerLatencies[i] = make([]time.Duration, 0, estimatedOpsPerWorker)
	}

	stopSignal := make(chan struct{})
	var wg sync.WaitGroup

	startTime := time.Now()

	// Launch worker goroutines
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(workerID int, c *Client) {
			defer wg.Done()

			latencies := workerLatencies[workerID]
			idx := (workerID * 17) % cfg.KeyCount // Stagger initial key index per worker
			opCount := int64(0)

			for {
				select {
				case <-stopSignal:
					workerLatencies[workerID] = latencies
					return
				default:
				}

				if cfg.MaxOps > 0 && atomic.LoadInt64(&totalSuccess)+atomic.LoadInt64(&totalErrors) >= cfg.MaxOps {
					workerLatencies[workerID] = latencies
					return
				}

				// Select command based on workload
				var cmd []byte
				switch cfg.Workload {
				case WorkloadGet:
					cmd = getCmds[idx]
				case WorkloadSet:
					cmd = setCmds[idx]
				case WorkloadMixed:
					if opCount%2 == 0 {
						cmd = getCmds[idx]
					} else {
						cmd = setCmds[idx]
					}
				}

				// Measure round-trip latency
				opStart := time.Now()
				resp, err := c.Execute(cmd)
				duration := time.Since(opStart)

				if err != nil || resp.Type == protocol.TypeError {
					atomic.AddInt64(&totalErrors, 1)
				} else {
					atomic.AddInt64(&totalSuccess, 1)
					// Only record latencies of successful operations
					latencies = append(latencies, duration)
				}

				idx = (idx + 1) % cfg.KeyCount
				opCount++
			}
		}(i, clients[i])
	}

	// Wait for duration or ops limit
	if cfg.Duration > 0 {
		time.Sleep(cfg.Duration)
		close(stopSignal)
	}
	wg.Wait()
	actualDuration := time.Since(startTime)

	// Snapshot end resources
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	endGoroutines := runtime.NumGoroutine()

	// Aggregate and sort all recorded latencies across workers
	totalRecorded := 0
	for _, l := range workerLatencies {
		totalRecorded += len(l)
	}

	allLatencies := make([]time.Duration, 0, totalRecorded)
	for _, l := range workerLatencies {
		allLatencies = append(allLatencies, l...)
	}

	res := &Result{
		Config:          cfg,
		TotalOps:        totalSuccess + totalErrors,
		SuccessfulOps:   totalSuccess,
		ErrorOps:        totalErrors,
		Duration:        actualDuration,
		OpsPerSec:       float64(totalSuccess) / actualDuration.Seconds(),
		StartGoroutines: startGoroutines,
		EndGoroutines:   endGoroutines,
		StartAllocBytes: memBefore.Alloc,
		EndAllocBytes:   memAfter.Alloc,
	}

	if len(allLatencies) > 0 {
		sort.Slice(allLatencies, func(i, j int) bool {
			return allLatencies[i] < allLatencies[j]
		})

		var totalDuration time.Duration
		for _, d := range allLatencies {
			totalDuration += d
		}

		res.MinLatency = allLatencies[0]
		res.MaxLatency = allLatencies[len(allLatencies)-1]
		res.AvgLatency = totalDuration / time.Duration(len(allLatencies))
		res.P50Latency = allLatencies[int(float64(len(allLatencies))*0.50)]
		res.P95Latency = allLatencies[int(float64(len(allLatencies))*0.95)]
		res.P99Latency = allLatencies[int(float64(len(allLatencies))*0.99)]
	}

	return res, nil
}

// RandomHex generates a short random hex identifier for unique keys.
func RandomHex(n int) string {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "rand"
	}
	return hex.EncodeToString(bytes)
}
