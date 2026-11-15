package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"miniredis/aof"
	"miniredis/loadtest"
	"miniredis/storage"
)

// startLoadTestServer starts an ephemeral MiniRedis TCP server for load testing.
func startLoadTestServer(t testing.TB, store *storage.Store, aofWriter *aof.AOF) (string, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to bind load test listener: %v", err)
	}

	go func() {
		_ = runServer(listener, store, aofWriter)
	}()

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

// TestLoadTestMatrixConcurrency executes the concurrency scaling matrix (1, 10, 25, 50, 100, 250 clients).
func TestLoadTestMatrixConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping load test matrix in short mode")
	}

	clientCounts := []int{1, 10, 25, 50, 100, 250}

	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startLoadTestServer(t, store, nil)
	defer cleanup()

	// Pre-populate keys for GET / MIXED workloads
	const keyCount = 10000
	if err := loadtest.PrePopulateKeys(addr, keyCount, 16); err != nil {
		t.Fatalf("failed to pre-populate keys: %v", err)
	}

	fmt.Println("\n==========================================================================================================")
	fmt.Println("                       MiniRedis Load Test Matrix: Concurrency Scaling (50% GET / 50% SET)                 ")
	fmt.Println("==========================================================================================================")
	fmt.Printf("%-8s | %-12s | %-12s | %-10s | %-10s | %-10s | %-10s | %-8s\n",
		"Clients", "Throughput", "Total Ops", "p50 Lat", "p95 Lat", "p99 Lat", "Max Lat", "Errors")
	fmt.Println("---------+--------------+--------------+------------+------------+------------+------------+----------")

	for _, clients := range clientCounts {
		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        clients,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadMixed,
			PayloadSize:    16,
			KeyCount:       keyCount,
		}

		res, err := loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("load test failed for %d clients: %v", clients, err)
		}

		fmt.Printf("%-8d | %9.1f r/s | %12d | %10v | %10v | %10v | %10v | %-8d\n",
			clients, res.OpsPerSec, res.TotalOps, res.P50Latency, res.P95Latency, res.P99Latency, res.MaxLatency, res.ErrorOps)
	}
	fmt.Println("==========================================================================================================")
}

// TestLoadTestWorkloadComparison compares GET-only, SET-only, and MIXED workloads under 50 clients.
func TestLoadTestWorkloadComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	workloads := []loadtest.WorkloadType{
		loadtest.WorkloadGet,
		loadtest.WorkloadSet,
		loadtest.WorkloadMixed,
	}

	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startLoadTestServer(t, store, nil)
	defer cleanup()

	const keyCount = 10000
	if err := loadtest.PrePopulateKeys(addr, keyCount, 16); err != nil {
		t.Fatalf("failed to pre-populate keys: %v", err)
	}

	fmt.Println("\n==========================================================================================================")
	fmt.Println("                       MiniRedis Load Test: Workload Comparison (50 Clients, 16B Payload)                  ")
	fmt.Println("==========================================================================================================")
	fmt.Printf("%-10s | %-12s | %-12s | %-10s | %-10s | %-10s | %-10s | %-8s\n",
		"Workload", "Throughput", "Total Ops", "p50 Lat", "p95 Lat", "p99 Lat", "Max Lat", "Errors")
	fmt.Println("-----------+--------------+--------------+------------+------------+------------+------------+----------")

	for _, w := range workloads {
		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       w,
			PayloadSize:    16,
			KeyCount:       keyCount,
		}

		res, err := loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("load test failed for workload %s: %v", w, err)
		}

		fmt.Printf("%-10s | %9.1f r/s | %12d | %10v | %10v | %10v | %10v | %-8d\n",
			w, res.OpsPerSec, res.TotalOps, res.P50Latency, res.P95Latency, res.P99Latency, res.MaxLatency, res.ErrorOps)
	}
	fmt.Println("==========================================================================================================")
}

// TestLoadTestPayloadComparison compares 16B vs 1KB vs 4KB payload sizes under 50 clients.
func TestLoadTestPayloadComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	payloads := []int{16, 1024, 4096}

	store := storage.NewStoreWithInterval(0)
	addr, cleanup := startLoadTestServer(t, store, nil)
	defer cleanup()

	fmt.Println("\n==========================================================================================================")
	fmt.Println("                       MiniRedis Load Test: Payload Size Comparison (50 Clients, SET Workload)             ")
	fmt.Println("==========================================================================================================")
	fmt.Printf("%-10s | %-12s | %-12s | %-10s | %-10s | %-10s | %-10s | %-8s\n",
		"Payload", "Throughput", "Total Ops", "p50 Lat", "p95 Lat", "p99 Lat", "Max Lat", "Errors")
	fmt.Println("-----------+--------------+--------------+------------+------------+------------+------------+----------")

	for _, p := range payloads {
		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadSet,
			PayloadSize:    p,
			KeyCount:       10000,
		}

		res, err := loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("load test failed for payload %d: %v", p, err)
		}

		fmt.Printf("%-10s | %9.1f r/s | %12d | %10v | %10v | %10v | %10v | %-8d\n",
			fmt.Sprintf("%d Bytes", p), res.OpsPerSec, res.TotalOps, res.P50Latency, res.P95Latency, res.P99Latency, res.MaxLatency, res.ErrorOps)
	}
	fmt.Println("==========================================================================================================")
}

// TestLoadTestPersistenceComparison compares throughput with AOF disabled vs AOF SyncEverySecond.
func TestLoadTestPersistenceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	tempDir, err := os.MkdirTemp(".", "miniredis_loadtest_aof_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fmt.Println("\n==========================================================================================================")
	fmt.Println("                       MiniRedis Load Test: Persistence Impact (50 Clients, SET Workload)                  ")
	fmt.Println("==========================================================================================================")
	fmt.Printf("%-16s | %-12s | %-12s | %-10s | %-10s | %-10s | %-10s | %-8s\n",
		"Persistence", "Throughput", "Total Ops", "p50 Lat", "p95 Lat", "p99 Lat", "Max Lat", "Errors")
	fmt.Println("-----------------+--------------+--------------+------------+------------+------------+------------+----------")

	// 1. In-memory only (AOF disabled)
	{
		store := storage.NewStoreWithInterval(0)
		addr, cleanup := startLoadTestServer(t, store, nil)

		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadSet,
			PayloadSize:    16,
			KeyCount:       10000,
		}

		res, err := loadtest.Run(cfg)
		cleanup()
		if err != nil {
			t.Fatalf("load test failed: %v", err)
		}

		fmt.Printf("%-16s | %9.1f r/s | %12d | %10v | %10v | %10v | %10v | %-8d\n",
			"Disabled (RAM)", res.OpsPerSec, res.TotalOps, res.P50Latency, res.P95Latency, res.P99Latency, res.MaxLatency, res.ErrorOps)
	}

	// 2. AOF SyncEverySecond (Redis default)
	{
		store := storage.NewStoreWithInterval(0)
		aofPath := filepath.Join(tempDir, "loadtest_everysec.aof")
		aofWriter, err := aof.Open(aofPath, aof.SyncEverySecond)
		if err != nil {
			t.Fatalf("failed to open aof: %v", err)
		}
		addr, cleanup := startLoadTestServer(t, store, aofWriter)

		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadSet,
			PayloadSize:    16,
			KeyCount:       10000,
		}

		res, err := loadtest.Run(cfg)
		cleanup()
		if err != nil {
			t.Fatalf("load test failed: %v", err)
		}

		fmt.Printf("%-16s | %9.1f r/s | %12d | %10v | %10v | %10v | %10v | %-8d\n",
			"AOF EverySecond", res.OpsPerSec, res.TotalOps, res.P50Latency, res.P95Latency, res.P99Latency, res.MaxLatency, res.ErrorOps)
	}
	fmt.Println("==========================================================================================================")
}

// TestLoadTestReliabilitySustained runs a 15-second sustained load test with 50 concurrent clients
// to measure system stability, connection health, memory growth, and goroutine behavior over time.
func TestLoadTestReliabilitySustained(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	store := storage.NewStoreWithInterval(100 * time.Millisecond)
	addr, cleanup := startLoadTestServer(t, store, nil)
	defer cleanup()

	const keyCount = 10000
	if err := loadtest.PrePopulateKeys(addr, keyCount, 64); err != nil {
		t.Fatalf("failed to pre-populate keys: %v", err)
	}

	fmt.Println("\n==========================================================================================================")
	fmt.Println("                       MiniRedis Load Test: Sustained Reliability (15 Seconds, 50 Clients)                  ")
	fmt.Println("==========================================================================================================")

	cfg := loadtest.Config{
		TargetAddr:     addr,
		Clients:        50,
		Duration:       15 * time.Second,
		WarmupDuration: 1 * time.Second,
		Workload:       loadtest.WorkloadMixed,
		PayloadSize:    64,
		KeyCount:       keyCount,
	}

	res, err := loadtest.Run(cfg)
	if err != nil {
		t.Fatalf("sustained reliability load test failed: %v", err)
	}

	fmt.Printf("Total Duration:      %v\n", res.Duration.Round(time.Millisecond))
	fmt.Printf("Total Operations:    %d\n", res.TotalOps)
	fmt.Printf("Successful Ops:      %d\n", res.SuccessfulOps)
	fmt.Printf("Failed Ops (Errors): %d (Error Rate: %.4f%%)\n", res.ErrorOps, float64(res.ErrorOps)*100.0/float64(res.TotalOps))
	fmt.Printf("Sustained RPS:       %.1f req/sec\n", res.OpsPerSec)
	fmt.Printf("p50 Latency:         %v\n", res.P50Latency)
	fmt.Printf("p95 Latency:         %v\n", res.P95Latency)
	fmt.Printf("p99 Latency:         %v\n", res.P99Latency)
	fmt.Printf("Max Latency:         %v\n", res.MaxLatency)
	fmt.Printf("Active Goroutines:   Before=%d, After=%d (Delta=%+d)\n",
		res.StartGoroutines, res.EndGoroutines, res.EndGoroutines-res.StartGoroutines)
	fmt.Printf("Heap Memory:         Before=%.2f MB, After=%.2f MB (Delta=%+.2f MB)\n",
		float64(res.StartAllocBytes)/(1024*1024), float64(res.EndAllocBytes)/(1024*1024),
		float64(int64(res.EndAllocBytes)-int64(res.StartAllocBytes))/(1024*1024))
	fmt.Println("==========================================================================================================")

	if res.ErrorOps > 0 {
		t.Errorf("expected zero errors during sustained load test, got %d", res.ErrorOps)
	}
}
