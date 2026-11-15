package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"miniredis/loadtest"
)

func main() {
	host := flag.String("host", "127.0.0.1", "Target MiniRedis host")
	port := flag.String("port", "6379", "Target MiniRedis port")
	clients := flag.Int("clients", 10, "Number of concurrent persistent client connections")
	durationStr := flag.String("duration", "5s", "Load test measurement duration (e.g., 5s, 10s)")
	warmupStr := flag.String("warmup", "1s", "Warm-up duration before metrics recording starts")
	workloadStr := flag.String("workload", "MIXED", "Workload type: GET, SET, or MIXED (50/50)")
	payloadSize := flag.Int("payload", 16, "Payload size in bytes for SET values")
	keyCount := flag.Int("keys", 10000, "Number of unique keys in the key universe")
	prepopulate := flag.Bool("prepopulate", true, "Pre-populate keys before testing GET/MIXED workloads")
	flag.Parse()

	duration, err := time.ParseDuration(*durationStr)
	if err != nil {
		fmt.Printf("Invalid duration: %v\n", err)
		os.Exit(1)
	}

	warmup, err := time.ParseDuration(*warmupStr)
	if err != nil {
		fmt.Printf("Invalid warmup duration: %v\n", err)
		os.Exit(1)
	}

	targetAddr := fmt.Sprintf("%s:%s", *host, *port)
	workload := loadtest.WorkloadType(*workloadStr)

	fmt.Println("==============================================================")
	fmt.Println("             MiniRedis Load Testing Tool                      ")
	fmt.Println("==============================================================")
	fmt.Printf("Target:           %s\n", targetAddr)
	fmt.Printf("Concurrent Clients: %d\n", *clients)
	fmt.Printf("Duration:         %s (Warm-up: %s)\n", duration, warmup)
	fmt.Printf("Workload:         %s\n", workload)
	fmt.Printf("Key Universe:     %d keys\n", *keyCount)
	fmt.Printf("Payload Size:     %d bytes\n", *payloadSize)
	fmt.Println("--------------------------------------------------------------")

	// Pre-populate if requested
	if *prepopulate && (workload == loadtest.WorkloadGet || workload == loadtest.WorkloadMixed) {
		fmt.Printf("[*] Pre-populating %d keys on %s...\n", *keyCount, targetAddr)
		if err := loadtest.PrePopulateKeys(targetAddr, *keyCount, *payloadSize); err != nil {
			fmt.Printf("[-] Pre-population failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[+] Pre-population complete.")
	}

	cfg := loadtest.Config{
		TargetAddr:     targetAddr,
		Clients:        *clients,
		Duration:       duration,
		WarmupDuration: warmup,
		Workload:       workload,
		PayloadSize:    *payloadSize,
		KeyCount:       *keyCount,
	}

	fmt.Printf("[*] Starting load test with %d clients for %s...\n", *clients, duration)
	result, err := loadtest.Run(cfg)
	if err != nil {
		fmt.Printf("[-] Load test failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("==============================================================")
	fmt.Println("                     Test Results                             ")
	fmt.Println("==============================================================")
	fmt.Printf("Total Operations:    %d\n", result.TotalOps)
	fmt.Printf("Successful Ops:      %d\n", result.SuccessfulOps)
	fmt.Printf("Failed Ops (Errors): %d\n", result.ErrorOps)
	fmt.Printf("Actual Duration:     %v\n", result.Duration.Round(time.Millisecond))
	fmt.Printf("Throughput (RPS):    %.2f req/sec\n", result.OpsPerSec)
	fmt.Println("--------------------------------------------------------------")
	fmt.Printf("Average Latency:     %v\n", result.AvgLatency)
	fmt.Printf("p50 (Median):        %v\n", result.P50Latency)
	fmt.Printf("p95 Latency:         %v\n", result.P95Latency)
	fmt.Printf("p99 Latency:         %v\n", result.P99Latency)
	fmt.Printf("Max Latency:         %v\n", result.MaxLatency)
	fmt.Println("==============================================================")
}
