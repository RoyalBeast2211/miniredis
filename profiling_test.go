package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"miniredis/aof"
	"miniredis/loadtest"
	"miniredis/storage"
)

const profileDir = "profiles"

// ensureProfileDir creates the profiles output directory.
func ensureProfileDir(t *testing.T) string {
	err := os.MkdirAll(profileDir, 0755)
	if err != nil {
		t.Fatalf("failed to create profile dir: %v", err)
	}
	return profileDir
}

// TestProfileCPU runs controlled workloads while capturing CPU profiles.
func TestProfileCPU(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	dir := ensureProfileDir(t)

	scenarios := []struct {
		name     string
		workload loadtest.WorkloadType
		clients  int
		fileName string
	}{
		{"GET_Heavy", loadtest.WorkloadGet, 50, "cpu_get.pprof"},
		{"SET_Heavy", loadtest.WorkloadSet, 50, "cpu_set.pprof"},
		{"Mixed_50_50", loadtest.WorkloadMixed, 50, "cpu_mixed.pprof"},
		{"High_Concurrency_200", loadtest.WorkloadMixed, 200, "cpu_high_concurrency.pprof"},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			store := storage.NewStoreWithInterval(0)
			addr, cleanup := startLoadTestServer(t, store, nil)
			defer cleanup()

			if sc.workload == loadtest.WorkloadGet || sc.workload == loadtest.WorkloadMixed {
				if err := loadtest.PrePopulateKeys(addr, 10000, 16); err != nil {
					t.Fatalf("pre-populate failed: %v", err)
				}
			}

			filePath := filepath.Join(dir, sc.fileName)
			f, err := os.Create(filePath)
			if err != nil {
				t.Fatalf("failed to create cpu profile file: %v", err)
			}
			defer f.Close()

			if err := pprof.StartCPUProfile(f); err != nil {
				t.Fatalf("failed to start cpu profile: %v", err)
			}

			cfg := loadtest.Config{
				TargetAddr:     addr,
				Clients:        sc.clients,
				Duration:       3 * time.Second,
				WarmupDuration: 500 * time.Millisecond,
				Workload:       sc.workload,
				PayloadSize:    16,
				KeyCount:       10000,
			}

			_, err = loadtest.Run(cfg)
			pprof.StopCPUProfile()

			if err != nil {
				t.Fatalf("loadtest failed: %v", err)
			}

			fmt.Printf("[+] Generated CPU profile: %s\n", filePath)
		})
	}
}

// TestProfileMemory captures heap and allocation profiles under MIXED and LRU workloads.
func TestProfileMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	dir := ensureProfileDir(t)

	// Set allocation profiling rate to capture every single allocation
	runtime.MemProfileRate = 1

	// Scenario 1: Memory profile for standard in-memory MIXED workload
	{
		store := storage.NewStoreWithInterval(0)
		addr, cleanup := startLoadTestServer(t, store, nil)
		defer cleanup()

		if err := loadtest.PrePopulateKeys(addr, 10000, 16); err != nil {
			t.Fatalf("pre-populate failed: %v", err)
		}

		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadMixed,
			PayloadSize:    16,
			KeyCount:       10000,
		}

		_, err := loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("loadtest failed: %v", err)
		}

		filePath := filepath.Join(dir, "mem_mixed.pprof")
		f, err := os.Create(filePath)
		if err != nil {
			t.Fatalf("failed to create mem profile: %v", err)
		}
		defer f.Close()

		runtime.GC() // Run GC before capturing heap to see live vs allocated objects
		if err := pprof.WriteHeapProfile(f); err != nil {
			t.Fatalf("failed to write heap profile: %v", err)
		}
		fmt.Printf("[+] Generated Memory profile: %s\n", filePath)
	}

	// Scenario 2: Memory profile under LRU eviction (fixed capacity)
	{
		store := storage.NewStoreWithConfig(1000, 0)
		addr, cleanup := startLoadTestServer(t, store, nil)
		defer cleanup()

		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadSet,
			PayloadSize:    64,
			KeyCount:       50000, // Forces continuous LRU eviction
		}

		_, err := loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("loadtest failed: %v", err)
		}

		filePath := filepath.Join(dir, "mem_lru.pprof")
		f, err := os.Create(filePath)
		if err != nil {
			t.Fatalf("failed to create mem profile: %v", err)
		}
		defer f.Close()

		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			t.Fatalf("failed to write lru heap profile: %v", err)
		}
		fmt.Printf("[+] Generated LRU Memory profile: %s\n", filePath)
	}
}

// TestProfileConcurrency captures mutex contention, block delays, and goroutine stack dumps.
func TestProfileConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}

	dir := ensureProfileDir(t)

	// Enable 100% mutex and block profile sampling
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	defer func() {
		runtime.SetMutexProfileFraction(0)
		runtime.SetBlockProfileRate(0)
	}()

	// Sub-test 1: Mutex contention under MIXED workload (50 clients)
	t.Run("Mutex_Mixed_Contention", func(t *testing.T) {
		store := storage.NewStoreWithInterval(0)
		addr, cleanup := startLoadTestServer(t, store, nil)
		defer cleanup()

		if err := loadtest.PrePopulateKeys(addr, 10000, 16); err != nil {
			t.Fatalf("pre-populate failed: %v", err)
		}

		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadMixed,
			PayloadSize:    16,
			KeyCount:       10000,
		}

		_, err := loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("loadtest failed: %v", err)
		}

		// Write Mutex Profile
		mutexFile := filepath.Join(dir, "mutex_mixed.pprof")
		mf, err := os.Create(mutexFile)
		if err != nil {
			t.Fatalf("failed to create mutex profile: %v", err)
		}
		defer mf.Close()
		if err := pprof.Lookup("mutex").WriteTo(mf, 0); err != nil {
			t.Fatalf("failed to write mutex profile: %v", err)
		}
		fmt.Printf("[+] Generated Mutex profile: %s\n", mutexFile)

		// Write Block Profile
		blockFile := filepath.Join(dir, "block_mixed.pprof")
		bf, err := os.Create(blockFile)
		if err != nil {
			t.Fatalf("failed to create block profile: %v", err)
		}
		defer bf.Close()
		if err := pprof.Lookup("block").WriteTo(bf, 0); err != nil {
			t.Fatalf("failed to write block profile: %v", err)
		}
		fmt.Printf("[+] Generated Block profile: %s\n", blockFile)

		// Write Goroutine Profile
		goroutineFile := filepath.Join(dir, "goroutine_mixed.pprof")
		gf, err := os.Create(goroutineFile)
		if err != nil {
			t.Fatalf("failed to create goroutine profile: %v", err)
		}
		defer gf.Close()
		if err := pprof.Lookup("goroutine").WriteTo(gf, 0); err != nil {
			t.Fatalf("failed to write goroutine profile: %v", err)
		}
		fmt.Printf("[+] Generated Goroutine profile: %s\n", goroutineFile)
	})

	// Sub-test 2: Mutex contention with AOF persistence enabled (everysec)
	t.Run("Mutex_AOF_Contention", func(t *testing.T) {
		tempDir, err := os.MkdirTemp(".", "miniredis_prof_aof_*")
		if err != nil {
			t.Fatalf("failed to create temp dir: %v", err)
		}
		defer os.RemoveAll(tempDir)

		aofPath := filepath.Join(tempDir, "profile_everysec.aof")
		aofWriter, err := aof.Open(aofPath, aof.SyncEverySecond)
		if err != nil {
			t.Fatalf("failed to open aof: %v", err)
		}

		store := storage.NewStoreWithInterval(0)
		addr, cleanup := startLoadTestServer(t, store, aofWriter)
		defer cleanup()

		cfg := loadtest.Config{
			TargetAddr:     addr,
			Clients:        50,
			Duration:       3 * time.Second,
			WarmupDuration: 500 * time.Millisecond,
			Workload:       loadtest.WorkloadSet,
			PayloadSize:    16,
			KeyCount:       10000,
		}

		_, err = loadtest.Run(cfg)
		if err != nil {
			t.Fatalf("loadtest failed: %v", err)
		}

		mutexFile := filepath.Join(dir, "mutex_aof.pprof")
		mf, err := os.Create(mutexFile)
		if err != nil {
			t.Fatalf("failed to create aof mutex profile: %v", err)
		}
		defer mf.Close()
		if err := pprof.Lookup("mutex").WriteTo(mf, 0); err != nil {
			t.Fatalf("failed to write aof mutex profile: %v", err)
		}
		fmt.Printf("[+] Generated AOF Mutex profile: %s\n", mutexFile)
	})
}
