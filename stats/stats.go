package stats

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

// Metrics manages atomic runtime metrics and telemetry for the MiniRedis instance.
type Metrics struct {
	TotalConnections    atomic.Int64
	ActiveConnections   atomic.Int64
	RejectedConnections atomic.Int64
	TotalCommands       atomic.Int64
	TotalWrites         atomic.Int64
	TotalReads          atomic.Int64

	StartTime time.Time
	PID       int
}

// NewMetrics initializes a fresh metrics tracker.
func NewMetrics() *Metrics {
	return &Metrics{
		StartTime: time.Now(),
		PID:       os.Getpid(),
	}
}

// ConnAccepted records a newly established client connection.
func (m *Metrics) ConnAccepted() {
	m.TotalConnections.Add(1)
	m.ActiveConnections.Add(1)
}

// ConnClosed records the termination of an active client connection.
func (m *Metrics) ConnClosed() {
	m.ActiveConnections.Add(-1)
}

// ConnRejected records a connection rejected due to MaxClients or system exhaustion.
func (m *Metrics) ConnRejected() {
	m.RejectedConnections.Add(1)
}

// IncrCommands records an executed command.
func (m *Metrics) IncrCommands() {
	m.TotalCommands.Add(1)
}

// IncrWrites records a state-mutating write command.
func (m *Metrics) IncrWrites() {
	m.TotalWrites.Add(1)
}

// IncrReads records a read query.
func (m *Metrics) IncrReads() {
	m.TotalReads.Add(1)
}

// humanBytes converts a raw byte count into a readable string (e.g. "1.42M").
func humanBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%c", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatInfo generates a Redis-compatible multi-line text report suitable for the INFO command.
// If section is empty or "default" / "all", all standard sections are returned.
func (m *Metrics) FormatInfo(section string, extraSections map[string]string) string {
	section = strings.ToLower(strings.TrimSpace(section))
	showAll := section == "" || section == "all" || section == "default"

	var sb strings.Builder
	uptime := int64(time.Since(m.StartTime).Seconds())

	// # Server
	if showAll || section == "server" {
		sb.WriteString("# Server\r\n")
		sb.WriteString("redis_version:miniredis-1.0.0\r\n")
		sb.WriteString("redis_mode:standalone\r\n")
		sb.WriteString(fmt.Sprintf("os:%s\r\n", runtime.GOOS))
		sb.WriteString(fmt.Sprintf("arch:%s\r\n", runtime.GOARCH))
		sb.WriteString(fmt.Sprintf("go_version:%s\r\n", runtime.Version()))
		sb.WriteString(fmt.Sprintf("process_id:%d\r\n", m.PID))
		sb.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", uptime))
		sb.WriteString(fmt.Sprintf("uptime_in_days:%d\r\n", uptime/86400))
		sb.WriteString("\r\n")
	}

	// # Clients
	if showAll || section == "clients" {
		sb.WriteString("# Clients\r\n")
		sb.WriteString(fmt.Sprintf("connected_clients:%d\r\n", m.ActiveConnections.Load()))
		sb.WriteString(fmt.Sprintf("rejected_connections:%d\r\n", m.RejectedConnections.Load()))
		sb.WriteString("\r\n")
	}

	// # Memory
	if showAll || section == "memory" {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		sb.WriteString("# Memory\r\n")
		sb.WriteString(fmt.Sprintf("used_memory:%d\r\n", mem.Alloc))
		sb.WriteString(fmt.Sprintf("used_memory_human:%s\r\n", humanBytes(mem.Alloc)))
		sb.WriteString(fmt.Sprintf("used_memory_rss:%d\r\n", mem.Sys))
		sb.WriteString(fmt.Sprintf("used_memory_rss_human:%s\r\n", humanBytes(mem.Sys)))
		sb.WriteString(fmt.Sprintf("total_allocated:%d\r\n", mem.TotalAlloc))
		sb.WriteString("\r\n")
	}

	// # Stats
	if showAll || section == "stats" {
		sb.WriteString("# Stats\r\n")
		sb.WriteString(fmt.Sprintf("total_connections_received:%d\r\n", m.TotalConnections.Load()))
		sb.WriteString(fmt.Sprintf("total_commands_processed:%d\r\n", m.TotalCommands.Load()))
		sb.WriteString(fmt.Sprintf("total_writes_processed:%d\r\n", m.TotalWrites.Load()))
		sb.WriteString(fmt.Sprintf("total_reads_processed:%d\r\n", m.TotalReads.Load()))
		sb.WriteString("\r\n")
	}

	// Dynamic sections supplied by caller (e.g. Replication, Persistence, Keyspace)
	for secName, secContent := range extraSections {
		if showAll || section == strings.ToLower(secName) {
			sb.WriteString(fmt.Sprintf("# %s\r\n", secName))
			sb.WriteString(secContent)
			sb.WriteString("\r\n")
		}
	}

	return strings.TrimSpace(sb.String()) + "\r\n"
}
