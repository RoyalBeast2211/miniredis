package stats

import (
	"strings"
	"testing"
)

func TestMetrics_Counters(t *testing.T) {
	m := NewMetrics()

	m.ConnAccepted()
	m.ConnAccepted()
	m.ConnClosed()
	m.ConnRejected()

	if m.TotalConnections.Load() != 2 {
		t.Errorf("expected 2 total connections, got %d", m.TotalConnections.Load())
	}
	if m.ActiveConnections.Load() != 1 {
		t.Errorf("expected 1 active connection, got %d", m.ActiveConnections.Load())
	}
	if m.RejectedConnections.Load() != 1 {
		t.Errorf("expected 1 rejected connection, got %d", m.RejectedConnections.Load())
	}

	m.IncrCommands()
	m.IncrWrites()
	m.IncrReads()

	if m.TotalCommands.Load() != 1 || m.TotalWrites.Load() != 1 || m.TotalReads.Load() != 1 {
		t.Errorf("command counters mismatch")
	}
}

func TestMetrics_FormatInfo(t *testing.T) {
	m := NewMetrics()
	m.ConnAccepted()
	m.IncrCommands()

	// 1. All sections
	allInfo := m.FormatInfo("", map[string]string{
		"Replication": "role:master\r\nconnected_slaves:0\r\n",
	})

	if !strings.Contains(allInfo, "# Server") ||
		!strings.Contains(allInfo, "# Clients") ||
		!strings.Contains(allInfo, "# Memory") ||
		!strings.Contains(allInfo, "# Stats") ||
		!strings.Contains(allInfo, "# Replication") {
		t.Errorf("missing sections in full info: %s", allInfo)
	}

	// 2. Specific section
	clientsOnly := m.FormatInfo("clients", nil)
	if !strings.Contains(clientsOnly, "# Clients") || strings.Contains(clientsOnly, "# Server") {
		t.Errorf("clients section filtering failed: %s", clientsOnly)
	}
}
