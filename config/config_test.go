package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfig_Defaults(t *testing.T) {
	cfg := NewDefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}

	if cfg.Port != 6379 || cfg.Host != "127.0.0.1" || cfg.MaxClients != 10000 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
}

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{
			name:    "invalid low port",
			mutate:  func(c *Config) { c.Port = 0 },
			wantErr: true,
		},
		{
			name:    "invalid high port",
			mutate:  func(c *Config) { c.Port = 70000 },
			wantErr: true,
		},
		{
			name:    "same port and health port",
			mutate:  func(c *Config) { c.Port = 6379; c.HealthPort = 6379 },
			wantErr: true,
		},
		{
			name:    "invalid appendfsync",
			mutate:  func(c *Config) { c.AppendFsync = "sometimes" },
			wantErr: true,
		},
		{
			name:    "invalid loglevel",
			mutate:  func(c *Config) { c.LogLevel = "verbose" },
			wantErr: true,
		},
		{
			name:    "zero max clients",
			mutate:  func(c *Config) { c.MaxClients = 0 },
			wantErr: true,
		},
		{
			name:    "tiny repl backlog size",
			mutate:  func(c *Config) { c.ReplBacklogSize = 100 },
			wantErr: true,
		},
		{
			name:    "empty aof path",
			mutate:  func(c *Config) { c.AOFPath = "  " },
			wantErr: true,
		},
		{
			name:    "zero shutdown timeout",
			mutate:  func(c *Config) { c.ShutdownTimeout = 0 },
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := NewDefaultConfig()
			tc.mutate(cfg)
			err := cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestConfig_LoadConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	confPath := filepath.Join(tempDir, "miniredis.conf")

	content := `
# MiniRedis Configuration File Example
bind 0.0.0.0
port 6380
maxkeys 50000
appendonly-file custom.aof
dbfilename custom.rdb
appendfsync always
maxclients 500
timeout 60
replicaof 10.0.0.1 6379
repl-backlog-size 2097152
loglevel debug
health-port 8080
pprof-port 6060
shutdown-timeout 10
`
	if err := os.WriteFile(confPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test conf file: %v", err)
	}

	cfg := NewDefaultConfig()
	if err := cfg.LoadConfigFile(confPath); err != nil {
		t.Fatalf("failed to load config file: %v", err)
	}

	if cfg.Host != "0.0.0.0" {
		t.Errorf("expected host 0.0.0.0, got %s", cfg.Host)
	}
	if cfg.Port != 6380 {
		t.Errorf("expected port 6380, got %d", cfg.Port)
	}
	if cfg.MaxKeys != 50000 {
		t.Errorf("expected maxkeys 50000, got %d", cfg.MaxKeys)
	}
	if cfg.AOFPath != "custom.aof" {
		t.Errorf("expected AOFPath custom.aof, got %s", cfg.AOFPath)
	}
	if cfg.AppendFsync != "always" {
		t.Errorf("expected AppendFsync always, got %s", cfg.AppendFsync)
	}
	if cfg.ReplicaOf != "10.0.0.1:6379" {
		t.Errorf("expected ReplicaOf 10.0.0.1:6379, got %s", cfg.ReplicaOf)
	}
	if cfg.ReplBacklogSize != 2097152 {
		t.Errorf("expected ReplBacklogSize 2097152, got %d", cfg.ReplBacklogSize)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("expected LogLevel debug, got %s", cfg.LogLevel)
	}
	if cfg.HealthPort != 8080 {
		t.Errorf("expected HealthPort 8080, got %d", cfg.HealthPort)
	}
	if cfg.ClientTimeout != 60*time.Second {
		t.Errorf("expected ClientTimeout 60s, got %v", cfg.ClientTimeout)
	}
}

func TestConfig_FlagsOverrideConfigFile(t *testing.T) {
	tempDir := t.TempDir()
	confPath := filepath.Join(tempDir, "miniredis.conf")

	content := "port 6380\nloglevel debug\n"
	if err := os.WriteFile(confPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write conf: %v", err)
	}

	// Flag -port 6385 should override 6380 from config file
	args := []string{"-config", confPath, "-port", "6385", "-loglevel", "warn"}
	cfg, err := LoadFromFlags(args)
	if err != nil {
		t.Fatalf("unexpected flag load error: %v", err)
	}

	if cfg.Port != 6385 {
		t.Errorf("expected port 6385 from flag override, got %d", cfg.Port)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("expected loglevel warn from flag override, got %s", cfg.LogLevel)
	}
}
