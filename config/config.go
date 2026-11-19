package config

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Default configuration values
const (
	DefaultHost            = "127.0.0.1"
	DefaultPort            = 6379
	DefaultMaxKeys         = 0 // 0 = unlimited
	DefaultAOFPath         = "appendonly.aof"
	DefaultSnapshotPath    = "dump.rdb"
	DefaultAppendFsync     = "everysec"
	DefaultCleanupInterval = 1 * time.Second
	DefaultMaxClients      = 10000
	DefaultClientTimeout   = 0           // 0 = no timeout
	DefaultReplBacklogSize = 1024 * 1024 // 1 MB
	DefaultLogLevel        = "info"
	DefaultShutdownTimeout = 5 * time.Second
)

// Config represents all operational configuration parameters for MiniRedis.
type Config struct {
	// Network binding
	Host string
	Port int

	// In-memory storage & eviction
	MaxKeys int

	// Persistence
	AOFPath      string
	SnapshotPath string
	AppendFsync  string // "always", "everysec", "no"

	// Active expiration
	CleanupInterval time.Duration

	// Resource limits
	MaxClients    int
	ClientTimeout time.Duration

	// Replication
	ReplicaOf       string // "host port" or "host:port"
	ReplBacklogSize int64

	// Observability & Management
	LogLevel        string // "debug", "info", "warn", "error"
	HealthPort      int    // 0 = disabled
	PprofPort       int    // 0 = disabled
	ShutdownTimeout time.Duration

	// Source configuration file (if loaded from file)
	ConfigFile string
}

// NewDefaultConfig initializes a Config with sensible production defaults.
func NewDefaultConfig() *Config {
	return &Config{
		Host:            DefaultHost,
		Port:            DefaultPort,
		MaxKeys:         DefaultMaxKeys,
		AOFPath:         DefaultAOFPath,
		SnapshotPath:    DefaultSnapshotPath,
		AppendFsync:     DefaultAppendFsync,
		CleanupInterval: DefaultCleanupInterval,
		MaxClients:      DefaultMaxClients,
		ClientTimeout:   DefaultClientTimeout,
		ReplicaOf:       "",
		ReplBacklogSize: DefaultReplBacklogSize,
		LogLevel:        DefaultLogLevel,
		HealthPort:      0,
		PprofPort:       0,
		ShutdownTimeout: DefaultShutdownTimeout,
	}
}

// Validate checks all configuration values for validity and sanity.
// Returns an error describing the exact invalid parameter if validation fails.
func (c *Config) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("config error: invalid port %d (must be between 1 and 65535)", c.Port)
	}

	if c.HealthPort != 0 && (c.HealthPort < 1 || c.HealthPort > 65535) {
		return fmt.Errorf("config error: invalid health-port %d (must be between 1 and 65535)", c.HealthPort)
	}

	if c.PprofPort != 0 && (c.PprofPort < 1 || c.PprofPort > 65535) {
		return fmt.Errorf("config error: invalid pprof-port %d (must be between 1 and 65535)", c.PprofPort)
	}

	if c.Port == c.HealthPort && c.HealthPort != 0 {
		return fmt.Errorf("config error: port and health-port cannot be the same (%d)", c.Port)
	}

	if c.MaxClients < 1 {
		return fmt.Errorf("config error: max-clients must be at least 1, got %d", c.MaxClients)
	}

	if c.ReplBacklogSize < 1024 {
		return fmt.Errorf("config error: repl-backlog-size must be at least 1024 bytes, got %d", c.ReplBacklogSize)
	}

	fsync := strings.ToLower(c.AppendFsync)
	if fsync != "always" && fsync != "everysec" && fsync != "no" {
		return fmt.Errorf("config error: invalid appendfsync %q (must be 'always', 'everysec', or 'no')", c.AppendFsync)
	}

	level := strings.ToLower(c.LogLevel)
	if level != "debug" && level != "info" && level != "warn" && level != "error" {
		return fmt.Errorf("config error: invalid log-level %q (must be 'debug', 'info', 'warn', or 'error')", c.LogLevel)
	}

	if strings.TrimSpace(c.AOFPath) == "" {
		return errors.New("config error: aof-path cannot be empty")
	}

	if strings.TrimSpace(c.SnapshotPath) == "" {
		return errors.New("config error: snapshot-path cannot be empty")
	}

	if c.ShutdownTimeout <= 0 {
		return errors.New("config error: shutdown-timeout must be positive")
	}

	return nil
}

// LoadConfigFile parses a Redis-style configuration file (e.g. miniredis.conf).
// Lines beginning with '#' or empty lines are ignored.
// Syntax: <directive> <value> [value2...]
func (c *Config) LoadConfigFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	c.ConfigFile = path
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			return fmt.Errorf("config file %s:%d: invalid directive %q", path, lineNum, line)
		}

		directive := strings.ToLower(parts[0])
		val := parts[1]

		switch directive {
		case "bind", "host":
			c.Host = val
		case "port":
			p, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid port %q", path, lineNum, val)
			}
			c.Port = p
		case "maxkeys", "max-keys":
			m, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid maxkeys %q", path, lineNum, val)
			}
			c.MaxKeys = m
		case "appendonly-file", "aof-path":
			c.AOFPath = val
		case "dbfilename", "snapshot-path":
			c.SnapshotPath = val
		case "appendfsync":
			c.AppendFsync = strings.ToLower(val)
		case "maxclients", "max-clients":
			mc, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid maxclients %q", path, lineNum, val)
			}
			c.MaxClients = mc
		case "timeout", "client-timeout":
			sec, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid client-timeout %q", path, lineNum, val)
			}
			c.ClientTimeout = time.Duration(sec) * time.Second
		case "replicaof", "slaveof":
			if len(parts) >= 3 {
				c.ReplicaOf = fmt.Sprintf("%s:%s", parts[1], parts[2])
			} else {
				c.ReplicaOf = val
			}
		case "repl-backlog-size":
			size, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid repl-backlog-size %q", path, lineNum, val)
			}
			c.ReplBacklogSize = size
		case "loglevel", "log-level":
			c.LogLevel = strings.ToLower(val)
		case "health-port":
			hp, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid health-port %q", path, lineNum, val)
			}
			c.HealthPort = hp
		case "pprof-port":
			pp, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid pprof-port %q", path, lineNum, val)
			}
			c.PprofPort = pp
		case "shutdown-timeout":
			sec, err := strconv.Atoi(val)
			if err != nil {
				return fmt.Errorf("config file %s:%d: invalid shutdown-timeout %q", path, lineNum, val)
			}
			c.ShutdownTimeout = time.Duration(sec) * time.Second
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading config file %s: %w", path, err)
	}

	return c.Validate()
}

// LoadFromFlags parses command line arguments and overlays them on top of the configuration.
// If `-config <path>` is provided, the file is loaded first, then any additional flags override it.
func LoadFromFlags(args []string) (*Config, error) {
	cfg := NewDefaultConfig()

	fs := flag.NewFlagSet("miniredis", flag.ContinueOnError)

	configPath := fs.String("config", "", "Path to configuration file (e.g. miniredis.conf)")
	host := fs.String("host", "", "Host address to bind to")
	port := fs.Int("port", 0, "TCP port to listen on")
	maxKeys := fs.Int("maxkeys", -1, "Maximum number of keys in LRU storage (0 = unlimited)")
	aofPath := fs.String("aof", "", "Path to Append-Only File")
	snapshotPath := fs.String("snapshot", "", "Path to database snapshot (dump.rdb)")
	appendFsync := fs.String("appendfsync", "", "fsync policy: always, everysec, no")
	maxClients := fs.Int("maxclients", 0, "Maximum concurrent client connections")
	clientTimeoutSec := fs.Int("timeout", -1, "Client idle timeout in seconds (0 = disabled)")
	replicaOf := fs.String("replicaof", "", "Primary address to replicate from (e.g. '127.0.0.1:6379')")
	replBacklogSize := fs.Int64("repl-backlog-size", 0, "Replication backlog ring buffer size in bytes")
	logLevel := fs.String("loglevel", "", "Log level: debug, info, warn, error")
	healthPort := fs.Int("health-port", -1, "HTTP health check port (0 = disabled)")
	pprofPort := fs.Int("pprof-port", -1, "HTTP pprof profiling port (0 = disabled)")
	shutdownTimeoutSec := fs.Int("shutdown-timeout", 0, "Graceful shutdown timeout in seconds")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// 1. If config file is specified, load it first
	if *configPath != "" {
		if err := cfg.LoadConfigFile(*configPath); err != nil {
			return nil, err
		}
	}

	// 2. Command-line flags take precedence over config file values
	if *host != "" {
		cfg.Host = *host
	}
	if *port != 0 {
		cfg.Port = *port
	}
	if *maxKeys != -1 {
		cfg.MaxKeys = *maxKeys
	}
	if *aofPath != "" {
		cfg.AOFPath = *aofPath
	}
	if *snapshotPath != "" {
		cfg.SnapshotPath = *snapshotPath
	}
	if *appendFsync != "" {
		cfg.AppendFsync = *appendFsync
	}
	if *maxClients != 0 {
		cfg.MaxClients = *maxClients
	}
	if *clientTimeoutSec != -1 {
		cfg.ClientTimeout = time.Duration(*clientTimeoutSec) * time.Second
	}
	if *replicaOf != "" {
		cfg.ReplicaOf = *replicaOf
	}
	if *replBacklogSize != 0 {
		cfg.ReplBacklogSize = *replBacklogSize
	}
	if *logLevel != "" {
		cfg.LogLevel = *logLevel
	}
	if *healthPort != -1 {
		cfg.HealthPort = *healthPort
	}
	if *pprofPort != -1 {
		cfg.PprofPort = *pprofPort
	}
	if *shutdownTimeoutSec != 0 {
		cfg.ShutdownTimeout = time.Duration(*shutdownTimeoutSec) * time.Second
	}

	// 3. Final validation
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}
