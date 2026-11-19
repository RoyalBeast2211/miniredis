package logger

import (
	"log/slog"
	"os"
	"strings"
)

// Default Logger instance
var defaultLogger *slog.Logger

func init() {
	// Initialize with standard INFO text handler by default
	Init("info")
}

// Init configures the global structured logger using Go's standard log/slog package.
func Init(levelStr string) {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)
	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
}

// Debug logs at LevelDebug.
func Debug(msg string, args ...any) {
	defaultLogger.Debug(msg, args...)
}

// Info logs at LevelInfo.
func Info(msg string, args ...any) {
	defaultLogger.Info(msg, args...)
}

// Warn logs at LevelWarn.
func Warn(msg string, args ...any) {
	defaultLogger.Warn(msg, args...)
}

// Error logs at LevelError.
func Error(msg string, args ...any) {
	defaultLogger.Error(msg, args...)
}
