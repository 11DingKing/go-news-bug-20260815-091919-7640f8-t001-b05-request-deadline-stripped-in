// Package config loads service configuration from environment variables with
// production-safe defaults. The service listens on port 54278 by default.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config aggregates all runtime settings for the ledger service.
type Config struct {
	Port            int
	DataDir         string
	QueueSize       int
	Workers         int
	EnqueueTimeout  time.Duration
	ActiveWindow    time.Duration
	RetentionWindow time.Duration
	ArchiveInterval time.Duration
	ReapInterval    time.Duration
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

// Load reads configuration from LEDGERD_* environment variables, falling back
// to defaults that keep the service fully self-contained and offline.
func Load() Config {
	return Config{
		Port:            getInt("LEDGERD_PORT", 54278),
		DataDir:         getStr("LEDGERD_DATA_DIR", "./data"),
		QueueSize:       getInt("LEDGERD_QUEUE_SIZE", 1024),
		Workers:         getInt("LEDGERD_WORKERS", 4),
		EnqueueTimeout:  getDur("LEDGERD_ENQUEUE_TIMEOUT", 100*time.Millisecond),
		ActiveWindow:    getDur("LEDGERD_ACTIVE_WINDOW", 24*time.Hour),
		RetentionWindow: getDur("LEDGERD_RETENTION_WINDOW", 7*24*time.Hour),
		ArchiveInterval: getDur("LEDGERD_ARCHIVE_INTERVAL", time.Minute),
		ReapInterval:    getDur("LEDGERD_REAP_INTERVAL", time.Minute),
		ShutdownTimeout: getDur("LEDGERD_SHUTDOWN_TIMEOUT", 10*time.Second),
		ReadTimeout:     getDur("LEDGERD_READ_TIMEOUT", 5*time.Second),
		WriteTimeout:    getDur("LEDGERD_WRITE_TIMEOUT", 10*time.Second),
	}
}

func getStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
