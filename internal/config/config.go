package config

import (
	"os"
	"time"
)

type Config struct {
	DatabaseURL  string
	Port         string
	PollInterval time.Duration
	MaxBodyBytes int64 // Protects against memory exhaustion from large payloads.
}

func Load() Config {
	return Config{
		DatabaseURL:  getenv("DATABASE_URL", "postgres://trawler:trawler@localhost:5432/trawler"),
		Port:         getenv("PORT", "8080"),
		PollInterval: time.Second,
		MaxBodyBytes: 1 << 20,
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
