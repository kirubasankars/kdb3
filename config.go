package main

import (
	"flag"
	"os"
	"time"
)

// Config holds process configuration from flags and environment.
type Config struct {
	Addr         string
	DataDir      string
	Token        string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// LoadConfig parses CLI flags and environment (KDB3_TOKEN overrides empty -token).
func LoadConfig() Config {
	cfg := Config{}
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:8001", "HTTP listen address")
	flag.StringVar(&cfg.DataDir, "data", "./data", "data directory for databases and views")
	flag.StringVar(&cfg.Token, "token", "", "bearer token required for API access (empty disables auth)")
	flag.DurationVar(&cfg.ReadTimeout, "read-timeout", 60*time.Second, "HTTP read timeout")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", 60*time.Second, "HTTP write timeout")
	flag.Parse()

	if cfg.Token == "" {
		cfg.Token = os.Getenv("KDB3_TOKEN")
	}
	return cfg
}
