package config

import (
	"fmt"
	"os"
)

type Config struct {
	HTTPPort    string
	PostgresURL string
	NATSURL     string
	RedisAddr   string
}

func Load() (Config, error) {
	cfg := Config{
		HTTPPort:    os.Getenv("HTTP_PORT"),
		PostgresURL: os.Getenv("POSTGRES_URL"),
		NATSURL:     os.Getenv("NATS_URL"),
		RedisAddr:   os.Getenv("REDIS_ADDR"),
	}

	if cfg.HTTPPort == "" {
		return Config{}, fmt.Errorf("HTTP_PORT is required")
	}

	if cfg.PostgresURL == "" {
		return Config{}, fmt.Errorf("POSTGRES_URL is required")
	}

	if cfg.NATSURL == "" {
		return Config{}, fmt.Errorf("NATS_URL is required")
	}

	if cfg.RedisAddr == "" {
		return Config{}, fmt.Errorf("REDIS_ADDR is required")
	}

	return cfg, nil
}
