package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/postgres"
	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func (s ServerConfig) Addr() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

type Config struct {
	Server   ServerConfig    `yaml:"server"`
	Postgres postgres.Config `yaml:"postgres"`
	Kafka    kafka.Config    `yaml:"kafka"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.Server.Host = getEnv("ACCOUNT_SERVICE_HOST", cfg.Server.Host)
	cfg.Server.Port = getEnvInt("ACCOUNT_SERVICE_PORT", cfg.Server.Port)

	cfg.Postgres.Host = getEnv("ACCOUNT_SERVICE_POSTGRES_HOST", cfg.Postgres.Host)
	cfg.Postgres.Port = getEnvInt("ACCOUNT_SERVICE_POSTGRES_PORT", cfg.Postgres.Port)
	cfg.Postgres.Username = getEnv("ACCOUNT_SERVICE_POSTGRES_USERNAME", cfg.Postgres.Username)
	cfg.Postgres.Password = getEnv("ACCOUNT_SERVICE_POSTGRES_PASSWORD", cfg.Postgres.Password)
	cfg.Postgres.Database = getEnv("ACCOUNT_SERVICE_POSTGRES_DATABASE", cfg.Postgres.Database)

	cfg.Kafka.Brokers = getEnvStringSlice("KAFKA_BROKERS", cfg.Kafka.Brokers)

	return &cfg, nil
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvStringSlice(key string, fallback []string) []string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		parts := strings.Split(v, ",")
		result := make([]string, 0, len(parts))
		for _, p := range parts {
			if t := strings.TrimSpace(p); t != "" {
				result = append(result, t)
			}
		}
		return result
	}
	return fallback
}
