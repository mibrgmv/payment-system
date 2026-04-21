package config

import (
	"fmt"
	"os"
	"strconv"

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
	Server   ServerConfig `yaml:"server"`
	Services struct {
		Account     ServerConfig `yaml:"account"`
		Transaction ServerConfig `yaml:"transaction"`
	} `yaml:"services"`
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

	cfg.Server.Host = getEnv("GATEWAY_SERVICE_HOST", cfg.Server.Host)
	cfg.Server.Port = getEnvInt("GATEWAY_SERVICE_PORT", cfg.Server.Port)

	cfg.Services.Account.Host = getEnv("ACCOUNT_SERVICE_HOST", cfg.Services.Account.Host)
	cfg.Services.Account.Port = getEnvInt("ACCOUNT_SERVICE_PORT", cfg.Services.Account.Port)

	cfg.Services.Transaction.Host = getEnv("TRANSACTION_SERVICE_HOST", cfg.Services.Transaction.Host)
	cfg.Services.Transaction.Port = getEnvInt("TRANSACTION_SERVICE_PORT", cfg.Services.Transaction.Port)

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
