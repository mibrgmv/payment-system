package config

import (
	"log"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-service/shared/env"
	"github.com/mibrgmv/payment-service/shared/loader"
	"github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/mibrgmv/payment-service/shared/server"
)

type Config struct {
	Server   server.Config   `yaml:"server"`
	Postgres postgres.Config `yaml:"postgres"`
}

func Load(config *Config) error {
	yamlPath := filepath.Join("internal", "config", "config.yaml")
	if err := loader.Load(config, yamlPath); err != nil {
		return err
	}

	envPath := filepath.Join("..", "..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		log.Printf("no .env file found: %v", err)
	}

	config.Postgres.Username = env.GetString("TRANSACTION_SERVICE_POSTGRES_USERNAME", config.Postgres.Username)
	config.Postgres.Password = env.GetString("TRANSACTION_SERVICE_POSTGRES_PASSWORD", config.Postgres.Password)
	return nil
}
