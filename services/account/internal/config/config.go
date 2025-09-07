package config

import (
	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-service/shared/env"
	"github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/mibrgmv/payment-service/shared/yaml"
	"log"
	"path/filepath"
)

type Config struct {
	Postgres postgres.Config `yaml:"postgres"`
}

func Load(config *Config) error {
	yamlPath := filepath.Join("internal", "config", "config.yaml")
	if err := yaml.Load(config, yamlPath); err != nil {
		return err
	}

	envPath := filepath.Join("..", "..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		log.Printf("no .env file found: %v", err)
	}

	config.Postgres.Username = env.GetString("ACCOUNT_SERVICE_POSTGRES_USERNAME", config.Postgres.Username)
	config.Postgres.Password = env.GetString("ACCOUNT_SERVICE_POSTGRES_PASSWORD", config.Postgres.Password)
	return nil
}
