package config

import (
	"log"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-system/shared/env"
	"github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/loader"
	"github.com/mibrgmv/payment-system/shared/postgres"
	"github.com/mibrgmv/payment-system/shared/server"
)

type Config struct {
	Server   server.Config   `yaml:"server"`
	Postgres postgres.Config `yaml:"postgres"`
	Kafka    kafka.Config    `yaml:"kafka"`
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

	config.Server.Host = env.GetString("TRANSACTION_SERVICE_HOST", config.Server.Host)
	config.Server.Port = env.GetInt("TRANSACTION_SERVICE_PORT", config.Server.Port)

	config.Postgres.Host = env.GetString("TRANSACTION_SERVICE_POSTGRES_HOST", config.Postgres.Host)
	config.Postgres.Port = env.GetInt("TRANSACTION_SERVICE_POSTGRES_PORT", config.Postgres.Port)
	config.Postgres.Username = env.GetString("TRANSACTION_SERVICE_POSTGRES_USERNAME", config.Postgres.Username)
	config.Postgres.Password = env.GetString("TRANSACTION_SERVICE_POSTGRES_PASSWORD", config.Postgres.Password)
	config.Postgres.Database = env.GetString("TRANSACTION_SERVICE_POSTGRES_DATABASE", config.Postgres.Database)

	config.Kafka.Brokers = env.GetStringSlice("TRANSACTION_SERVICE_KAFKA_BROKERS", config.Kafka.Brokers)

	return nil
}
