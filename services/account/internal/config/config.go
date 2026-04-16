package config

import (
	"log"
	"path/filepath"
	"runtime"

	"github.com/joho/godotenv"
	"github.com/mibrgmv/go-platform/kafka"
	"github.com/mibrgmv/go-platform/postgres"
	"github.com/mibrgmv/payment-system/shared/env"
	"github.com/mibrgmv/payment-system/shared/loader"
	"github.com/mibrgmv/payment-system/shared/server"
)

type Config struct {
	Server   server.Config   `yaml:"server"`
	Postgres postgres.Config `yaml:"postgres"`
	Kafka    kafka.Config    `yaml:"kafka"`
}

func Load(config *Config) error {
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)

	yamlPath := filepath.Join(basepath, "config.yaml")
	if err := loader.Load(config, yamlPath); err != nil {
		return err
	}

	envPath := filepath.Join(basepath, "..", "..", "..", "..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		log.Printf("no .env file found: %v", err)
	}

	config.Server.Host = env.GetString("ACCOUNT_SERVICE_HOST", config.Server.Host)
	config.Server.Port = env.GetInt("ACCOUNT_SERVICE_PORT", config.Server.Port)

	config.Postgres.Host = env.GetString("ACCOUNT_SERVICE_POSTGRES_HOST", config.Postgres.Host)
	config.Postgres.Port = env.GetInt("ACCOUNT_SERVICE_POSTGRES_PORT", config.Postgres.Port)
	config.Postgres.Username = env.GetString("ACCOUNT_SERVICE_POSTGRES_USERNAME", config.Postgres.Username)
	config.Postgres.Password = env.GetString("ACCOUNT_SERVICE_POSTGRES_PASSWORD", config.Postgres.Password)
	config.Postgres.Database = env.GetString("ACCOUNT_SERVICE_POSTGRES_DATABASE", config.Postgres.Database)

	config.Kafka.Brokers = env.GetStringSlice("ACCOUNT_SERVICE_KAFKA_BROKERS", config.Kafka.Brokers)

	return nil
}
