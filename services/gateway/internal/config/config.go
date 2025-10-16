package config

import (
	"log"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-system/shared/env"
	"github.com/mibrgmv/payment-system/shared/loader"
	"github.com/mibrgmv/payment-system/shared/server"
)

type Config struct {
	Server   server.Config `yaml:"server"`
	Services struct {
		Account     server.Config `yaml:"account"`
		Transaction server.Config `yaml:"transaction"`
	}
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

	config.Server.Host = env.GetString("GATEWAY_SERVICE_HOST", config.Server.Host)
	config.Server.Port = env.GetInt("GATEWAY_SERVICE_PORT", config.Server.Port)

	config.Services.Account.Host = env.GetString("ACCOUNT_SERVICE_HOST", config.Services.Account.Host)
	config.Services.Account.Port = env.GetInt("ACCOUNT_SERVICE_PORT", config.Services.Account.Port)

	config.Services.Transaction.Host = env.GetString("TRANSACTION_SERVICE_HOST", config.Services.Transaction.Host)
	config.Services.Transaction.Port = env.GetInt("TRANSACTION_SERVICE_PORT", config.Services.Transaction.Port)

	return nil
}
