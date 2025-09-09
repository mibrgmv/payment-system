package config

import (
	"log"
	"path/filepath"

	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-service/shared/loader"
	"github.com/mibrgmv/payment-service/shared/server"
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

	return nil
}
