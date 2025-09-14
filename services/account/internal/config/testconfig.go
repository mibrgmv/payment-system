package config

import (
	"log"
	"path/filepath"
	"runtime"

	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-service/shared/env"
	"github.com/mibrgmv/payment-service/shared/loader"
	"github.com/mibrgmv/payment-service/shared/postgres"
)

type TestConfig struct {
	PostgresTest postgres.Config `yaml:"postgres-test"`
}

func LoadTest(config *TestConfig) error {
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)

	yamlPath := filepath.Join(basepath, "test.yaml")
	if err := loader.Load(config, yamlPath); err != nil {
		return err
	}

	envPath := filepath.Join(basepath, "..", "..", "..", "..", ".env.test")
	if err := godotenv.Load(envPath); err != nil {
		log.Printf("no .env file found: %v", err)
	}

	config.PostgresTest.Username = env.GetString("ACCOUNT_SERVICE_POSTGRES_USERNAME_TEST", config.PostgresTest.Username)
	config.PostgresTest.Password = env.GetString("ACCOUNT_SERVICE_POSTGRES_PASSWORD_TEST", config.PostgresTest.Password)
	return nil
}
