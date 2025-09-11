package service_test

import (
	"context"
	"encoding/json"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
	"github.com/mibrgmv/payment-service/shared/env"
	"github.com/mibrgmv/payment-service/shared/loader"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
)

func BenchmarkBalanceProcessor_ProcessBalanceChangeEvent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	balanceRepo := postgres.NewBalanceRepository(pool)
	eventTrackingRepo := postgres.NewEventTrackingRepository(pool)
	processor := service.NewBalanceProcessor(balanceRepo, eventTrackingRepo, pool)

	ctx := context.Background()
	accountID := uuid.New().String()
	userID := uuid.New().String()
	setupBenchmarkAccount(b, pool, accountID, userID, 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := models.BalanceChangeEvent{
			EventID:   "bench_event_" + uuid.New().String(),
			AccountID: accountID,
			Amount:    int64(i % 1000), // Vary amounts
			Timestamp: time.Now(),
			Source:    "transaction_service",
		}

		eventBytes, _ := json.Marshal(event)
		processor.ProcessBalanceChangeEvent(ctx, eventBytes)
	}
}

func setupBenchmarkPostgres(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}) *pgxpool.Pool {
	t.Helper()

	var config testConfig
	err := LoadTest(&config)
	if err != nil {
		t.Errorf("Failed to load test config: %v", err)
		t.FailNow()
	}

	pool, err := pgxpool.New(context.Background(), config.Postgres.ConnectionString())
	if err != nil {
		t.Errorf("Failed to connect to test database: %v", err)
		t.FailNow()
	}

	migrationPath := "../migrations"
	err = postgresshared.MigrateUp(pool, migrationPath)
	if err != nil {
		t.Errorf("Failed to run up migrations: %v", err)
		t.FailNow()
	}

	return pool
}

func dropBenchmarkPostgres(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}, pool *pgxpool.Pool) {
	t.Helper()

	migrationPath := "../migrations"
	err := postgresshared.MigrateDown(pool, migrationPath)
	if err != nil {
		t.Errorf("Failed to run down migrations: %v", err)
		t.FailNow()
	}
}

func setupBenchmarkAccount(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}, pool *pgxpool.Pool, accountID, userID string, initialBalance int64) {
	t.Helper()

	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into accounts (account_id, user_id, currency, created_at, updated_at)
    	values ($1, $2, 'USD', now(), now())
  	`, accountID, userID)
	if err != nil {
		t.Errorf("Failed to insert test account: %v", err)
		t.FailNow()
	}

	_, err = pool.Exec(ctx, `
        insert into balances (account_id, amount, last_updated)
        values ($1, $2, now())
  	`, accountID, initialBalance)
	if err != nil {
		t.Errorf("Failed to insert test balance: %v", err)
		t.FailNow()
	}
}

type testConfig struct {
	Postgres postgresshared.Config `yaml:"postgres-test"`
}

func LoadTest(config *testConfig) error {
	yamlPath := filepath.Join("..", "config", "test.yaml")
	if err := loader.Load(config, yamlPath); err != nil {
		return err
	}

	envPath := filepath.Join("..", "..", "..", "..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		log.Printf("no .env file found: %v", err)
	}

	config.Postgres.Username = env.GetString("ACCOUNT_SERVICE_POSTGRES_USERNAME_TEST", config.Postgres.Username)
	config.Postgres.Password = env.GetString("ACCOUNT_SERVICE_POSTGRES_PASSWORD_TEST", config.Postgres.Password)
	return nil
}
