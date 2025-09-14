package kafka_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/transaction/internal/config"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/stretchr/testify/require"
)

type mockKafkaProducer struct {
	produced     bool
	produceCount int
}

func (m *mockKafkaProducer) Produce(ctx context.Context, topic string, key string, value interface{}) error {
	m.produced = true
	m.produceCount++
	return nil
}

func (m *mockKafkaProducer) Close() error {
	return nil
}

func setupTestPostgres(t *testing.T) *pgxpool.Pool {
	var cfg config.TestConfig
	err := config.LoadTest(&cfg)
	require.NoError(t, err)

	pool, err := pgxpool.New(context.Background(), cfg.PostgresTest.ConnectionString())
	require.NoError(t, err)

	migrationPath := "../migrations"
	err = postgresshared.MigrateUp(pool, migrationPath)
	require.NoError(t, err)

	return pool
}

func dropTestPostgres(t *testing.T, pool *pgxpool.Pool) {
	migrationPath := "../migrations"
	err := postgresshared.MigrateDown(pool, migrationPath)
	require.NoError(t, err)
}

func setupTestTransaction(t *testing.T, pool *pgxpool.Pool, transactionID, fromAccountID, toAccountID string, amount float64, status string) {
	ctx := context.Background()

	var fromAccountIDPtr, toAccountIDPtr *string
	if fromAccountID != "" {
		fromAccountIDPtr = &fromAccountID
	}
	if toAccountID != "" {
		toAccountIDPtr = &toAccountID
	}

	_, err := pool.Exec(ctx, `
       insert into transactions (
          transaction_id, type, from_account_id, to_account_id, 
          amount, currency, status, idempotency_key
       ) values ($1, $2, $3, $4, $5, 'USD', $6, $7)
    `, transactionID, "transfer", fromAccountIDPtr, toAccountIDPtr, amount, status, "test_key_"+uuid.New().String())
	require.NoError(t, err)
}

func setupTestOutboxEvent(t *testing.T, pool *pgxpool.Pool, eventID, eventType, payload, topic string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
       insert into outbox_events (event_id, event_type, payload, topic)
       values ($1, $2, $3::jsonb, $4)
    `, eventID, eventType, payload, topic)
	require.NoError(t, err)
}

func markEventPublished(t *testing.T, pool *pgxpool.Pool, eventID string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
       update outbox_events set status = 'published', published_at = now()
       where event_id = $1
    `, eventID)
	require.NoError(t, err)
}

func setupBenchmarkPostgres(b *testing.B) *pgxpool.Pool {
	var cfg config.TestConfig
	err := config.LoadTest(&cfg)
	if err != nil {
		b.Fatalf("failed to load test config: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), cfg.PostgresTest.ConnectionString())
	if err != nil {
		b.Fatalf("failed to connect to test database: %v", err)
	}

	migrationPath := "../migrations"
	err = postgresshared.MigrateUp(pool, migrationPath)
	if err != nil {
		b.Fatalf("failed to run up migrations: %v", err)
	}

	return pool
}

func dropBenchmarkPostgres(b *testing.B, pool *pgxpool.Pool) {
	migrationPath := "../migrations"
	err := postgresshared.MigrateDown(pool, migrationPath)
	if err != nil {
		b.Fatalf("failed to run down migrations: %v", err)
	}
}

func setupBenchmarkTransaction(b *testing.B, pool *pgxpool.Pool, transactionID, fromAccountID, toAccountID string, amount float64, status string) {
	ctx := context.Background()

	var fromAccountIDPtr, toAccountIDPtr *string
	if fromAccountID != "" {
		fromAccountIDPtr = &fromAccountID
	}
	if toAccountID != "" {
		toAccountIDPtr = &toAccountID
	}

	_, err := pool.Exec(ctx, `
		insert into transactions (
			transaction_id, type, from_account_id, to_account_id, 
			amount, currency, status, idempotency_key
		) values ($1, $2, $3, $4, $5, 'USD', $6, $7)
	`, transactionID, "transfer", fromAccountIDPtr, toAccountIDPtr, amount, status, "bench_key_"+uuid.New().String())
	if err != nil {
		b.Fatalf("failed to insert benchmark transaction: %v", err)
	}
}

func setupBenchmarkOutboxEvent(b *testing.B, pool *pgxpool.Pool, eventID, eventType, payload, topic string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, payload, topic)
		values ($1, $2, $3::jsonb, $4)
	`, eventID, eventType, payload, topic)
	if err != nil {
		b.Fatalf("failed to insert outbox event: %v", err)
	}
}
