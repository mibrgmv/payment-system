package kafka_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/config"
	kafkashared "github.com/mibrgmv/payment-service/shared/kafka"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/stretchr/testify/require"
)

type mockKafkaProducer struct {
	*kafkashared.Producer
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

func markEventPublished(t *testing.T, pool *pgxpool.Pool, eventID string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		update outbox_events set status = 'published', published_at = now()
		where event_id = $1
	`, eventID)
	require.NoError(t, err)
}

func setupTestOutboxEvent(t *testing.T, pool *pgxpool.Pool, eventID, eventType, payload, topic string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, payload, topic, status)
		values ($1, $2, $3::jsonb, $4, 'pending')
	`, eventID, eventType, payload, topic)
	require.NoError(t, err)
}

func setupTestAccount(t *testing.T, pool *pgxpool.Pool, accountID, userID string, initialBalance int64) {
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into accounts (account_id, user_id, currency, created_at, updated_at)
		values ($1, $2, 'USD', now(), now())
	`, accountID, userID)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		insert into balances (account_id, amount, last_updated)
		values ($1, $2, now())
	`, accountID, initialBalance)
	require.NoError(t, err)
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

func setupBenchmarkOutboxEvent(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}, pool *pgxpool.Pool, eventID, eventType, payload, topic string) {
	t.Helper()
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, payload, topic, status)
		values ($1, $2, $3::jsonb, $4, 'pending')
	`, eventID, eventType, payload, topic)
	if err != nil {
		t.Errorf("failed to insert outbox event: %v", err)
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

func setupBenchmarkPostgres(t interface {
	Helper()
	Errorf(format string, args ...interface{})
	FailNow()
}) *pgxpool.Pool {
	t.Helper()

	var cfg config.TestConfig
	err := config.LoadTest(&cfg)
	if err != nil {
		t.Errorf("Failed to load test config: %v", err)
		t.FailNow()
	}

	pool, err := pgxpool.New(context.Background(), cfg.PostgresTest.ConnectionString())
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
