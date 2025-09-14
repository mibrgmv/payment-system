package kafka_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	kafkashared "github.com/mibrgmv/payment-service/shared/kafka"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventPublisher_ProcessSingleEvent_Success(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "balance_updated", `{
		"account_id": "test_account",
		"user_id": "test_user",
		"old_balance": 100.0,
		"new_balance": 150.0,
		"change_amount": 50.0,
		"change_type": "deposit",
		"source": "test",
		"transaction_id": "test_txn",
		"timestamp": "2023-12-07T10:00:00Z"
	}`, "balances.updated")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "balance_updated",
		Topic:     "balances.updated",
	})

	assert.NoError(t, err)
	assert.True(t, mockProducer.produced)
	assert.Equal(t, 1, mockProducer.produceCount)

	published, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, published, 0)
}

func TestEventPublisher_ProcessSingleEvent_AlreadyPublished(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")
	markEventPublished(t, pool, eventID)

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "balance_updated",
		Topic:     "balances.updated",
	})

	assert.NoError(t, err)
	assert.False(t, mockProducer.produced)
}

func TestEventPublisher_ProcessSingleEvent_InvalidPayload(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "balance_updated", `invalid json`, "balances.updated")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "balance_updated",
		Topic:     "balances.updated",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal")
}

func TestEventPublisher_ProcessOutboxBatch(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		eventID := uuid.New().String()
		setupTestOutboxEvent(t, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")
	}

	err := publisher.ProcessOutboxBatch(ctx)

	assert.NoError(t, err)
	assert.Equal(t, 5, mockProducer.produceCount)

	pending, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pending, 0)
}

func BenchmarkEventPublisher_ProcessSingleEvent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		eventID := uuid.New().String()
		setupBenchmarkOutboxEvent(b, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")

		err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
			EventID:   eventID,
			EventType: "balance_updated",
			Topic:     "balances.updated",
		})

		if err != nil {
			b.Fatalf("Failed to process event: %v", err)
		}
	}
}

func BenchmarkEventPublisher_ProcessOutboxBatch(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			eventID := uuid.New().String()
			setupBenchmarkOutboxEvent(b, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")
		}

		err := publisher.ProcessOutboxBatch(ctx)
		if err != nil {
			b.Fatalf("Failed to process batch: %v", err)
		}
	}
}

func BenchmarkEventPublisher_ProcessOutboxBatch_Concurrent(b *testing.B) {
	pool := setupBenchmarkPostgres(b)
	defer dropBenchmarkPostgres(b, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			eventID := uuid.New().String()
			setupBenchmarkOutboxEvent(b, pool, eventID, "balance_updated", `{"account_id": "test"}`, "balances.updated")

			err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
				EventID:   eventID,
				EventType: "balance_updated",
				Topic:     "balances.updated",
			})

			if err != nil {
				b.Fatalf("Failed to process event: %v", err)
			}
			i++
		}
	})
}

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

func setupTestOutboxEvent(t *testing.T, pool *pgxpool.Pool, eventID, eventType, payload, topic string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, payload, topic, status)
		values ($1, $2, $3::jsonb, $4, 'pending')
	`, eventID, eventType, payload, topic)
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

func markEventPublished(t *testing.T, pool *pgxpool.Pool, eventID string) {
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		update outbox_events set status = 'published', published_at = now()
		where event_id = $1
	`, eventID)
	require.NoError(t, err)
}
