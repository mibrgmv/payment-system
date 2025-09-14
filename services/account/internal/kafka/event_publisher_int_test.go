package kafka_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/repository/postgres"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventPublisher_ProcessSingleEvent_BalanceUpdated_Success(t *testing.T) {
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

func TestEventPublisher_ProcessSingleEvent_BalanceUpdated_AlreadyPublished(t *testing.T) {
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

func TestEventPublisher_ProcessSingleEvent_BalanceUpdated_InvalidPayload(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "balance_updated", `{
		"wrong_field": "value",
		"another_wrong_field": 123,
		"this_wont_match": true
	}`, "balances.updated")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "balance_updated",
		Topic:     "balances.updated",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal")
}

func TestEventPublisher_ProcessSingleEvent_TransactionResult_Success(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "transaction_result", `{
		"transaction_id": "test_txn_123",
		"status": "completed",
		"timestamp": "2023-12-07T10:00:00Z"
	}`, "transactions.results")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "transaction_result",
		Topic:     "transactions.results",
	})

	assert.NoError(t, err)
	assert.True(t, mockProducer.produced)
}

func TestEventPublisher_ProcessSingleEvent_TransactionResult_InvalidPayload(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "transaction_result", `{
		"wrong_field": "value"
	}`, "transactions.results")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "transaction_result",
		Topic:     "transactions.results",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal")
}

func TestEventPublisher_ProcessSingleEvent_UnknownType(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "unknown_type", `{"data": "test"}`, "unknown.topic")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "unknown_type",
		Topic:     "unknown.topic",
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown event type")
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
