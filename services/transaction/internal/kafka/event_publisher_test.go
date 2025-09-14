package kafka_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository/postgres"
	postgresshared "github.com/mibrgmv/payment-service/shared/postgres"
	"github.com/stretchr/testify/assert"
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

func TestEventPublisher_ProcessSingleEvent_TransactionCreated_Success(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "transaction_created", `{
		"transaction_id": "test_txn_123",
		"type": "transfer",
		"amount": 100.0,
		"currency": "USD",
		"from_account_id": "acc_from_123",
		"to_account_id": "acc_to_456",
		"description": "Test transfer",
		"timestamp": "2023-12-07T10:00:00Z"
	}`, "transactions.created")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "transaction_created",
		Topic:     "transactions.created",
	})

	assert.NoError(t, err)
	assert.True(t, mockProducer.produced)
	assert.Equal(t, 1, mockProducer.produceCount)

	published, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, published, 0)
}

func TestEventPublisher_ProcessSingleEvent_TransactionCreated_AlreadyPublished(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "transaction_created", `{"transaction_id": "test"}`, "transactions.created")
	markEventPublished(t, pool, eventID)

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "transaction_created",
		Topic:     "transactions.created",
	})

	assert.NoError(t, err)
	assert.False(t, mockProducer.produced)
}

func TestEventPublisher_ProcessSingleEvent_TransactionCreated_InvalidPayload(t *testing.T) {
	pool := setupTestPostgres(t)
	defer dropTestPostgres(t, pool)

	outboxRepo := postgres.NewOutboxRepository(pool)
	db := postgresshared.NewDB(pool)
	mockProducer := &mockKafkaProducer{}
	publisher := kafka.NewEventPublisher(outboxRepo, mockProducer, db)

	ctx := context.Background()
	eventID := uuid.New().String()

	setupTestOutboxEvent(t, pool, eventID, "transaction_created", `{
		"wrong_field": "value",
		"another_wrong_field": 123
	}`, "transactions.created")

	err := publisher.ProcessSingleEvent(ctx, events.OutboxEvent{
		EventID:   eventID,
		EventType: "transaction_created",
		Topic:     "transactions.created",
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
		setupTestOutboxEvent(t, pool, eventID, "transaction_created", `{
			"transaction_id": "test_txn_`+eventID+`",
			"type": "transfer",
			"amount": 100.0,
			"currency": "USD",
			"from_account_id": "acc_from",
			"to_account_id": "acc_to",
			"timestamp": "2023-12-07T10:00:00Z"
		}`, "transactions.created")
	}

	err := publisher.ProcessOutboxBatch(ctx)

	assert.NoError(t, err)
	assert.Equal(t, 5, mockProducer.produceCount)

	pending, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pending, 0)
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
