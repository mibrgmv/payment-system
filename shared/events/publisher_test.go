package events_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-system/shared/events"
	"github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/outbox"
	"github.com/mibrgmv/payment-system/shared/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrestest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestEventHandler struct {
	processedEvents []string
	shouldFail      bool
}

func (h *TestEventHandler) HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error {
	if h.shouldFail {
		return assert.AnError
	}

	h.processedEvents = append(h.processedEvents, event.EventID)

	var payload map[string]interface{}
	if err := json.Unmarshal(event.RawPayload, &payload); err != nil {
		return err
	}

	return producer.Produce(ctx, event.Topic, event.EventID, payload)
}

func (h *TestEventHandler) GetEventType() string {
	return "test_event"
}

type MockProducer struct {
	producedEvents []string
}

func (m *MockProducer) Produce(ctx context.Context, topic, key string, value interface{}) error {
	m.producedEvents = append(m.producedEvents, key)
	return nil
}

func (m *MockProducer) Close() error {
	return nil
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgrestest.Run(ctx,
		"postgres:15-alpine",
		postgrestest.WithDatabase("testdb"),
		postgrestest.WithUsername("testuser"),
		postgrestest.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		create table if not exists outbox_events (
			event_id varchar(255) primary key,
			event_type varchar(100) not null,
			topic varchar(255) not null,
			payload jsonb not null,
			status varchar(50) not null default 'pending',
			retry_count integer not null default 0,
			max_retries integer not null default 5,
			error_message text,
			created_at timestamptz not null default now(),
			published_at timestamptz,
			updated_at timestamptz not null default now(),
			next_retry_at timestamptz
		);
	`)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return pool, cleanup
}

func TestPublisher_ProcessSingle_Success(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgres.NewDB(pool)
	producer := &MockProducer{}

	registry := events.NewPublisherRegistry()
	handler := &TestEventHandler{}
	registry.Register(handler)

	config := events.PublisherConfig{
		BatchSize:         10,
		WorkerCount:       1,
		ProcessInterval:   time.Hour,
		RetryInterval:     time.Hour,
		CleanupInterval:   time.Hour,
		CleanupDays:       30,
		BaseRetryDelay:    time.Second,
		MaxRetryBackoff:   time.Minute,
		RetryJitterFactor: 0.1,
	}

	publisher := events.NewPublisher(registry, outboxRepo, producer, db, config)

	eventPayload := map[string]interface{}{
		"account_id": "test_account",
		"balance":    100.0,
	}

	eventID := uuid.New().String()
	outboxEvent, err := outbox.NewEvent(eventID, "test_event", "test.topic", eventPayload)
	require.NoError(t, err)

	err = db.WithTransaction(ctx, func(tx pgx.Tx) error {
		return outboxRepo.AddToOutboxTx(ctx, tx, *outboxEvent)
	})
	require.NoError(t, err)

	err = publisher.ProcessSingle(ctx, *outboxEvent)
	assert.NoError(t, err)

	assert.Contains(t, handler.processedEvents, eventID)
	assert.Contains(t, producer.producedEvents, eventID)

	publishedEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)

	found := false
	for _, e := range publishedEvents {
		if e.EventID == eventID {
			found = true
			break
		}
	}
	assert.False(t, found, "Event should not be in pending events after processing")
}

func TestPublisher_ProcessSingle_RetryOnFailure(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgres.NewDB(pool)
	producer := &MockProducer{}

	failingHandler := &TestEventHandler{shouldFail: true}
	registry := events.NewPublisherRegistry()
	registry.Register(failingHandler)

	config := events.PublisherConfig{
		BatchSize:         10,
		WorkerCount:       1,
		BaseRetryDelay:    100 * time.Millisecond,
		MaxRetryBackoff:   time.Second,
		RetryJitterFactor: 0.1,
	}

	publisher := events.NewPublisher(registry, outboxRepo, producer, db, config)

	eventID := uuid.New().String()
	outboxEvent, err := outbox.NewEvent(eventID, "test_event", "test.topic", `{"test": "data"}`)
	require.NoError(t, err)

	err = db.WithTransaction(ctx, func(tx pgx.Tx) error {
		return outboxRepo.AddToOutboxTx(ctx, tx, *outboxEvent)
	})
	require.NoError(t, err)

	err = publisher.ProcessSingle(ctx, *outboxEvent)
	assert.NoError(t, err)

	retryEvents, err := outboxRepo.GetRetryEvents(ctx, 10)
	require.NoError(t, err)

	found := false
	for _, e := range retryEvents {
		if e.EventID == eventID && e.Status == "failed" {
			found = true
			assert.NotNil(t, e.NextRetryAt)
			assert.Greater(t, e.RetryCount, 0)
			break
		}
	}
	assert.True(t, found, "Event should be in retry events after failure")
}

func TestPublisher_ProcessSingle_NoHandler(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgres.NewDB(pool)
	producer := &MockProducer{}

	registry := events.NewPublisherRegistry()

	config := events.PublisherConfig{
		BatchSize:         10,
		WorkerCount:       1,
		BaseRetryDelay:    time.Second,
		MaxRetryBackoff:   time.Minute,
		RetryJitterFactor: 0.1,
	}

	publisher := events.NewPublisher(registry, outboxRepo, producer, db, config)

	eventID := uuid.New().String()
	outboxEvent, err := outbox.NewEvent(eventID, "unknown_event_type", "test.topic", `{"test": "data"}`)
	require.NoError(t, err)

	err = db.WithTransaction(ctx, func(tx pgx.Tx) error {
		return outboxRepo.AddToOutboxTx(ctx, tx, *outboxEvent)
	})
	require.NoError(t, err)

	err = publisher.ProcessSingle(ctx, *outboxEvent)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no handler registered")

	pendingEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)

	found := false
	for _, e := range pendingEvents {
		if e.EventID == eventID && e.Status == "pending" {
			found = true
			break
		}
	}
	assert.True(t, found, "Event should remain pending when no handler exists")

	retryEvents, err := outboxRepo.GetRetryEvents(ctx, 10)
	require.NoError(t, err)

	notInRetry := true
	for _, e := range retryEvents {
		if e.EventID == eventID {
			notInRetry = false
			break
		}
	}
	assert.True(t, notInRetry, "Event should not be in retry queue when no handler exists")
}

func TestPublisher_BatchProcessing(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	outboxRepo := outbox.NewPostgresRepository(pool)
	db := postgres.NewDB(pool)
	producer := &MockProducer{}

	registry := events.NewPublisherRegistry()
	handler := &TestEventHandler{}
	registry.Register(handler)

	config := events.PublisherConfig{
		BatchSize:       5,
		WorkerCount:     2,
		BaseRetryDelay:  time.Second,
		MaxRetryBackoff: time.Minute,
	}

	publisher := events.NewPublisher(registry, outboxRepo, producer, db, config)

	eventCount := 3
	for i := 0; i < eventCount; i++ {
		eventID := uuid.New().String()
		outboxEvent, err := outbox.NewEvent(eventID, "test_event", "test.topic", map[string]interface{}{"index": i})
		require.NoError(t, err)

		err = db.WithTransaction(ctx, func(tx pgx.Tx) error {
			return outboxRepo.AddToOutboxTx(ctx, tx, *outboxEvent)
		})
		require.NoError(t, err)
	}

	err := publisher.ProcessBatch(ctx, outboxRepo.GetPendingEvents)
	assert.NoError(t, err)

	assert.Eventually(t, func() bool {
		return len(handler.processedEvents) == eventCount
	}, 2*time.Second, 100*time.Millisecond, "All events should be processed")

	assert.Eventually(t, func() bool {
		return len(producer.producedEvents) == eventCount
	}, 2*time.Second, 100*time.Millisecond, "All events should be produced")

	assert.Eventually(t, func() bool {
		pendingEvents, _ := outboxRepo.GetPendingEvents(ctx, 10)
		return len(pendingEvents) == 0
	}, 2*time.Second, 100*time.Millisecond, "No events should be pending")

	assert.Len(t, handler.processedEvents, eventCount)
	assert.Len(t, producer.producedEvents, eventCount)

	pendingEvents, err := outboxRepo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pendingEvents, 0)
}
