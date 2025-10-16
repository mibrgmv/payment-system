package events_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/payment-system/shared/events"
	"github.com/mibrgmv/payment-system/shared/inbox"
	"github.com/mibrgmv/payment-system/shared/postgres"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrestest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type MockEventHandler struct {
	mock.Mock
}

func (m *MockEventHandler) HandleEvent(ctx context.Context, tx pgx.Tx, eventData []byte) error {
	args := m.Called(ctx, tx, eventData)
	return args.Error(0)
}

func (m *MockEventHandler) GetEventType() string {
	return "test_event"
}

func (m *MockEventHandler) GetEventID(eventData []byte) (string, error) {
	return "test_event_id", nil
}

type MockInboxRepo struct {
	mock.Mock
}

func (m *MockInboxRepo) IsEventProcessed(ctx context.Context, eventID string) (bool, error) {
	args := m.Called(ctx, eventID)
	return args.Bool(0), args.Error(1)
}

func (m *MockInboxRepo) MarkEventProcessedTx(ctx context.Context, tx pgx.Tx, eventID, eventType string) error {
	args := m.Called(ctx, tx, eventID, eventType)
	return args.Error(0)
}

func (m *MockInboxRepo) CleanupOldEvents(ctx context.Context, olderThanDays int) error {
	args := m.Called(ctx, olderThanDays)
	return args.Error(0)
}

func setupTestContainer(t *testing.T) (*pgxpool.Pool, func()) {
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
		create table if not exists processed_events (
			event_id varchar(255) primary key,
			event_type varchar(100) not null,
			source_service varchar(100) not null,
			processed_at timestamptz not null default now()
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

func TestProcessor_Process_Success(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	mockHandler := new(MockEventHandler)
	mockInbox := new(MockInboxRepo)

	registry := events.NewConsumerRegistry()
	registry.Register(mockHandler)

	db := postgres.NewDB(pool)
	processor := events.NewProcessor(registry, mockInbox, db, events.ProcessorConfig{})

	ctx := context.Background()
	eventData := []byte(`{"event_id": "test_event_id", "data": "test"}`)

	mockInbox.On("IsEventProcessed", ctx, "test_event_id").Return(false, nil)
	mockHandler.On("HandleEvent", ctx, mock.Anything, eventData).Return(nil)
	mockInbox.On("MarkEventProcessedTx", ctx, mock.Anything, "test_event_id", "test_event").Return(nil)

	err := processor.Process(ctx, eventData, mockHandler)
	assert.NoError(t, err)

	mockInbox.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestProcessor_Process_AlreadyProcessed(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	mockHandler := new(MockEventHandler)
	mockInbox := new(MockInboxRepo)

	registry := events.NewConsumerRegistry()
	registry.Register(mockHandler)

	db := postgres.NewDB(pool)
	processor := events.NewProcessor(registry, mockInbox, db, events.ProcessorConfig{})

	ctx := context.Background()
	eventData := []byte(`{"event_id": "test_event_id"}`)

	mockInbox.On("IsEventProcessed", ctx, "test_event_id").Return(true, nil)

	err := processor.Process(ctx, eventData, mockHandler)
	assert.NoError(t, err)

	mockHandler.AssertNotCalled(t, "HandleEvent")
	mockInbox.AssertExpectations(t)
}

func TestProcessor_Process_HandlerError(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	mockHandler := new(MockEventHandler)
	mockInbox := new(MockInboxRepo)

	registry := events.NewConsumerRegistry()
	registry.Register(mockHandler)

	db := postgres.NewDB(pool)
	processor := events.NewProcessor(registry, mockInbox, db, events.ProcessorConfig{})

	ctx := context.Background()
	eventData := []byte(`{"event_id": "test_event_id"}`)

	mockInbox.On("IsEventProcessed", ctx, "test_event_id").Return(false, nil)
	mockHandler.On("HandleEvent", ctx, mock.Anything, eventData).Return(errors.New("handler failed"))
	mockInbox.On("MarkEventProcessedTx", ctx, mock.Anything, "test_event_id", "test_event").Return(nil)

	err := processor.Process(ctx, eventData, mockHandler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler failed")

	mockInbox.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mockInbox.AssertNotCalled(t, "MarkEventProcessedTx")
}

func TestProcessor_Process_ConcurrentProcessing(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupTestContainer(t)
	defer cleanup()

	mockHandler := new(MockEventHandler)
	mockInbox := new(MockInboxRepo)

	registry := events.NewConsumerRegistry()
	registry.Register(mockHandler)

	db := postgres.NewDB(pool)
	processor := events.NewProcessor(registry, mockInbox, db, events.ProcessorConfig{})

	ctx := context.Background()
	eventData := []byte(`{"event_id": "test_event_id"}`)

	mockInbox.On("IsEventProcessed", ctx, "test_event_id").Return(false, nil)
	mockInbox.On("MarkEventProcessedTx", ctx, mock.Anything, "test_event_id", "test_event").
		Return(inbox.ErrEventAlreadyProcessed)

	err := processor.Process(ctx, eventData, mockHandler)
	assert.NoError(t, err)

	mockInbox.AssertExpectations(t)
	mockHandler.AssertNotCalled(t, "HandleEvent")
}
