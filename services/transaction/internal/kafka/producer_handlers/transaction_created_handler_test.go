package producer_handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/transaction/internal/kafka/producer_handlers"
	"github.com/mibrgmv/payment-service/shared/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Produce(ctx context.Context, topic, key string, value interface{}) error {
	args := m.Called(ctx, topic, key, value)
	return args.Error(0)
}

func (m *MockProducer) Close() error {
	return nil
}

func TestTransactionCreatedHandler_HandleEvent_Success(t *testing.T) {
	t.Parallel()

	mockProducer := new(MockProducer)
	handler := producer_handlers.NewTransactionCreatedHandler()

	ctx := context.Background()
	event := &outbox.Event{
		EventID: "test_event_id",
		Topic:   "transactions.created",
		RawPayload: []byte(`{
			"event_id": "test_event_id",
			"transaction_id": "txn_123",
			"type": "transfer",
			"amount": 100.50,
			"currency": "USD",
			"from_account_id": "acc_123",
			"to_account_id": "acc_456",
			"description": "Test transfer",
			"timestamp": "2023-12-07T10:00:00Z"
		}`),
	}

	expectedPayload := events.TransactionCreated{
		EventID:       "test_event_id",
		TransactionID: "txn_123",
		Type:          "transfer",
		Amount:        100.50,
		Currency:      "USD",
		FromAccountID: "acc_123",
		ToAccountID:   "acc_456",
		Description:   "Test transfer",
		Timestamp:     time.Date(2023, 12, 7, 10, 0, 0, 0, time.UTC),
	}

	mockProducer.On("Produce", ctx, "transactions.created", "test_event_id", expectedPayload).Return(nil)

	err := handler.HandleEvent(ctx, event, mockProducer)
	assert.NoError(t, err)

	mockProducer.AssertExpectations(t)
}

func TestTransactionCreatedHandler_HandleEvent_InvalidJSON(t *testing.T) {
	t.Parallel()

	mockProducer := new(MockProducer)
	handler := producer_handlers.NewTransactionCreatedHandler()

	ctx := context.Background()
	event := &outbox.Event{
		EventID:    "test_event_id",
		Topic:      "transactions.created",
		RawPayload: []byte(`{"invalid": "json"`),
	}

	err := handler.HandleEvent(ctx, event, mockProducer)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal transaction created event")

	mockProducer.AssertNotCalled(t, "Produce")
}

func TestTransactionCreatedHandler_HandleEvent_ProducerError(t *testing.T) {
	t.Parallel()

	mockProducer := new(MockProducer)
	handler := producer_handlers.NewTransactionCreatedHandler()

	ctx := context.Background()
	event := &outbox.Event{
		EventID: "test_event_id",
		Topic:   "transactions.created",
		RawPayload: []byte(`{
			"event_id": "test_event_id",
			"transaction_id": "txn_123",
			"type": "deposit",
			"amount": 50.00,
			"currency": "USD",
			"to_account_id": "acc_789",
			"timestamp": "2023-12-07T10:00:00Z"
		}`),
	}

	expectedPayload := events.TransactionCreated{
		EventID:       "test_event_id",
		TransactionID: "txn_123",
		Type:          "deposit",
		Amount:        50.00,
		Currency:      "USD",
		ToAccountID:   "acc_789",
		Timestamp:     time.Date(2023, 12, 7, 10, 0, 0, 0, time.UTC),
	}

	mockProducer.On("Produce", ctx, "transactions.created", "test_event_id", expectedPayload).Return(assert.AnError)

	err := handler.HandleEvent(ctx, event, mockProducer)
	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)

	mockProducer.AssertExpectations(t)
}

func TestTransactionCreatedHandler_GetEventType(t *testing.T) {
	t.Parallel()

	handler := producer_handlers.NewTransactionCreatedHandler()

	eventType := handler.GetEventType()
	assert.Equal(t, "transaction_created", eventType)
}
