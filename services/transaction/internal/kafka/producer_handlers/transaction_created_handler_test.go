package producer_handlers_test

import (
	"context"
	"testing"

	"github.com/mibrgmv/go-platform/outbox"
	"github.com/mibrgmv/payment-system/transaction/internal/kafka/producer_handlers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Produce(ctx context.Context, topic, key string, value []byte) error {
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
	payload := []byte(`{"event_id":"test_event_id","transaction_id":"txn_123"}`)
	event := &outbox.Event{
		EventID:    "test_event_id",
		Topic:      "transactions.created",
		RawPayload: payload,
	}

	mockProducer.On("Produce", ctx, "transactions.created", "test_event_id", payload).Return(nil)

	err := handler.HandleEvent(ctx, event, mockProducer)
	assert.NoError(t, err)

	mockProducer.AssertExpectations(t)
}

func TestTransactionCreatedHandler_HandleEvent_ProducerError(t *testing.T) {
	t.Parallel()

	mockProducer := new(MockProducer)
	handler := producer_handlers.NewTransactionCreatedHandler()

	ctx := context.Background()
	payload := []byte(`{"event_id":"test_event_id"}`)
	event := &outbox.Event{
		EventID:    "test_event_id",
		Topic:      "transactions.created",
		RawPayload: payload,
	}

	mockProducer.On("Produce", ctx, "transactions.created", "test_event_id", payload).Return(assert.AnError)

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
