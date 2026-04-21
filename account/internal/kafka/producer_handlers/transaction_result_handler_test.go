package producer_handlers_test

import (
	"context"
	"testing"

	"github.com/mibrgmv/go-platform/outbox"
	"github.com/mibrgmv/payment-system/account/internal/kafka/producer_handlers"
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

func TestTransactionResultHandler_HandleEvent_Success(t *testing.T) {
	t.Parallel()

	mockProducer := new(MockProducer)
	handler := producer_handlers.NewTransactionResultHandler()

	ctx := context.Background()
	payload := []byte(`{"transaction_id":"txn_123","status":"completed"}`)
	event := &outbox.Event{
		EventID:    "test_event_id",
		Topic:      "transactions.results",
		RawPayload: payload,
	}

	mockProducer.On("Produce", ctx, "transactions.results", "test_event_id", payload).Return(nil)

	err := handler.HandleEvent(ctx, event, mockProducer)
	assert.NoError(t, err)

	mockProducer.AssertExpectations(t)
}

func TestTransactionResultHandler_GetEventType(t *testing.T) {
	t.Parallel()

	handler := producer_handlers.NewTransactionResultHandler()

	eventType := handler.GetEventType()
	assert.Equal(t, "transaction_result", eventType)
}
