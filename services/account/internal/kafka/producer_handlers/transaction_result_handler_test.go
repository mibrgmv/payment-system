package producer_handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/mibrgmv/payment-service/services/account/internal/kafka/events"
	"github.com/mibrgmv/payment-service/services/account/internal/kafka/producer_handlers"
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

func TestTransactionResultHandler_HandleEvent_Success(t *testing.T) {
	t.Parallel()

	mockProducer := new(MockProducer)
	handler := producer_handlers.NewTransactionResultHandler()

	ctx := context.Background()
	event := &outbox.Event{
		EventID: "test_event_id",
		Topic:   "transactions.results",
		RawPayload: []byte(`{
			"transaction_id": "txn_123",
			"status": "completed",
			"timestamp": "2023-12-07T10:00:00Z"
		}`),
	}

	expectedPayload := events.TransactionResult{
		TransactionID: "txn_123",
		Status:        "completed",
		Timestamp:     time.Date(2023, 12, 7, 10, 0, 0, 0, time.UTC),
	}

	mockProducer.On("Produce", ctx, "transactions.results", "test_event_id", expectedPayload).Return(nil)

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
