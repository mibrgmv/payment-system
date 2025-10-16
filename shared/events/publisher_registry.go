package events

import (
	"context"
	"sync"

	"github.com/mibrgmv/payment-system/shared/kafka"
	"github.com/mibrgmv/payment-system/shared/outbox"
)

type PublisherEventHandler interface {
	HandleEvent(ctx context.Context, event *outbox.Event, producer kafka.Producer) error
	GetEventType() string
}

type PublisherRegistry struct {
	handlers map[string]PublisherEventHandler
	mu       sync.RWMutex
}

func NewPublisherRegistry() *PublisherRegistry {
	return &PublisherRegistry{
		handlers: make(map[string]PublisherEventHandler),
	}
}

func (r *PublisherRegistry) Register(handler PublisherEventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[handler.GetEventType()] = handler
}

func (r *PublisherRegistry) GetHandler(eventType string) (PublisherEventHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, exists := r.handlers[eventType]
	return handler, exists
}
