package events

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
)

type ProcessorEventHandler interface {
	HandleEvent(ctx context.Context, tx pgx.Tx, eventData []byte) error
	GetEventType() string
	GetEventID(eventData []byte) (string, error)
}

type ProcessorRegistry struct {
	handlers map[string]ProcessorEventHandler
	mu       sync.RWMutex
}

func NewConsumerRegistry() *ProcessorRegistry {
	return &ProcessorRegistry{
		handlers: make(map[string]ProcessorEventHandler),
	}
}

func (r *ProcessorRegistry) Register(handler ProcessorEventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[handler.GetEventType()] = handler
}

func (r *ProcessorRegistry) GetHandler(eventType string) (ProcessorEventHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, exists := r.handlers[eventType]
	return handler, exists
}

func (r *ProcessorRegistry) GetAllEventTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.handlers))
	for eventType := range r.handlers {
		types = append(types, eventType)
	}
	return types
}
