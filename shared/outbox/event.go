package outbox

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventID      string     `json:"event_id"`
	EventType    string     `json:"event_type"`
	RawPayload   []byte     `json:"-"`
	CreatedAt    time.Time  `json:"created_at"`
	Topic        string     `json:"topic"`
	Status       string     `json:"status"`
	RetryCount   int        `json:"retry_count"`
	MaxRetries   int        `json:"max_retries"`
	ErrorMessage *string    `json:"error_message"`
	NextRetryAt  *time.Time `json:"next_retry_at"`
}

type EventOptions struct {
	MaxRetries int
}

func NewEvent(eventID, eventType, topic string, payload interface{}, opts ...EventOptions) (*Event, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	event := &Event{
		EventID:    eventID,
		EventType:  eventType,
		Topic:      topic,
		RawPayload: payloadBytes,
		CreatedAt:  time.Now(),
		Status:     "pending",
		MaxRetries: 5,
	}

	if len(opts) > 0 && opts[0].MaxRetries > 0 {
		event.MaxRetries = opts[0].MaxRetries
	}

	return event, nil
}
