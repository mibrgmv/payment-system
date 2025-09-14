package events

import (
	"encoding/json"
	"time"
)

type OutboxEvent struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	RawPayload []byte    `json:"-"`
	CreatedAt  time.Time `json:"created_at"`
	Topic      string    `json:"topic"`
}

func NewOutboxEvent(eventID, eventType, topic string, payload interface{}) (*OutboxEvent, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &OutboxEvent{
		EventID:    eventID,
		EventType:  eventType,
		Topic:      topic,
		RawPayload: payloadBytes,
		CreatedAt:  time.Now(),
	}, nil
}
