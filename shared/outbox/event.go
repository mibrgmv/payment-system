package outbox

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventID    string    `json:"event_id"`
	EventType  string    `json:"event_type"`
	RawPayload []byte    `json:"-"`
	CreatedAt  time.Time `json:"created_at"`
	Topic      string    `json:"topic"`
}

func NewEvent(eventID, eventType, topic string, payload interface{}) (*Event, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Event{
		EventID:    eventID,
		EventType:  eventType,
		Topic:      topic,
		RawPayload: payloadBytes,
		CreatedAt:  time.Now(),
	}, nil
}
