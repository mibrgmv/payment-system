package models

import (
	"encoding/json"
	"time"
)

type OutboxEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Payload   []byte    `json:"-"`
	CreatedAt time.Time `json:"created_at"`
	Topic     string    `json:"topic"`
}

func (e *OutboxEvent) UnmarshalPayload(target interface{}) error {
	return json.Unmarshal(e.Payload, target)
}
