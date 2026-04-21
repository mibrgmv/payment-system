package events

import "time"

type TransactionResult struct {
	EventID       string    `json:"event_id"`
	TransactionID string    `json:"transaction_id"`
	Status        string    `json:"status"`
	FailureReason string    `json:"failure_reason,omitempty"`
	FailureCode   string    `json:"failure_code,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
	EventType     string    `json:"event_type"`
}
