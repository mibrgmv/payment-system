package events

import "time"

type TransactionCreated struct {
	EventID       string    `json:"event_id"`
	TransactionID string    `json:"transaction_id"`
	Type          string    `json:"type"` // "transfer", "payment", "withdrawal"
	Amount        float64   `json:"amount"`
	Currency      string    `json:"currency"`
	ToAccountID   string    `json:"to_account_id"`
	FromAccountID string    `json:"from_account_id"`
	Description   string    `json:"description"`
	Timestamp     time.Time `json:"timestamp"`
}
