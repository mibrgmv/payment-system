package models

import "time"

type BalanceChangeEvent struct {
	EventID   string    `json:"event_id"`
	AccountID string    `json:"account_id"`
	Amount    int64     `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"`
}

type TransactionEvent struct {
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
