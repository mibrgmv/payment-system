package events

import "time"

type BalanceUpdated struct {
	EventID       string    `json:"event_id"`
	AccountID     string    `json:"account_id"`
	UserID        string    `json:"user_id"`
	OldBalance    float64   `json:"old_balance"`
	NewBalance    float64   `json:"new_balance"`
	ChangeAmount  float64   `json:"change_amount"`
	ChangeType    string    `json:"change_type"`
	Source        string    `json:"source"`
	TransactionID string    `json:"transaction_id,omitempty"`
	Timestamp     time.Time `json:"timestamp"`
	EventType     string    `json:"event_type"`
}
