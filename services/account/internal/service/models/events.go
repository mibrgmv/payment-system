package models

import (
	"time"
)

type BalanceChangeEvent struct {
	EventID   string    `json:"event_id"`
	AccountID string    `json:"account_id"`
	Amount    int64     `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"`
}
