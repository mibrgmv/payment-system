package models

import (
	"time"

	accountv1 "github.com/mibrgmv/payment-system/account/internal/protogen/account/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Balance struct {
	AccountID   string    `db:"account_id"`
	Amount      float64   `db:"amount"`
	Currency    Currency  `db:"currency"`
	LastUpdated time.Time `db:"last_updated"`
}

func (b *Balance) ToProto() *accountv1.Balance {
	return &accountv1.Balance{
		AccountId:   b.AccountID,
		Amount:      b.Amount,
		Currency:    b.Currency.ToProto(),
		LastUpdated: timestamppb.New(b.LastUpdated),
	}
}

func BalanceFromProto(pb *accountv1.Balance) (*Balance, error) {
	currency, err := CurrencyFromProto(pb.Currency)
	if err != nil {
		return nil, err
	}

	return &Balance{
		AccountID:   pb.AccountId,
		Amount:      pb.Amount,
		Currency:    currency,
		LastUpdated: pb.LastUpdated.AsTime(),
	}, nil
}
