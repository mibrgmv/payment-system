package models

import (
	"time"

	accountv1 "github.com/mibrgmv/payment-system/services/account/internal/protogen/account"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Account struct {
	AccountID string    `db:"account_id"`
	UserID    string    `db:"user_id"`
	Currency  Currency  `db:"currency"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (a *Account) ToProto() *accountv1.Account {
	return &accountv1.Account{
		AccountId: a.AccountID,
		UserId:    a.UserID,
		Currency:  a.Currency.ToProto(),
		CreatedAt: timestamppb.New(a.CreatedAt),
		UpdatedAt: timestamppb.New(a.UpdatedAt),
	}
}

func AccountFromProto(pb *accountv1.Account) (*Account, error) {
	currency, err := CurrencyFromProto(pb.Currency)
	if err != nil {
		return nil, err
	}

	return &Account{
		AccountID: pb.AccountId,
		UserID:    pb.UserId,
		Currency:  currency,
		CreatedAt: pb.CreatedAt.AsTime(),
		UpdatedAt: pb.UpdatedAt.AsTime(),
	}, nil
}
