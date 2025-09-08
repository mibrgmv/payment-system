package models

import (
	"time"

	transactionv1 "github.com/mibrgmv/payment-service/services/transaction/internal/protogen/transaction"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Transaction struct {
	TransactionID  string
	Type           TransactionType
	FromAccountID  *string
	ToAccountID    *string
	Amount         float64
	Currency       Currency
	Status         TransactionStatus
	IdempotencyKey string
	ErrorMessage   *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

func (t *Transaction) ToProto() *transactionv1.Transaction {
	protoT := &transactionv1.Transaction{
		TransactionId:  t.TransactionID,
		Type:           t.Type.ToProto(),
		Amount:         t.Amount,
		Currency:       t.Currency.ToProto(),
		Status:         t.Status.ToProto(),
		IdempotencyKey: t.IdempotencyKey,
		CreatedAt:      timestamppb.New(t.CreatedAt),
		UpdatedAt:      timestamppb.New(t.UpdatedAt),
	}

	if t.FromAccountID != nil {
		protoT.FromAccountId = *t.FromAccountID
	}
	if t.ToAccountID != nil {
		protoT.ToAccountId = *t.ToAccountID
	}
	if t.ErrorMessage != nil {
		protoT.ErrorMessage = *t.ErrorMessage
	}
	if t.CompletedAt != nil {
		protoT.CompletedAt = timestamppb.New(*t.CompletedAt)
	}

	return protoT
}

func TransactionFromProto(pb *transactionv1.Transaction) (*Transaction, error) {
	transactionType, err := TransactionTypeFromProto(pb.Type)
	if err != nil {
		return nil, err
	}

	currency, err := CurrencyFromProto(pb.Currency)
	if err != nil {
		return nil, err
	}

	status, err := TransactionStatusFromProto(pb.Status)
	if err != nil {
		return nil, err
	}

	t := &Transaction{
		TransactionID:  pb.TransactionId,
		Type:           transactionType,
		Amount:         pb.Amount,
		Currency:       currency,
		Status:         status,
		IdempotencyKey: pb.IdempotencyKey,
		CreatedAt:      pb.CreatedAt.AsTime(),
		UpdatedAt:      pb.UpdatedAt.AsTime(),
	}

	if pb.FromAccountId != "" {
		t.FromAccountID = &pb.FromAccountId
	}
	if pb.ToAccountId != "" {
		t.ToAccountID = &pb.ToAccountId
	}
	if pb.ErrorMessage != "" {
		t.ErrorMessage = &pb.ErrorMessage
	}
	if pb.CompletedAt != nil {
		completedAt := pb.CompletedAt.AsTime()
		t.CompletedAt = &completedAt
	}

	return t, nil
}

type TransactionFilters struct {
	AccountIDs []string
	Type       *TransactionType
	Status     *TransactionStatus
	FromDate   time.Time
	ToDate     time.Time
}
