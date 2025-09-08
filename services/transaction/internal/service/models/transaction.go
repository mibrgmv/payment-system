package models

import (
	"fmt"
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

type TransactionType string

const (
	TransactionTypeUnspecified TransactionType = "unspecified"
	TransactionTypeTransfer    TransactionType = "transfer"
	TransactionTypeDeposit     TransactionType = "deposit"
	TransactionTypeWithdrawal  TransactionType = "withdrawal"
)

func (t TransactionType) String() string {
	return string(t)
}

func TransactionTypeFromString(s string) (TransactionType, error) {
	switch s {
	case "transfer":
		return TransactionTypeTransfer, nil
	case "deposit":
		return TransactionTypeDeposit, nil
	case "withdrawal":
		return TransactionTypeWithdrawal, nil
	case "unspecified":
		return TransactionTypeUnspecified, nil
	default:
		return TransactionTypeUnspecified, fmt.Errorf("invalid transaction type: %s", s)
	}
}

func (t TransactionType) ToProto() transactionv1.TransactionType {
	switch t {
	case TransactionTypeTransfer:
		return transactionv1.TransactionType_TRANSACTION_TYPE_TRANSFER
	case TransactionTypeDeposit:
		return transactionv1.TransactionType_TRANSACTION_TYPE_DEPOSIT
	case TransactionTypeWithdrawal:
		return transactionv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL
	default:
		return transactionv1.TransactionType_TRANSACTION_TYPE_UNSPECIFIED
	}
}

func TransactionTypeFromProto(pb transactionv1.TransactionType) (TransactionType, error) {
	switch pb {
	case transactionv1.TransactionType_TRANSACTION_TYPE_TRANSFER:
		return TransactionTypeTransfer, nil
	case transactionv1.TransactionType_TRANSACTION_TYPE_DEPOSIT:
		return TransactionTypeDeposit, nil
	case transactionv1.TransactionType_TRANSACTION_TYPE_WITHDRAWAL:
		return TransactionTypeWithdrawal, nil
	case transactionv1.TransactionType_TRANSACTION_TYPE_UNSPECIFIED:
		return TransactionTypeUnspecified, nil
	default:
		return TransactionTypeUnspecified, fmt.Errorf("invalid proto transaction type: %v", pb)
	}
}

type TransactionStatus string

const (
	TransactionStatusUnspecified TransactionStatus = "unspecified"
	TransactionStatusPending     TransactionStatus = "pending"
	TransactionStatusProcessing  TransactionStatus = "processing"
	TransactionStatusCompleted   TransactionStatus = "completed"
	TransactionStatusFailed      TransactionStatus = "failed"
	TransactionStatusCancelled   TransactionStatus = "cancelled"
)

func (s TransactionStatus) String() string {
	return string(s)
}

func TransactionStatusFromString(s string) (TransactionStatus, error) {
	switch s {
	case "pending":
		return TransactionStatusPending, nil
	case "processing":
		return TransactionStatusProcessing, nil
	case "completed":
		return TransactionStatusCompleted, nil
	case "failed":
		return TransactionStatusFailed, nil
	case "cancelled":
		return TransactionStatusCancelled, nil
	case "unspecified":
		return TransactionStatusUnspecified, nil
	default:
		return TransactionStatusUnspecified, fmt.Errorf("invalid transaction status: %s", s)
	}
}

func (s TransactionStatus) ToProto() transactionv1.TransactionStatus {
	switch s {
	case TransactionStatusPending:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_PENDING
	case TransactionStatusProcessing:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_PROCESSING
	case TransactionStatusCompleted:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_COMPLETED
	case TransactionStatusFailed:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_FAILED
	case TransactionStatusCancelled:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_CANCELLED
	default:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED
	}
}

func TransactionStatusFromProto(pb transactionv1.TransactionStatus) (TransactionStatus, error) {
	switch pb {
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_PENDING:
		return TransactionStatusPending, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_PROCESSING:
		return TransactionStatusProcessing, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_COMPLETED:
		return TransactionStatusCompleted, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_FAILED:
		return TransactionStatusFailed, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_CANCELLED:
		return TransactionStatusCancelled, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED:
		return TransactionStatusUnspecified, nil
	default:
		return TransactionStatusUnspecified, fmt.Errorf("invalid proto transaction status: %v", pb)
	}
}
