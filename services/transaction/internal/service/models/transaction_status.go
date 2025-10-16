package models

import (
	"fmt"

	transactionv1 "github.com/mibrgmv/payment-system/services/transaction/internal/protogen/transaction"
)

type TransactionStatus string

const (
	TransactionStatusPending    TransactionStatus = "pending"
	TransactionStatusProcessing TransactionStatus = "processing"
	TransactionStatusCompleted  TransactionStatus = "completed"
	TransactionStatusFailed     TransactionStatus = "failed"
	TransactionStatusCancelled  TransactionStatus = "cancelled"
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
	default:
		return "", fmt.Errorf("invalid transaction status: %s", s)
	}
}

func (s TransactionStatus) ToProto() transactionv1.TransactionStatus {
	switch s {
	case TransactionStatusPending:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_PENDING
	case TransactionStatusCompleted:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_COMPLETED
	case TransactionStatusFailed:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_FAILED
	default:
		return transactionv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED
	}
}

func TransactionStatusFromProto(pb transactionv1.TransactionStatus) (TransactionStatus, error) {
	switch pb {
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_PENDING:
		return TransactionStatusPending, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_COMPLETED:
		return TransactionStatusCompleted, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_FAILED:
		return TransactionStatusFailed, nil
	case transactionv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED:
		return "", fmt.Errorf("transaction status must be specified")
	default:
		return "", fmt.Errorf("invalid proto transaction status: %v", pb)
	}
}
