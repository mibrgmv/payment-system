package models

import (
	"fmt"
	transactionv1 "github.com/mibrgmv/payment-system/services/transaction/internal/protogen/transaction/v1"
)

type TransactionType string

const (
	TransactionTypeTransfer   TransactionType = "transfer"
	TransactionTypeDeposit    TransactionType = "deposit"
	TransactionTypeWithdrawal TransactionType = "withdrawal"
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
	default:
		return "", fmt.Errorf("invalid transaction type: %s", s)
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
		return "", fmt.Errorf("transaction type must be specified")
	default:
		return "", fmt.Errorf("invalid proto transaction type: %v", pb)
	}
}
