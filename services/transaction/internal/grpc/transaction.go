package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/mibrgmv/payment-service/services/transaction/internal/protogen/transaction"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service"
	"github.com/mibrgmv/payment-service/services/transaction/internal/service/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type transactionServer struct {
	transactionv1.UnimplementedTransactionServiceServer
	service service.TransactionService
}

func NewTransactionServer(service service.TransactionService) transactionv1.TransactionServiceServer {
	return &transactionServer{service: service}
}

func (s *transactionServer) CreateTransfer(ctx context.Context, req *transactionv1.CreateTransferRequest) (*transactionv1.Transaction, error) {
	if req.FromAccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "from_account_id is required")
	}
	if _, err := uuid.Parse(req.FromAccountId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "from_account_id must be a valid UUID")
	}
	if req.ToAccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "to_account_id is required")
	}
	if _, err := uuid.Parse(req.ToAccountId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "to_account_id must be a valid UUID")
	}
	if req.FromAccountId == req.ToAccountId {
		return nil, status.Error(codes.InvalidArgument, "cannot transfer to same account")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("amount must be greater than zero, got %v", req.Amount))
	}
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	transaction, err := s.service.CreateTransfer(
		ctx,
		req.FromAccountId,
		req.ToAccountId,
		req.Amount,
		currency,
		req.IdempotencyKey,
	)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidTransaction):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, service.ErrIdempotencyConflict):
			return nil, status.Error(codes.AlreadyExists, "transaction with this idempotency key already exists")
		case errors.Is(err, service.ErrInsufficientFunds):
			return nil, status.Error(codes.FailedPrecondition, "insufficient funds for transfer")
		default:
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create transfer: %v", err))
		}
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) CreateDeposit(ctx context.Context, req *transactionv1.CreateDepositRequest) (*transactionv1.Transaction, error) {
	if req.ToAccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "to_account_id is required")
	}
	if _, err := uuid.Parse(req.ToAccountId); err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("to_account_id must be a valid UUID"))
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("amount must be greater than zero, got %v", req.Amount))
	}
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	transaction, err := s.service.CreateDeposit(
		ctx,
		req.ToAccountId,
		req.Amount,
		currency,
		req.IdempotencyKey,
	)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidTransaction):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, service.ErrIdempotencyConflict):
			return nil, status.Error(codes.AlreadyExists, "transaction with this idempotency key already exists")
		default:
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create deposit: %v", err))
		}
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) CreateWithdrawal(ctx context.Context, req *transactionv1.CreateWithdrawalRequest) (*transactionv1.Transaction, error) {
	if req.FromAccountId == "" {
		return nil, status.Error(codes.InvalidArgument, "from_account_id is required")
	}
	if _, err := uuid.Parse(req.FromAccountId); err != nil {
		return nil, status.Error(codes.InvalidArgument, "from_account_id must be a valid UUID")
	}
	if req.Amount <= 0 {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("amount must be greater than zero, got %v", req.Amount))
	}
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	transaction, err := s.service.CreateWithdrawal(
		ctx,
		req.FromAccountId,
		req.Amount,
		currency,
		req.IdempotencyKey,
	)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrInvalidTransaction):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, service.ErrIdempotencyConflict):
			return nil, status.Error(codes.AlreadyExists, "transaction with this idempotency key already exists")
		case errors.Is(err, service.ErrInsufficientFunds):
			return nil, status.Error(codes.FailedPrecondition, "insufficient funds for withdrawal")
		default:
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create withdrawal: %v", err))
		}
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) GetTransaction(ctx context.Context, req *transactionv1.GetTransactionRequest) (*transactionv1.Transaction, error) {
	if err := validateTransactionId(req.TransactionId); err != nil {
		return nil, err
	}

	transaction, err := s.service.GetTransaction(ctx, req.TransactionId)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrTransactionNotFound):
			return nil, status.Error(codes.NotFound, "transaction not found")
		default:
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get transaction: %v", err))
		}
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) ListTransactions(ctx context.Context, req *transactionv1.ListTransactionsRequest) (*transactionv1.ListTransactionsResponse, error) {
	filters := models.TransactionFilters{
		AccountIDs: req.AccountIds,
	}

	if req.FromDate != nil {
		filters.FromDate = req.FromDate.AsTime()
	}

	if req.ToDate != nil {
		filters.ToDate = req.ToDate.AsTime()
	}

	if req.Type != transactionv1.TransactionType_TRANSACTION_TYPE_UNSPECIFIED {
		transactionType, err := models.TransactionTypeFromProto(req.Type)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid transaction type: %v", err))
		}
		filters.Type = &transactionType
	}

	if req.Status != transactionv1.TransactionStatus_TRANSACTION_STATUS_UNSPECIFIED {
		transactionStatus, err := models.TransactionStatusFromProto(req.Status)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid transaction status: %v", err))
		}
		filters.Status = &transactionStatus
	}

	transactions, nextPageToken, err := s.service.ListTransactions(
		ctx,
		filters,
		req.PageSize,
		req.PageToken,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to list transactions: %v", err))
	}

	protoTransactions := make([]*transactionv1.Transaction, len(transactions))
	for i, t := range transactions {
		protoTransactions[i] = t.ToProto()
	}

	return &transactionv1.ListTransactionsResponse{
		Transactions:  protoTransactions,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *transactionServer) GetTransactionStatus(ctx context.Context, req *transactionv1.GetTransactionStatusRequest) (*transactionv1.TransactionStatusResponse, error) {
	if err := validateTransactionId(req.TransactionId); err != nil {
		return nil, err
	}

	transaction, err := s.service.GetTransactionStatus(ctx, req.TransactionId)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrTransactionNotFound):
			return nil, status.Error(codes.NotFound, "transaction not found")
		default:
			return nil, status.Error(codes.Internal, "failed to get transaction status")
		}
	}

	return &transactionv1.TransactionStatusResponse{
		TransactionId: transaction.TransactionID,
		Status:        transaction.Status.ToProto(),
		LastUpdated:   timestamppb.New(transaction.UpdatedAt),
	}, nil
}

func validateTransactionId(transactionID string) error {
	if transactionID == "" {
		return status.Error(codes.InvalidArgument, "transaction_id is required")
	}
	if _, err := uuid.Parse(transactionID); err != nil {
		return status.Error(codes.InvalidArgument, "transaction_id must be a valid UUID")
	}
	return nil
}
