package grpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/mibrgmv/payment-service/services/transaction/internal/repository"

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
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
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
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create transfer: %v", err))
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) CreateDeposit(ctx context.Context, req *transactionv1.CreateDepositRequest) (*transactionv1.Transaction, error) {
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}

	transaction, err := s.service.CreateDeposit(
		ctx,
		req.ToAccountId,
		req.Amount,
		currency,
		req.IdempotencyKey,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create deposit: %v", err))
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) CreateWithdrawal(ctx context.Context, req *transactionv1.CreateWithdrawalRequest) (*transactionv1.Transaction, error) {
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}

	transaction, err := s.service.CreateWithdrawal(
		ctx,
		req.FromAccountId,
		req.Amount,
		currency,
		req.IdempotencyKey,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create withdrawal: %v", err))
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) GetTransaction(ctx context.Context, req *transactionv1.GetTransactionRequest) (*transactionv1.Transaction, error) {
	transaction, err := s.service.GetTransaction(ctx, req.TransactionId)
	if err != nil {
		if errors.Is(err, repository.ErrTransactionNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get transaction: %v", err))
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) ListTransactions(ctx context.Context, req *transactionv1.ListTransactionsRequest) (*transactionv1.ListTransactionsResponse, error) {
	filters := models.TransactionFilters{
		AccountIDs: req.AccountIds,
		FromDate:   req.FromDate.AsTime(),
		ToDate:     req.ToDate.AsTime(),
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

func (s *transactionServer) CancelTransaction(ctx context.Context, req *transactionv1.CancelTransactionRequest) (*transactionv1.Transaction, error) {
	transaction, err := s.service.CancelTransaction(ctx, req.TransactionId)
	if err != nil {
		if errors.Is(err, repository.ErrTransactionNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to cancel transaction: %v", err))
	}

	return transaction.ToProto(), nil
}

func (s *transactionServer) GetTransactionStatus(ctx context.Context, req *transactionv1.GetTransactionStatusRequest) (*transactionv1.TransactionStatusResponse, error) {
	transaction, err := s.service.GetTransactionStatus(ctx, req.TransactionId)
	if err != nil {
		if errors.Is(err, repository.ErrTransactionNotFound) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get transaction status: %v", err))
	}

	return &transactionv1.TransactionStatusResponse{
		TransactionId: transaction.TransactionID,
		Status:        transaction.Status.ToProto(),
		LastUpdated:   timestamppb.New(transaction.UpdatedAt),
	}, nil
}
