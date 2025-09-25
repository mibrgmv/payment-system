package grpc

import (
	"context"
	"errors"
	"fmt"

	accountv1 "github.com/mibrgmv/payment-service/services/account/internal/protogen/account"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type accountServer struct {
	accountv1.UnimplementedAccountServiceServer
	service service.AccountService
}

func NewAccountServiceServer(service service.AccountService) accountv1.AccountServiceServer {
	return &accountServer{service: service}
}

func (s *accountServer) CreateAccount(ctx context.Context, req *accountv1.CreateAccountRequest) (*accountv1.Account, error) {
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}

	account, err := s.service.CreateAccount(ctx, req.UserId, currency)
	if err != nil {
		if errors.Is(err, service.ErrFieldIsRequired) || errors.Is(err, service.ErrInvalidField) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create account: %v", err))
	}

	return account.ToProto(), nil
}

func (s *accountServer) GetAccount(ctx context.Context, req *accountv1.GetAccountRequest) (*accountv1.Account, error) {
	account, err := s.service.GetAccount(ctx, req.AccountId)
	if err != nil {
		if errors.Is(err, service.ErrFieldIsRequired) || errors.Is(err, service.ErrInvalidField) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, service.ErrAccountNotFound) {
			return nil, status.Error(codes.NotFound, "account not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get account: %v", err))
	}

	return account.ToProto(), nil
}

func (s *accountServer) ListAccounts(ctx context.Context, req *accountv1.ListAccountsRequest) (*accountv1.ListAccountsResponse, error) {
	accounts, nextPageToken, err := s.service.ListAccounts(ctx, req.UserId, req.PageSize, req.PageToken)
	if err != nil {
		if errors.Is(err, service.ErrInvalidField) || errors.Is(err, service.ErrFieldIsRequired) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to list accounts: %v", err))
	}

	pbAccounts := make([]*accountv1.Account, len(accounts))
	for i, account := range accounts {
		pbAccounts[i] = account.ToProto()
	}

	return &accountv1.ListAccountsResponse{
		Accounts:      pbAccounts,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *accountServer) UpdateAccount(ctx context.Context, req *accountv1.UpdateAccountRequest) (*accountv1.Account, error) {
	currency, err := models.CurrencyFromProto(req.Account.Currency)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid currency: %v", err))
	}

	account := &models.Account{
		AccountID: req.Account.AccountId,
		Currency:  currency,
	}

	updatedAccount, err := s.service.UpdateAccount(ctx, account)
	if err != nil {
		if errors.Is(err, service.ErrFieldIsRequired) || errors.Is(err, service.ErrInvalidField) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, service.ErrAccountNotFound) {
			return nil, status.Error(codes.NotFound, "account not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to update account: %v", err))
	}

	return updatedAccount.ToProto(), nil
}

func (s *accountServer) DeleteAccount(ctx context.Context, req *accountv1.DeleteAccountRequest) (*emptypb.Empty, error) {
	if err := s.service.DeleteAccount(ctx, req.AccountId); err != nil {
		if errors.Is(err, service.ErrFieldIsRequired) || errors.Is(err, service.ErrInvalidField) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, service.ErrAccountNotFound) {
			return nil, status.Error(codes.NotFound, "account not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to delete account: %v", err))
	}
	return &emptypb.Empty{}, nil
}

func (s *accountServer) GetBalance(ctx context.Context, req *accountv1.GetBalanceRequest) (*accountv1.Balance, error) {
	balance, err := s.service.GetBalance(ctx, req.AccountId)
	if err != nil {
		if errors.Is(err, service.ErrFieldIsRequired) || errors.Is(err, service.ErrInvalidField) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, service.ErrBalanceNotFound) {
			return nil, status.Error(codes.NotFound, "balance not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get balance: %v", err))
	}
	return balance.ToProto(), nil
}
