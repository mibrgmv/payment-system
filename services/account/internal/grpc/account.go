package grpc

import (
	"context"
	accountv1 "github.com/mibrgmv/payment-service/services/account/internal/protogen/account"
	"github.com/mibrgmv/payment-service/services/account/internal/service"
	"google.golang.org/protobuf/types/known/emptypb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type accountServiceServer struct {
	accountv1.UnimplementedAccountServiceServer
	service *service.AccountService
}

func NewAccountServiceServer(svc *service.AccountService) accountv1.AccountServiceServer {
	return &accountServiceServer{service: svc}
}

func (s *accountServiceServer) CreateAccount(ctx context.Context, req *accountv1.CreateAccountRequest) (*accountv1.Account, error) {
	return s.service.CreateAccount(ctx, req)
}

func (s *accountServiceServer) GetAccount(ctx context.Context, req *accountv1.GetAccountRequest) (*accountv1.Account, error) {
	return s.service.GetAccount(ctx, req)
}

func (s *accountServiceServer) ListAccounts(ctx context.Context, req *accountv1.ListAccountsRequest) (*accountv1.ListAccountsResponse, error) {
	return s.service.ListAccounts(ctx, req)
}

func (s *accountServiceServer) UpdateAccount(ctx context.Context, req *accountv1.UpdateAccountRequest) (*accountv1.Account, error) {
	return s.service.UpdateAccount(ctx, req)
}

func (s *accountServiceServer) DeleteAccount(ctx context.Context, req *accountv1.DeleteAccountRequest) (*emptypb.Empty, error) {
	if err := s.service.DeleteAccount(ctx, req); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete account: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *accountServiceServer) GetBalance(ctx context.Context, req *accountv1.GetBalanceRequest) (*accountv1.Balance, error) {
	return s.service.GetBalance(ctx, req)
}
