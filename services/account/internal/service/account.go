package service

import (
	"context"
	"fmt"

	accountv1 "github.com/mibrgmv/payment-service/services/account/internal/protogen/account"
	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

type AccountService struct {
	accountRepo repository.AccountRepository
	balanceRepo repository.BalanceRepository
}

func NewAccountService(accountRepo repository.AccountRepository, balanceRepo repository.BalanceRepository) *AccountService {
	return &AccountService{
		accountRepo: accountRepo,
		balanceRepo: balanceRepo,
	}
}

func (s *AccountService) CreateAccount(ctx context.Context, req *accountv1.CreateAccountRequest) (*accountv1.Account, error) {
	currency, err := models.CurrencyFromProto(req.Currency)
	if err != nil {
		return nil, fmt.Errorf("invalid currency: %w", err)
	}

	accountID, err := s.accountRepo.CreateAccount(ctx, req.UserId, currency)
	if err != nil {
		return nil, fmt.Errorf("failed to create account: %w", err)
	}

	return s.GetAccount(ctx, &accountv1.GetAccountRequest{AccountId: accountID})
}

func (s *AccountService) GetAccount(ctx context.Context, req *accountv1.GetAccountRequest) (*accountv1.Account, error) {
	account, err := s.accountRepo.GetAccount(ctx, req.AccountId)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	return account.ToProto(), nil
}

func (s *AccountService) ListAccounts(ctx context.Context, req *accountv1.ListAccountsRequest) (*accountv1.ListAccountsResponse, error) {
	limit := int(req.PageSize)
	if limit == 0 {
		limit = 50
	}
	offset := 0

	accounts, err := s.accountRepo.ListAccounts(ctx, req.UserId, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list accounts: %w", err)
	}

	pbAccounts := make([]*accountv1.Account, len(accounts))
	for i, account := range accounts {
		pbAccounts[i] = account.ToProto()
	}

	return &accountv1.ListAccountsResponse{
		Accounts: pbAccounts,
	}, nil
}

func (s *AccountService) UpdateAccount(ctx context.Context, req *accountv1.UpdateAccountRequest) (*accountv1.Account, error) {
	currency, err := models.CurrencyFromProto(req.Account.Currency)
	if err != nil {
		return nil, fmt.Errorf("invalid currency: %w", err)
	}

	account := &models.Account{
		AccountID: req.Account.AccountId,
		Currency:  currency,
	}

	if err := s.accountRepo.UpdateAccount(ctx, account); err != nil {
		return nil, fmt.Errorf("failed to update account: %w", err)
	}

	return s.GetAccount(ctx, &accountv1.GetAccountRequest{AccountId: req.Account.AccountId})
}

func (s *AccountService) DeleteAccount(ctx context.Context, req *accountv1.DeleteAccountRequest) error {
	if err := s.accountRepo.DeleteAccount(ctx, req.AccountId); err != nil {
		return fmt.Errorf("failed to delete account: %w", err)
	}
	return nil
}

func (s *AccountService) GetBalance(ctx context.Context, req *accountv1.GetBalanceRequest) (*accountv1.Balance, error) {
	balance, err := s.balanceRepo.GetBalance(ctx, req.AccountId)
	if err != nil {
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}

	return balance.ToProto(), nil
}
