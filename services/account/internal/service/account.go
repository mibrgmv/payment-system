package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/mibrgmv/payment-service/services/account/internal/repository"
	"github.com/mibrgmv/payment-service/services/account/internal/service/models"
)

var (
	ErrAccountNotFound   = repository.ErrAccountNotFound
	ErrBalanceNotFound   = repository.ErrBalanceNotFound
	ErrUserIDRequired    = errors.New("user ID is required")
	ErrAccountIDRequired = errors.New("account ID is required")
	ErrInvalidPageSize   = errors.New("page size cannot be negative")
)

type AccountService interface {
	CreateAccount(ctx context.Context, userID string, currency models.Currency) (*models.Account, error)
	GetAccount(ctx context.Context, accountID string) (*models.Account, error)
	ListAccounts(ctx context.Context, userID string, pageSize int32, pageToken string) ([]*models.Account, string, error)
	UpdateAccount(ctx context.Context, account *models.Account) (*models.Account, error)
	DeleteAccount(ctx context.Context, accountID string) error
	GetBalance(ctx context.Context, accountID string) (*models.Balance, error)
}

type accountService struct {
	accountRepo repository.AccountRepository
	balanceRepo repository.BalanceRepository
}

func NewAccountService(accountRepo repository.AccountRepository, balanceRepo repository.BalanceRepository) AccountService {
	return &accountService{
		accountRepo: accountRepo,
		balanceRepo: balanceRepo,
	}
}

func (s *accountService) CreateAccount(ctx context.Context, userID string, currency models.Currency) (*models.Account, error) {
	if userID == "" {
		return nil, ErrUserIDRequired
	}

	accountID, err := s.accountRepo.CreateAccount(ctx, userID, currency)
	if err != nil {
		return nil, fmt.Errorf("invalid currency: %w", err)
	}

	return s.GetAccount(ctx, accountID)
}

func (s *accountService) GetAccount(ctx context.Context, accountID string) (*models.Account, error) {
	if accountID == "" {
		return nil, ErrAccountIDRequired
	}

	account, err := s.accountRepo.GetAccount(ctx, accountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}

	return account, nil
}

func (s *accountService) ListAccounts(ctx context.Context, userID string, pageSize int32, pageToken string) ([]*models.Account, string, error) {
	if pageSize < 0 {
		return nil, "", ErrInvalidPageSize
	}

	accounts, nextPageToken, err := s.accountRepo.ListAccounts(ctx, userID, pageSize, pageToken)
	if err != nil {
		return nil, "", fmt.Errorf("failed to list accounts: %w", err)
	}

	return accounts, nextPageToken, nil
}

func (s *accountService) UpdateAccount(ctx context.Context, account *models.Account) (*models.Account, error) {
	if account.AccountID == "" {
		return nil, ErrAccountIDRequired
	}

	if err := s.accountRepo.UpdateAccount(ctx, account); err != nil {
		return nil, fmt.Errorf("failed to update account: %w", err)
	}

	return s.GetAccount(ctx, account.AccountID)
}

func (s *accountService) DeleteAccount(ctx context.Context, accountID string) error {
	if accountID == "" {
		return ErrAccountIDRequired
	}

	if err := s.accountRepo.DeleteAccount(ctx, accountID); err != nil {
		return fmt.Errorf("failed to delete account: %w", err)
	}

	return nil
}

func (s *accountService) GetBalance(ctx context.Context, accountID string) (*models.Balance, error) {
	if accountID == "" {
		return nil, ErrAccountIDRequired
	}

	balance, err := s.balanceRepo.GetBalance(ctx, accountID)
	if err != nil {
		if errors.Is(err, repository.ErrBalanceNotFound) {
			return nil, fmt.Errorf("balance not found for account %s: %w", accountID, ErrBalanceNotFound)
		}
		return nil, fmt.Errorf("failed to get balance: %w", err)
	}

	return balance, nil
}
