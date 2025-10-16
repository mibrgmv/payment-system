package repository

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/mibrgmv/payment-system/services/account/internal/service/models"
)

var (
	ErrAccountNotFound = errors.New("account not found")
)

type AccountRepository interface {
	CreateAccount(ctx context.Context, userID string, currency models.Currency) (string, error)
	GetAccount(ctx context.Context, accountID string) (*models.Account, error)
	GetAccountTx(ctx context.Context, tx pgx.Tx, accountID string) (*models.Account, error)
	AccountExistsTx(ctx context.Context, tx pgx.Tx, accountID string) (bool, error)
	ListAccounts(ctx context.Context, userID string, pageSize int32, pageToken string) ([]*models.Account, string, error)
	UpdateAccount(ctx context.Context, account *models.Account) error
	DeleteAccount(ctx context.Context, accountID string) error
}
