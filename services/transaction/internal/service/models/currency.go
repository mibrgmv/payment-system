package models

import (
	"fmt"
	transactionv1 "github.com/mibrgmv/payment-service/services/transaction/internal/protogen/transaction"
)

type Currency string

const (
	CurrencyUnspecified Currency = "unspecified"
	CurrencyRUB         Currency = "RUB"
	CurrencyUSD         Currency = "USD"
	CurrencyEUR         Currency = "EUR"
)

func (c Currency) String() string {
	return string(c)
}

func CurrencyFromString(s string) (Currency, error) {
	switch s {
	case "RUB":
		return CurrencyRUB, nil
	case "USD":
		return CurrencyUSD, nil
	case "EUR":
		return CurrencyEUR, nil
	case "unspecified":
		return CurrencyUnspecified, nil
	default:
		return CurrencyUnspecified, fmt.Errorf("invalid currency: %s", s)
	}
}

func (c Currency) ToProto() transactionv1.Currency {
	switch c {
	case CurrencyRUB:
		return transactionv1.Currency_CURRENCY_RUB
	case CurrencyUSD:
		return transactionv1.Currency_CURRENCY_USD
	case CurrencyEUR:
		return transactionv1.Currency_CURRENCY_EUR
	default:
		return transactionv1.Currency_CURRENCY_UNSPECIFIED
	}
}

func CurrencyFromProto(pb transactionv1.Currency) (Currency, error) {
	switch pb {
	case transactionv1.Currency_CURRENCY_RUB:
		return CurrencyRUB, nil
	case transactionv1.Currency_CURRENCY_USD:
		return CurrencyUSD, nil
	case transactionv1.Currency_CURRENCY_EUR:
		return CurrencyEUR, nil
	case transactionv1.Currency_CURRENCY_UNSPECIFIED:
		return CurrencyUnspecified, nil
	default:
		return CurrencyUnspecified, fmt.Errorf("invalid proto currency: %v", pb)
	}
}
