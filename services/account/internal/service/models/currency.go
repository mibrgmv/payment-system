package models

import (
	"fmt"
	accountv1 "github.com/mibrgmv/payment-system/services/account/internal/protogen/account/v1"
)

type Currency string

const (
	CurrencyRUB Currency = "RUB"
	CurrencyUSD Currency = "USD"
	CurrencyEUR Currency = "EUR"
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
	default:
		return "", fmt.Errorf("invalid currency: %s", s)
	}
}

func (c Currency) ToProto() accountv1.Currency {
	switch c {
	case CurrencyRUB:
		return accountv1.Currency_CURRENCY_RUB
	case CurrencyUSD:
		return accountv1.Currency_CURRENCY_USD
	case CurrencyEUR:
		return accountv1.Currency_CURRENCY_EUR
	default:
		return accountv1.Currency_CURRENCY_UNSPECIFIED
	}
}

func CurrencyFromProto(pb accountv1.Currency) (Currency, error) {
	switch pb {
	case accountv1.Currency_CURRENCY_RUB:
		return CurrencyRUB, nil
	case accountv1.Currency_CURRENCY_USD:
		return CurrencyUSD, nil
	case accountv1.Currency_CURRENCY_EUR:
		return CurrencyEUR, nil
	case accountv1.Currency_CURRENCY_UNSPECIFIED:
		return "", fmt.Errorf("currency must be specified")
	default:
		return "", fmt.Errorf("unknown currency value: %v", pb)
	}
}
