package retry

import (
	"math"
	"math/rand"
	"time"
)

type Calculator struct {
	baseDelay    time.Duration
	maxDelay     time.Duration
	jitterFactor float64
}

type CalculatorOption func(*Calculator)

func NewCalculator(baseDelay, maxDelay time.Duration, opts ...CalculatorOption) *Calculator {
	calc := &Calculator{
		baseDelay:    baseDelay,
		maxDelay:     maxDelay,
		jitterFactor: 0.2,
	}

	for _, opt := range opts {
		opt(calc)
	}

	return calc
}

func WithJitterFactor(factor float64) CalculatorOption {
	return func(c *Calculator) {
		if factor >= 0 && factor <= 1 {
			c.jitterFactor = factor
		}
	}
}

func (c *Calculator) CalculateNextRetry(attempt int) time.Time {
	if attempt < 0 {
		attempt = 0
	}

	backoff := c.calculateBackoff(attempt)
	return time.Now().Add(backoff)
}

func (c *Calculator) CalculateBackoffDuration(attempt int) time.Duration {
	return c.calculateBackoff(attempt)
}

func (c *Calculator) calculateBackoff(attempt int) time.Duration {
	if attempt == 0 {
		return 0
	}

	backoff := float64(c.baseDelay) * math.Pow(2, float64(attempt-1))

	if maxDelay := float64(c.maxDelay); backoff > maxDelay {
		backoff = maxDelay
	}

	if c.jitterFactor > 0 {
		jitterRange := backoff * c.jitterFactor
		jitter := (rand.Float64() - 0.5) * 2 * jitterRange
		backoff += jitter

		if backoff < float64(c.baseDelay) {
			backoff = float64(c.baseDelay)
		}
	}

	return time.Duration(backoff)
}
