package postgres

import (
	"fmt"
	"github.com/mibrgmv/payment-service/shared/types"
)

type Config struct {
	Host            string         `json:"host" yaml:"host"`
	Port            int            `json:"port" yaml:"port"`
	Database        string         `json:"database" yaml:"database"`
	Username        string         `json:"username" yaml:"username"`
	Password        string         `json:"password" yaml:"password"`
	SSLMode         string         `json:"ssl_mode" yaml:"ssl_mode"`
	MaxConns        int32          `json:"max_conns" yaml:"max_conns"`
	MinConns        int32          `json:"min_conns" yaml:"min_conns"`
	MaxConnLifetime types.Duration `json:"max_conn_lifetime" yaml:"max_conn_lifetime"`
	MaxConnIdleTime types.Duration `json:"max_conn_idle_time" yaml:"max_conn_idle_time"`
}

func (c Config) ConnectionString() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
	)
}
