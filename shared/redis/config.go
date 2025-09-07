package redis

import "time"

type Config struct {
	Addr     string        `yaml:"addr" env:"REDIS_ADDR"`
	Password string        `yaml:"password" env:"REDIS_PASSWORD"`
	DB       int           `yaml:"db" env:"REDIS_DB"`
	PoolSize int           `yaml:"pool_size" env:"REDIS_POOL_SIZE"`
	Timeout  time.Duration `yaml:"timeout" env:"REDIS_TIMEOUT"`
}

func DefaultConfig() Config {
	return Config{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		PoolSize: 20,
		Timeout:  5 * time.Second,
	}
}
