package server

import "fmt"

type Config struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func (c *Config) GetAddr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}
