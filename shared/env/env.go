package env

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func GetInt(key string, defaultValue int) int {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		panic(fmt.Errorf("environment variable %s=%q cannot be converted to an int", key, value))
	}
	return intValue
}

func GetBool(key string, defaultValue bool) bool {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}

	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		panic(fmt.Errorf("environment variable %s=%q cannot be converted to a bool", key, value))
	}
	return boolValue
}

func GetDuration(key string, defaultValue time.Duration) time.Duration {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}

	durationValue, err := time.ParseDuration(value)
	if err != nil {
		panic(fmt.Errorf("environment variable %s=%q cannot be converted to a time.Duration", key, value))
	}
	return durationValue
}

func GetString(key string, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return value
}
