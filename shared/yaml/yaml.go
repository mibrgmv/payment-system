package yaml

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(config interface{}, filename string) error {
	f, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	err = yaml.Unmarshal(f, config)
	if err != nil {
		return fmt.Errorf("error parsing config file: %w", err)
	}

	return nil
}
