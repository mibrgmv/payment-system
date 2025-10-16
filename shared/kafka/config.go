package kafka

type Config struct {
	Brokers []string `yaml:"brokers"`
	GroupID string   `yaml:"group_id"`
	Topics  Topics   `yaml:"topics"`
}

type Topics struct {
	Transactions string `yaml:"transactions"`
	Results      string `yaml:"results"`
	Balances     string `yaml:"balances"`
	Created      string `yaml:"created"`
}
