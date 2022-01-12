package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	ContractName       string `mapstructure:"contract-name"`
	DocTableName       string `mapstructure:"doc-table-name"`
	FirehoseEndpoint   string `mapstructure:"firehose-endpoint"`
	DfuseApiKey        string `mapstructure:"dfuse-api-key"`
	EosEndpoint        string `mapstructure:"eos-endpoint"`
	ElasticEndpoint    string `mapstructure:"elastic-endpoint"`
	ElasticCA          string `mapstructure:"elastic-ca"`
	PrometheusPort     uint   `mapstructure:"prometheus-port"`
	StartBlock         int64  `mapstructure:"start-block"`
	HeartBeatFrequency uint   `mapstructure:"heart-beat-frequency"`
	ElasticUser        string
	ElasticPassword    string
}

// LoadConfig reads configuration from file or environment variables.
func LoadConfig(filePath string) (*Config, error) {
	viper.SetConfigFile(filePath)

	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		return nil, err
	}
	config.ElasticUser = viper.GetString("es_user")
	config.ElasticPassword = viper.GetString("es_password")
	return &config, nil
}

func (m *Config) String() string {
	return fmt.Sprintf(
		`
			Config {
				ContractName: %v
				DocTableName: %v
				FirehoseEndpoint: %v
				EosEndpoint: %v
				ElasticEndpoint: %v
				ElasticCA: %v
				PrometheusPort: %v
				StartBlock: %v
				HeartBeatFrequency: %v
				ElasticUser: %v
				ElasticPassword: %v
			}
		`,
		m.ContractName,
		m.DocTableName,
		m.FirehoseEndpoint,
		m.EosEndpoint,
		m.ElasticEndpoint,
		m.ElasticCA,
		m.PrometheusPort,
		m.StartBlock,
		m.HeartBeatFrequency,
		m.ElasticUser,
		m.ElasticPassword,
	)
}
