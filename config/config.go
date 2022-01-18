package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type ContractConfig struct {
	Name         string `mapstructure:"name"`
	DocTableName string `mapstructure:"doc-table-name"`
	IndexPrefix  string `mapstructure:"index-prefix"`
}

func (m *ContractConfig) Validate() error {

	if m.Name == "" {
		return fmt.Errorf("contracts name property is required")
	}

	if m.DocTableName == "" {
		return fmt.Errorf("contracts doc-table-name property is required")
	}

	if m.IndexPrefix == "" {
		return fmt.Errorf("contracts index-prefix property is required")
	}
	return nil
}

func (m *ContractConfig) String() string {
	return fmt.Sprintf(
		`
			ContractConfig {
				Name: %v
				DocTableName: %v
				IndexPrefix: %v
			}
		`,
		m.Name,
		m.DocTableName,
		m.IndexPrefix,
	)
}

type ContractsConfig map[string]*ContractConfig

func (m ContractsConfig) Get(contract string, table string) *ContractConfig {
	if config, ok := m[contract]; ok {
		if config.DocTableName == table {
			return config
		}
	}
	return nil
}

type Config struct {
	ContractsRaw       []*ContractConfig `mapstructure:"contracts"`
	CursorIndexPrefix  string            `mapstructure:"cursor-index-prefix"`
	FirehoseEndpoint   string            `mapstructure:"firehose-endpoint"`
	DfuseApiKey        string            `mapstructure:"dfuse-api-key"`
	EosEndpoint        string            `mapstructure:"eos-endpoint"`
	ElasticEndpoint    string            `mapstructure:"elastic-endpoint"`
	ElasticCA          string            `mapstructure:"elastic-ca"`
	PrometheusPort     uint              `mapstructure:"prometheus-port"`
	StartBlock         int64             `mapstructure:"start-block"`
	HeartBeatFrequency uint              `mapstructure:"heart-beat-frequency"`
	Contracts          ContractsConfig   `mapstructure:"should-not-map-this"`
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

	config.Contracts, err = parseContracts(config.ContractsRaw)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

func parseContracts(raw []*ContractConfig) (ContractsConfig, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("failed parsing contracts, at least one contract must be specified")
	}
	contractsConfig := make(ContractsConfig)
	for _, cc := range raw {
		if err := cc.Validate(); err != nil {
			return nil, fmt.Errorf("failed parsing contracts, error: %v", err)
		}
		if _, ok := contractsConfig[cc.Name]; ok {
			return nil, fmt.Errorf("failed parsing contracts, contract: %v was specified more than once", cc.Name)
		}
		contractsConfig[cc.Name] = cc
	}
	return contractsConfig, nil
}

func (m *Config) String() string {
	return fmt.Sprintf(
		`
			Config {
				ContractsRaw: %v
				Contracts: %v
				CursorIndexPrefix: %v
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
		m.ContractsRaw,
		m.Contracts,
		m.CursorIndexPrefix,
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
