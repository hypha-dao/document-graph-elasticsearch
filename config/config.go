package config

import (
	"fmt"

	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"github.com/spf13/viper"
)

type SingleTextSearchFieldOp string

var (
	SingleTextSearchFieldOp_None    SingleTextSearchFieldOp = "none"
	SingleTextSearchFieldOp_Include SingleTextSearchFieldOp = "include"
	SingleTextSearchFieldOp_Replace SingleTextSearchFieldOp = "replace"
	CursorIndex                                             = "cursor"
	DocumentIndex                                           = "documents"
)

type ContractConfig struct {
	Name         string `mapstructure:"name"`
	DocTableName string `mapstructure:"doc-table-name"`
	IndexPrefix  string `mapstructure:"index-prefix"`
	IndexName    string
}

func (m *ContractConfig) Init() error {

	if err := m.Validate(); err != nil {
		return err
	}
	m.IndexName = getIndexName(m.IndexPrefix, DocumentIndex)
	return nil
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
				IndexName: %v
			}
		`,
		m.Name,
		m.DocTableName,
		m.IndexPrefix,
		m.IndexName,
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
	ContractsRaw          []*ContractConfig `mapstructure:"contracts"`
	CursorIndexPrefix     string            `mapstructure:"cursor-index-prefix"`
	FirehoseEndpoint      string            `mapstructure:"firehose-endpoint"`
	DfuseApiKey           string            `mapstructure:"dfuse-api-key"`
	EosEndpoint           string            `mapstructure:"eos-endpoint"`
	ElasticEndpoint       string            `mapstructure:"elastic-endpoint"`
	ElasticCA             string            `mapstructure:"elastic-ca"`
	PrometheusPort        uint              `mapstructure:"prometheus-port"`
	StartBlock            int64             `mapstructure:"start-block"`
	HeartBeatFrequency    uint              `mapstructure:"heart-beat-frequency"`
	Contracts             ContractsConfig   `mapstructure:"should-not-map-this"`
	SingleTextSearchField map[string]string `mapstructure:"single-text-search-field"`
	AddIntsAsStrings      bool              `mapstructure:"add-ints-as-strings"`
	ElasticUser           string
	ElasticPassword       string
	CursorIndexName       string
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
	err = config.processSingleTextSearchField()
	if err != nil {
		return nil, err
	}
	config.CursorIndexName = getIndexName(config.CursorIndexPrefix, CursorIndex)
	return &config, nil
}

func (m *Config) GetSingleTextSearchFieldOp(contentType string) SingleTextSearchFieldOp {
	op, ok := m.SingleTextSearchField[contentType]
	if !ok {
		return SingleTextSearchFieldOp_None
	}
	return SingleTextSearchFieldOp(op)
}

func (m *Config) GetCursorIndexName() string {
	return getIndexName(m.CursorIndexPrefix, CursorIndex)
}

func getIndexName(prefix, suffix string) string {
	return fmt.Sprintf(`%v-%v`, prefix, suffix)
}

func parseContracts(raw []*ContractConfig) (ContractsConfig, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("failed parsing contracts, at least one contract must be specified")
	}
	contractsConfig := make(ContractsConfig)
	for _, cc := range raw {
		if err := cc.Init(); err != nil {
			return nil, fmt.Errorf("failed parsing contracts, error: %v", err)
		}
		if _, ok := contractsConfig[cc.Name]; ok {
			return nil, fmt.Errorf("failed parsing contracts, contract: %v was specified more than once", cc.Name)
		}
		contractsConfig[cc.Name] = cc
	}
	return contractsConfig, nil
}

func (m *Config) processSingleTextSearchField() error {
	processed := make(map[string]string, len(m.SingleTextSearchField))
	for k, v := range m.SingleTextSearchField {
		if v != "none" && v != "include" && v != "replace" {
			return fmt.Errorf("failed processing single-text-search-field configuration, invalid value for %v property, valid values are: [none, include, replace] found: %v", k, v)
		}
		if v != "none" {
			if _, ok := domain.ContentTypeSuffixMap[k]; !ok {
				return fmt.Errorf("failed processing single-text-search-field configuration, invalid single-text-search-field property: %v", k)
			}
			processed[k] = v
		}
	}
	m.SingleTextSearchField = processed
	return nil
}

func (m *Config) RequiresSingleTextSearchField() bool {
	return len(m.SingleTextSearchField) > 0
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
				AddIntsAsStrings: %v
				SingleTextSearchField: %v
				CursorIndexName: %v

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
		m.AddIntsAsStrings,
		m.SingleTextSearchField,
		m.CursorIndexName,
	)
}
