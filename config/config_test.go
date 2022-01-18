package config_test

import (
	"os"
	"testing"

	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"gotest.tools/assert"
)

func TestValidConfig(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	cfg, err := config.LoadConfig("./config-valid.yml")
	assert.NilError(t, err)

	assert.Equal(t, cfg.FirehoseEndpoint, "fh.tekit.io:443")
	assert.Equal(t, cfg.EosEndpoint, "https://testnet.telos.caleos.io")
	assert.Equal(t, cfg.ElasticEndpoint, "https://localhost:9200")
	assert.Equal(t, cfg.ElasticCA, "certificates/ca/ca.crt")
	assert.Equal(t, cfg.PrometheusPort, uint(2114))
	assert.Equal(t, cfg.StartBlock, int64(149760151))
	assert.Equal(t, cfg.HeartBeatFrequency, uint(100))
	assert.Equal(t, cfg.ElasticUser, "elastic")
	assert.Equal(t, cfg.ElasticPassword, "password")
	assert.Equal(t, cfg.DfuseApiKey, "")
	assert.Equal(t, cfg.CursorIndexPrefix, "testnet1")
	expectedContracts := config.ContractsConfig{
		"contract1": {
			Name:         "contract1",
			DocTableName: "documents",
			IndexPrefix:  "index1",
		},
		"contract2": {
			Name:         "contract2",
			DocTableName: "docs",
			IndexPrefix:  "index2",
		},
	}
	assert.DeepEqual(t, expectedContracts, cfg.Contracts)

}

func TestShouldFailForNoContracts(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	_, err := config.LoadConfig("./config-no-contracts.yml")
	assert.ErrorContains(t, err, "least one contract must be specified")
}

func TestShouldFailForEmptyContractProp(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	_, err := config.LoadConfig("./config-empty-contract-props.yml")
	assert.ErrorContains(t, err, "index-prefix property is required")
}

func TestShouldFailForDuplicateContract(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	_, err := config.LoadConfig("./config-duplicate-contract.yml")
	assert.ErrorContains(t, err, "was specified more than once")
}
