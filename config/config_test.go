package config_test

import (
	"os"
	"testing"

	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
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
	assert.Equal(t, cfg.CursorIndexName, "testnet1-cursor")
	assert.Equal(t, cfg.AddIntsAsStrings, true)
	expectedContracts := config.ContractsConfig{
		"contract1": {
			Name:         "contract1",
			DocTableName: "documents",
			IndexPrefix:  "index1",
			IndexName:    "index1-documents",
		},
		"contract2": {
			Name:         "contract2",
			DocTableName: "docs",
			IndexPrefix:  "index2",
			IndexName:    "index2-documents",
		},
	}
	assert.DeepEqual(t, expectedContracts, cfg.Contracts)

	expectedSingleTextSearchField := map[string]string{
		"asset": "replace",
		"int64": "include",
	}
	assert.DeepEqual(t, expectedSingleTextSearchField, cfg.SingleTextSearchField)

	assert.Equal(t, config.SingleTextSearchFieldOp_Replace, cfg.GetSingleTextSearchFieldOp(domain.ContentType_Asset))
	assert.Equal(t, config.SingleTextSearchFieldOp_Include, cfg.GetSingleTextSearchFieldOp(domain.ContentType_Int64))
	assert.Equal(t, config.SingleTextSearchFieldOp_None, cfg.GetSingleTextSearchFieldOp(domain.ContentType_Checksum256))
}

func TestValidConfigNoSingleTextSearchFieldConfig(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	cfg, err := config.LoadConfig("./config-valid-no-single-text-search-field-config.yml")
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
	assert.Equal(t, cfg.AddIntsAsStrings, false)
	expectedContracts := config.ContractsConfig{
		"contract1": {
			Name:         "contract1",
			DocTableName: "documents",
			IndexPrefix:  "index1",
			IndexName:    "index1-documents",
		},
		"contract2": {
			Name:         "contract2",
			DocTableName: "docs",
			IndexPrefix:  "index2",
			IndexName:    "index2-documents",
		},
	}
	assert.DeepEqual(t, expectedContracts, cfg.Contracts)

	assert.Equal(t, 0, len(cfg.SingleTextSearchField))
}

func TestValidConfigSingleTextSearchFieldAllNone(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	cfg, err := config.LoadConfig("./config-valid-single-text-search-field-all-none.yml")
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
	assert.Equal(t, cfg.AddIntsAsStrings, false)
	expectedContracts := config.ContractsConfig{
		"contract1": {
			Name:         "contract1",
			DocTableName: "documents",
			IndexPrefix:  "index1",
			IndexName:    "index1-documents",
		},
		"contract2": {
			Name:         "contract2",
			DocTableName: "docs",
			IndexPrefix:  "index2",
			IndexName:    "index2-documents",
		},
	}
	assert.DeepEqual(t, expectedContracts, cfg.Contracts)

	assert.Equal(t, 0, len(cfg.SingleTextSearchField))
}

func TestShouldFailForInvalidSingleTextSeachFieldValue(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	_, err := config.LoadConfig("./config-invalid-single-text-search-field-value.yml")
	assert.ErrorContains(t, err, "failed processing single-text-search-field configuration, invalid value for")
}

func TestShouldFailForInvalidSingleTextSeachFieldProperty(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	_, err := config.LoadConfig("./config-invalid-single-text-search-field-property.yml")
	assert.ErrorContains(t, err, "invalid single-text-search-field property")
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
