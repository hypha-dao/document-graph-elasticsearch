package config_test

import (
	"os"
	"testing"

	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"gotest.tools/assert"
)

func TestConfig(t *testing.T) {
	os.Setenv("ES_USER", "elastic")
	os.Setenv("ES_PASSWORD", "password")
	config, err := config.LoadConfig("./config.yml")
	assert.NilError(t, err)

	assert.Equal(t, config.ContractName, "tlaclocmant2")
	assert.Equal(t, config.DocTableName, "documents")
	assert.Equal(t, config.FirehoseEndpoint, "fh.tekit.io:443")
	assert.Equal(t, config.EosEndpoint, "https://testnet.telos.caleos.io")
	assert.Equal(t, config.ElasticEndpoint, "https://localhost:9200")
	assert.Equal(t, config.ElasticCA, "certificates/ca/ca.crt")
	assert.Equal(t, config.PrometheusPort, 2114)
	assert.Equal(t, config.StartBlock, 149760151)
	assert.Equal(t, config.HeartBeatFrequency, 100)
	assert.Equal(t, config.ElasticUser, "elastic")
	assert.Equal(t, config.ElasticPassword, "password")
	assert.Equal(t, config.DfuseApiKey, "")

}
