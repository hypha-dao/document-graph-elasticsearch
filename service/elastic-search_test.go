package service_test

import (
	"log"
	"os"
	"testing"

	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"gotest.tools/assert"
)

var elasticSearch *service.ElasticSearch

func TestMain(m *testing.M) {
	beforeAll()
	// exec test and this returns an exit code to pass to os
	retCode := m.Run()
	afterAll()
	// If exit code is distinct of zero,
	// the test will be failed (red)
	os.Exit(retCode)
}

func beforeAll() {
	var err error
	config := &config.Config{
		ElasticEndpoint: "https://localhost:9200",
		ElasticCA:       "/home/sebastian/vsc-workspace/elastic-helm-charts/elasticsearch/examples/security/elastic-certificate.pem",
		ElasticUser:     "elastic",
		ElasticPassword: "8GXQlCxXy0p8bSilFMqI",
	}
	elasticSearch, err = service.NewElasticSearch(config)
	if err != nil {
		log.Fatal(err, "Failed creating elasticSearch client")
	}
}

func afterAll() {
}

func TestOpCycle(t *testing.T) {

	index := "prueba"
	doc1Id := "1"
	doc2Id := "2"
	doc1 := map[string]interface{}{
		"str":    "This is a string",
		"number": float64(30),
	}
	doc2 := map[string]interface{}{
		"id":     doc2Id,
		"str":    "This is a string for doc 2",
		"number": float64(30.45),
	}

	exists, err := elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	if exists {
		_, err := elasticSearch.DeleteIndex(index)
		assert.NilError(t, err)
	}

	exists, err = elasticSearch.DocumentExists(index, doc1Id)
	assert.NilError(t, err)
	assert.Assert(t, !exists)

	_, err = elasticSearch.Upsert(index, doc1Id, doc1)
	assert.NilError(t, err)

	exists, err = elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	exists, err = elasticSearch.DocumentExists(index, doc1Id)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	res, err := elasticSearch.Get(index, doc1Id)
	assert.NilError(t, err)
	assert.DeepEqual(t, doc1, res)

	_, err = elasticSearch.Upsert(index, doc2Id, doc2)
	assert.NilError(t, err)

	exists, err = elasticSearch.DocumentExists(index, doc2Id)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	res, err = elasticSearch.Get(index, doc2Id)
	assert.NilError(t, err)
	assert.DeepEqual(t, doc2, res)

	_, err = elasticSearch.DeleteDocument(index, doc2Id, true)
	assert.NilError(t, err)

	exists, err = elasticSearch.DocumentExists(index, doc2Id)
	assert.NilError(t, err)
	assert.Assert(t, !exists)

	_, err = elasticSearch.DeleteDocument(index, doc1Id, true)
	assert.NilError(t, err)

	exists, err = elasticSearch.DocumentExists(index, doc1Id)
	assert.NilError(t, err)
	assert.Assert(t, !exists)
}

func TestDeleteDocument(t *testing.T) {

	index := "prueba"
	doc1Id := "1"
	doc1 := map[string]interface{}{
		"str":    "This is a string",
		"number": float64(30),
	}

	exists, err := elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	if exists {
		_, err := elasticSearch.DeleteIndex(index)
		assert.NilError(t, err)
	}

	exists, err = elasticSearch.DocumentExists(index, doc1Id)
	assert.NilError(t, err)
	assert.Assert(t, !exists)

	_, err = elasticSearch.Upsert(index, doc1Id, doc1)
	assert.NilError(t, err)

	exists, err = elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	exists, err = elasticSearch.DocumentExists(index, doc1Id)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	_, err = elasticSearch.DeleteDocument(index, doc1Id, true)
	assert.NilError(t, err)

	exists, err = elasticSearch.DocumentExists(index, doc1Id)
	assert.NilError(t, err)
	assert.Assert(t, !exists)

	_, err = elasticSearch.DeleteDocument(index, doc1Id, true)
	assert.ErrorContains(t, err, "404")

	_, err = elasticSearch.DeleteDocument(index, doc1Id, false)
	assert.NilError(t, err)

}
