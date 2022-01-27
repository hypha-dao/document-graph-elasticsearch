package service_test

import (
	"encoding/json"
	"log"
	"os"
	"strings"
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

func TestUpsertIndex(t *testing.T) {

	index := "prueba2"
	indexBody := `
	{
		"settings": {
			"index": {
				"max_ngram_diff": 2
			},
			"analysis": {
				"analyzer": {
					"default": {
						"tokenizer": "whitespace",
						"filter": [ "3_5_grams" ]
					}
				},
				"filter": {
					"3_5_grams": {
						"type": "ngram",
						"min_gram": 3,
						"max_gram": 5
					}
				}
			}
		}
	}
	`
	exists, err := elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	if exists {
		_, err := elasticSearch.DeleteIndex(index)
		assert.NilError(t, err)
	}

	_, err = elasticSearch.UpsertIndex(index, indexBody)
	assert.NilError(t, err)

	exists, err = elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	res, err := elasticSearch.GetIndex(index)
	assert.NilError(t, err)
	resJSON, err := json.Marshal(res)
	assert.NilError(t, err)
	resJSONStr := string(resJSON)
	assert.Assert(t, strings.Contains(resJSONStr, "\"filter\":[\"3_5_grams\"]"))
	assert.Assert(t, strings.Contains(resJSONStr, "\"tokenizer\":\"whitespace\""))

}

func TestUpdateMappings(t *testing.T) {

	index := "prueba3"
	mappingsBody := `
	{
		"properties": {
			"title":  { "type": "text"}
		}
	}
	`
	// indexBody := `
	// {
	// 	"settings": {
	// 		"analysis": {
	// 			"analyzer": {
	// 				"default": {
	// 					"tokenizer": "standard"
	// 				}
	// 			}
	// 		}
	// 	}
	// }
	// `
	indexBody := `
	{
		"mappings": {
			"properties": {
				"title":  { "type": "text"}
			}
		}
	}
	`
	exists, err := elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	if exists {
		_, err := elasticSearch.DeleteIndex(index)
		assert.NilError(t, err)
	}

	_, err = elasticSearch.UpsertIndex(index, indexBody)
	assert.NilError(t, err)

	exists, err = elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	_, err = elasticSearch.UpdateMappings(index, mappingsBody)
	assert.NilError(t, err)

	exists, err = elasticSearch.IndexExists(index)
	assert.NilError(t, err)
	assert.Assert(t, exists)

	res, err := elasticSearch.GetMappings(index)
	assert.NilError(t, err)
	resJSON, err := json.Marshal(res)
	assert.NilError(t, err)
	resJSONStr := string(resJSON)
	assert.Assert(t, strings.Contains(resJSONStr, "\"title\":{\"type\":\"text\"}"))

	mappingsBody = `
	{
		"properties": {
			"name": { "type": "keyword" }
		}
	}
	`
	_, err = elasticSearch.UpdateMappings(index, mappingsBody)
	assert.NilError(t, err)

	res, err = elasticSearch.GetMappings(index)
	assert.NilError(t, err)
	resJSON, err = json.Marshal(res)
	assert.NilError(t, err)
	resJSONStr = string(resJSON)
	assert.Assert(t, strings.Contains(resJSONStr, "\"title\":{\"type\":\"text\"}"))
	assert.Assert(t, strings.Contains(resJSONStr, "\"name\":{\"type\":\"keyword\"}"))

}
