package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/sebastianmontero/document-graph-elasticsearch/config"
)

type ElasticSearch struct {
	Client *elasticsearch7.Client
}

func NewElasticSearch(config *config.Config) (*ElasticSearch, error) {

	cert, err := ioutil.ReadFile(config.ElasticCA)

	if err != nil {
		return nil, fmt.Errorf("failed reading elastic CA file: %v, error: %v", config.ElasticCA, err)
	}

	cfg := elasticsearch7.Config{
		Addresses: []string{
			config.ElasticEndpoint,
		},
		Username: config.ElasticUser,
		Password: config.ElasticPassword,
		CACert:   cert,
	}
	client, err := elasticsearch7.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed creating elastic search client, error: %v", err)
	}
	return &ElasticSearch{
		Client: client,
	}, nil
}

func (m *ElasticSearch) Upsert(index, documentId string, doc interface{}) (map[string]interface{}, error) {
	marshalledDoc, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("failed marshalling document: %v to json for index: %v, error: %v", doc, index, err)
	}
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: documentId,
		Body:       strings.NewReader(string(marshalledDoc)),
		Refresh:    "true",
	}
	res, err := req.Do(context.Background(), m.Client)
	if err != nil {
		return nil, fmt.Errorf("failed upserting document: %s in index: %v, error: %v", marshalledDoc, index, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("failed indexing document: %s in index: %v, status: %v", marshalledDoc, index, res.Status())
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return nil, fmt.Errorf("failed parsing the response body from upserting, index: %v, document: %v, error: %v", index, marshalledDoc, err)
	}
	return r, nil
}

func (m *ElasticSearch) Get(index, documentId string) (map[string]interface{}, error) {

	req := esapi.GetRequest{
		Index:      index,
		DocumentID: documentId,
	}
	res, err := req.Do(context.Background(), m.Client)
	if err != nil {
		return nil, fmt.Errorf("failed getting document: %s from index: %v, error: %v", documentId, index, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("failed getting document: %s from index: %v, status: %v", documentId, index, res.Status())
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return nil, fmt.Errorf("failed parsing the response body from getting document, index: %v, document: %v, error: %v", index, documentId, err)
	}
	return r["_source"].(map[string]interface{}), nil
}

func (m *ElasticSearch) DeleteIndex(index string) (map[string]interface{}, error) {

	req := esapi.IndicesDeleteRequest{
		Index: []string{index},
	}
	res, err := req.Do(context.Background(), m.Client)
	if err != nil {
		return nil, fmt.Errorf("failed deleting index: %s, error: %v", index, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("failed deleting index: %s, status: %v", index, res.Status())
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return nil, fmt.Errorf("failed parsing the response body from deleting index, index: %v, error: %v", index, err)
	}
	return r, nil
}

func (m *ElasticSearch) IndexExists(index string) (bool, error) {

	req := esapi.IndicesExistsRequest{
		Index: []string{index},
	}
	res, err := req.Do(context.Background(), m.Client)
	if err != nil {
		return false, fmt.Errorf("failed checking if index: %s exists, error: %v", index, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		if isNotExistsError(res) {
			return false, nil
		}
		return false, fmt.Errorf("failed checking if index: %s exists, status: %v", index, res.Status())
	}
	return true, nil
}

func (m *ElasticSearch) DocumentExists(index string, documentId string) (bool, error) {

	req := esapi.ExistsRequest{
		Index:      index,
		DocumentID: documentId,
	}
	res, err := req.Do(context.Background(), m.Client)
	if err != nil {
		return false, fmt.Errorf("failed checking if document exists, index: %s documentid: %s, error: %v", index, documentId, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		if isNotExistsError(res) {
			return false, nil
		}
		return false, fmt.Errorf("failed checking if document exists, index: %s, document: %s, status: %v", index, documentId, res.Status())
	}
	return true, nil
}

func (m *ElasticSearch) DeleteDocument(index, documentId string, failIfNotExists bool) (map[string]interface{}, error) {

	req := esapi.DeleteRequest{
		Index:      index,
		DocumentID: documentId,
		Refresh:    "true",
	}
	res, err := req.Do(context.Background(), m.Client)
	if err != nil {
		return nil, fmt.Errorf("failed deleting document: %s from index: %v, error: %v", documentId, index, err)
	}
	defer res.Body.Close()
	if res.IsError() {
		if failIfNotExists || !isNotExistsError(res) {
			return nil, fmt.Errorf("failed deleting document: %s from index: %v, status: %v", documentId, index, res.Status())
		}
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return nil, fmt.Errorf("failed parsing the response body from deleting document, index: %v, document: %v, error: %v", index, documentId, err)
	}
	return r, nil
}

func isNotExistsError(res *esapi.Response) bool {
	return strings.Contains(res.Status(), "404")
}