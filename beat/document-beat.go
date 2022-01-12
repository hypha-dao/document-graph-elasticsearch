package beat

import (
	"fmt"

	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"github.com/sebastianmontero/slog-go/slog"
)

var CursorIndex = "cursor"
var CursorId = "c1"
var CursorProperty = "cursor"
var DocumentIndex = "documents"

var log *slog.Log

type DocumentBeat struct {
	ElasticSearch *service.ElasticSearch
	Cursor        string
}

func NewDocumentBeat(elasticSearch *service.ElasticSearch, logConfig *slog.Config) (*DocumentBeat, error) {
	log = slog.New(logConfig, "document-beat")

	docbeat := &DocumentBeat{
		ElasticSearch: elasticSearch,
	}
	cursor, err := docbeat.GetCursor()

	if err != nil {
		return nil, err
	}
	docbeat.Cursor = cursor
	return docbeat, nil
}

func (m *DocumentBeat) StoreDocument(chainDoc *domain.ChainDocument, cursor string) error {
	log.Infof("Storing chain document: %v, cursor: %v", chainDoc, cursor)
	parsedDoc, err := chainDoc.ToParsedDoc(nil)
	if err != nil {
		return fmt.Errorf("failed storing document: %v, error: %v", chainDoc, err)
	}
	doc := parsedDoc.Instance.Values
	log.Infof("Storing parsed document: %v, cursor: %v", doc, cursor)
	_, err = m.ElasticSearch.Upsert(DocumentIndex, doc["docId"].(string), doc)
	if err != nil {
		return fmt.Errorf("failed storing document: %v, error: %v", doc, err)
	}
	return m.UpdateCursor(cursor)
}

func (m *DocumentBeat) DeleteDocument(chainDoc *domain.ChainDocument, cursor string) error {
	log.Infof("Deleting chain document: %v, cursor: %v", chainDoc, cursor)

	_, err := m.ElasticSearch.DeleteDocument(DocumentIndex, chainDoc.GetDocId(), false)
	if err != nil {
		return fmt.Errorf("failed deleting document: %v, error: %v", chainDoc.GetDocId(), err)
	}
	return m.UpdateCursor(cursor)
}

func (m *DocumentBeat) UpdateCursor(cursor string) error {
	// log.Infof("Updating cursor: %v", cursor)
	_, err := m.ElasticSearch.Upsert(CursorIndex, CursorId, map[string]string{"cursor": cursor})
	if err != nil {
		return fmt.Errorf("failed updating cursor, error: %v", err)
	}
	return nil
}

func (m *DocumentBeat) GetCursor() (string, error) {

	exists, err := m.CursorExists()
	if err != nil {
		return "", err
	}
	if !exists {
		log.Infof("Cursor does not exist")
		return "", nil
	}
	log.Infof("Getting current cursor")
	doc, err := m.ElasticSearch.Get(CursorIndex, CursorId)
	if err != nil {
		return "", fmt.Errorf("failed getting cursor, index: %v, id: %v, error: %v", CursorIndex, CursorId, err)
	}
	return doc[CursorProperty].(string), nil
}

func (m *DocumentBeat) CursorExists() (bool, error) {
	log.Infof("Checking if cursor exists")
	exists, err := m.ElasticSearch.DocumentExists(CursorIndex, CursorId)

	if err != nil {
		return false, fmt.Errorf("failed checking if cursor exists, index: %v, id: %v, error: %v", CursorIndex, CursorId, err)
	}
	return exists, nil
}

func (m *DocumentBeat) GetDocument(docId string) (map[string]interface{}, error) {

	exists, err := m.DocumentExists(docId)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Infof("Document: %v, index: %v  does not exist", docId, DocumentIndex)
		return nil, nil
	}
	log.Infof("Getting document: %v, index: %v", docId, DocumentIndex)
	doc, err := m.ElasticSearch.Get(DocumentIndex, docId)
	if err != nil {
		return nil, fmt.Errorf("failed getting document, index: %v, id: %v, error: %v", DocumentIndex, docId, err)
	}
	return doc, nil
}

func (m *DocumentBeat) DocumentExists(docId string) (bool, error) {
	log.Infof("Checking if document: %v exists", docId)
	exists, err := m.ElasticSearch.DocumentExists(DocumentIndex, docId)

	if err != nil {
		return false, fmt.Errorf("failed checking if document exists, index: %v, id: %v, error: %v", DocumentIndex, docId, err)
	}
	return exists, nil
}

func (m *DocumentBeat) DeleteDocumentIndex() error {
	return m.deleteIndex(DocumentIndex)
}

func (m *DocumentBeat) DeleteCursorIndex() error {
	return m.deleteIndex(CursorIndex)
}

func (m *DocumentBeat) deleteIndex(index string) error {
	log.Infof("Checking index exists: %v", index)
	exists, err := m.ElasticSearch.IndexExists(index)
	if err != nil {
		return fmt.Errorf("failed checking if index: %v exists, error: %v", index, err)
	}
	if exists {
		_, err := m.ElasticSearch.DeleteIndex(index)
		if err != nil {
			return fmt.Errorf("failed deleting index: %v, error: %v", index, err)
		}
	}
	return nil
}
