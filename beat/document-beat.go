package beat

import (
	"fmt"

	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/gql"
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
	Config        *config.Config
}

func NewDocumentBeat(elasticSearch *service.ElasticSearch, config *config.Config, logConfig *slog.Config) (*DocumentBeat, error) {
	log = slog.New(logConfig, "document-beat")

	docbeat := &DocumentBeat{
		ElasticSearch: elasticSearch,
		Config:        config,
	}
	cursor, err := docbeat.GetCursor()

	if err != nil {
		return nil, err
	}
	docbeat.Cursor = cursor
	return docbeat, nil
}

func (m *DocumentBeat) StoreDocument(chainDoc *domain.ChainDocument, cursor string, contractConfig *config.ContractConfig) error {
	log.Infof("Storing chain document: %v, cursor: %v, contract config: %v", chainDoc, cursor, contractConfig)
	doc, err := toParsedDoc(chainDoc)
	if err != nil {
		return fmt.Errorf("failed storing document: %v, cursor: %v, contract config: %v, error: %v", chainDoc, cursor, contractConfig, err)
	}
	log.Infof("Storing parsed document: %v, cursor: %v", doc, cursor)
	_, err = m.ElasticSearch.Upsert(getDocIndexName(contractConfig.IndexPrefix), doc["docId"].(string), doc)
	if err != nil {
		return fmt.Errorf("failed storing document: %v, cursor: %v, contract config: %v, error: %v", doc, cursor, contractConfig, err)
	}
	return m.UpdateCursor(cursor)
}

func (m *DocumentBeat) DeleteDocument(chainDoc *domain.ChainDocument, cursor string, contractConfig *config.ContractConfig) error {
	log.Infof("Deleting chain document: %v, cursor: %v, contract config: %v", chainDoc, cursor, contractConfig)

	_, err := m.ElasticSearch.DeleteDocument(getDocIndexName(contractConfig.IndexPrefix), chainDoc.GetDocId(), false)
	if err != nil {
		return fmt.Errorf("failed deleting document: %v, cursor: %v, contract config: %v, error: %v", chainDoc.GetDocId(), cursor, contractConfig, err)
	}
	return m.UpdateCursor(cursor)
}

func (m *DocumentBeat) UpdateCursor(cursor string) error {
	// log.Infof("Updating cursor: %v", cursor)
	_, err := m.ElasticSearch.Upsert(getCursorIndexName(m.Config.CursorIndexPrefix), CursorId, map[string]string{"cursor": cursor})
	if err != nil {
		return fmt.Errorf("failed updating cursor, name: %v, value: %v, error: %v", getCursorIndexName(m.Config.CursorIndexPrefix), cursor, err)
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
	doc, err := m.ElasticSearch.Get(getCursorIndexName(m.Config.CursorIndexPrefix), CursorId)
	if err != nil {
		return "", fmt.Errorf("failed getting cursor, index: %v, id: %v, error: %v", getCursorIndexName(m.Config.CursorIndexPrefix), CursorId, err)
	}
	return doc[CursorProperty].(string), nil
}

func (m *DocumentBeat) CursorExists() (bool, error) {
	log.Infof("Checking if cursor exists")
	exists, err := m.ElasticSearch.DocumentExists(getCursorIndexName(m.Config.CursorIndexPrefix), CursorId)

	if err != nil {
		return false, fmt.Errorf("failed checking if cursor exists, index: %v, id: %v, error: %v", getCursorIndexName(m.Config.CursorIndexPrefix), CursorId, err)
	}
	return exists, nil
}

func (m *DocumentBeat) GetDocument(docId, docIndexPrefix string) (map[string]interface{}, error) {

	exists, err := m.DocumentExists(docId, docIndexPrefix)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Infof("Document: %v, index: %v  does not exist", docId, getDocIndexName(docIndexPrefix))
		return nil, nil
	}
	log.Infof("Getting document: %v, index: %v", docId, getDocIndexName(docIndexPrefix))
	doc, err := m.ElasticSearch.Get(getDocIndexName(docIndexPrefix), docId)
	if err != nil {
		return nil, fmt.Errorf("failed getting document, index: %v, id: %v, error: %v", getDocIndexName(docIndexPrefix), docId, err)
	}
	return doc, nil
}

func (m *DocumentBeat) DocumentExists(docId, docIndexPrefix string) (bool, error) {
	log.Infof("Checking if document: %v exists", docId)
	exists, err := m.ElasticSearch.DocumentExists(getDocIndexName(docIndexPrefix), docId)

	if err != nil {
		return false, fmt.Errorf("failed checking if document exists, index: %v, id: %v, error: %v", getDocIndexName(docIndexPrefix), docId, err)
	}
	return exists, nil
}

func (m *DocumentBeat) DeleteDocumentIndex(docIndexPrefix string) error {
	return m.deleteIndex(getDocIndexName(docIndexPrefix))
}

func (m *DocumentBeat) DeleteCursorIndex() error {
	return m.deleteIndex(getCursorIndexName(m.Config.CursorIndexPrefix))
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

func toParsedDoc(chainDoc *domain.ChainDocument) (map[string]interface{}, error) {
	parsedDoc, err := chainDoc.ToParsedDoc(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to parse chain doc: %v, error: %v", chainDoc, err)
	}
	fields := parsedDoc.Instance.SimplifiedType.Fields
	values := parsedDoc.Instance.Values
	for _, f := range fields {
		if f.Name != "docId_i" && f.Type == gql.GQLType_Int64 {
			values[fmt.Sprintf("%v_s", f.Name)] = fmt.Sprintf("%v", values[f.Name])
		}
	}
	return values, nil
}

func getIndexName(prefix, suffix string) string {
	return fmt.Sprintf(`%v-%v`, prefix, suffix)
}

func getDocIndexName(prefix string) string {
	return getIndexName(prefix, DocumentIndex)
}

func getCursorIndexName(prefix string) string {
	return getIndexName(prefix, CursorIndex)
}
