package beat

import (
	"fmt"
	"strings"

	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"github.com/sebastianmontero/slog-go/slog"
)

var (
	CursorIndex                   = "cursor"
	CursorId                      = "c1"
	CursorProperty                = "cursor"
	DocumentIndex                 = "documents"
	SingleTextSearchFieldName     = "single_text_search_field"
	SingleTextSearchFieldMappings = fmt.Sprintf(` {
			"properties": {
				"%v": {
					"type": "completion"
				}
			}
		}
	`, SingleTextSearchFieldName)
	SingleTextSearchFieldIndexConfig = fmt.Sprintf(`
		{
			"mappings": %v	
		}
	`, SingleTextSearchFieldMappings)

	BaseIndex = `
		{
			"mappings": {}
		}
	`
)

var log *slog.Log

type SingleTextSearchField struct {
	strings.Builder
}

func (m *SingleTextSearchField) AddValue(value interface{}, op config.SingleTextSearchFieldOp) {
	if op == config.SingleTextSearchFieldOp_None {
		return
	}
	m.WriteString(fmt.Sprintf("%v ", value))
}

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

	err = docbeat.configureIndexes()
	if err != nil {
		return nil, fmt.Errorf("failed configuring indexes, error: %v", err)
	}
	return docbeat, nil
}

func (m *DocumentBeat) StoreDocument(chainDoc *domain.ChainDocument, cursor string, contractConfig *config.ContractConfig) error {
	log.Infof("Storing chain document: %v, cursor: %v, contract config: %v", chainDoc, cursor, contractConfig)
	doc, err := m.ToParsedDoc(chainDoc)
	if err != nil {
		return fmt.Errorf("failed storing document: %v, cursor: %v, contract config: %v, error: %v", chainDoc, cursor, contractConfig, err)
	}
	log.Infof("Storing parsed document: %v, cursor: %v", doc, cursor)
	_, err = m.ElasticSearch.Upsert(contractConfig.IndexName, doc["docId"].(string), doc)
	if err != nil {
		return fmt.Errorf("failed storing document: %v, cursor: %v, contract config: %v, error: %v", doc, cursor, contractConfig, err)
	}
	return m.UpdateCursor(cursor)
}

func (m *DocumentBeat) DeleteDocument(chainDoc *domain.ChainDocument, cursor string, contractConfig *config.ContractConfig) error {
	log.Infof("Deleting chain document: %v, cursor: %v, contract config: %v", chainDoc, cursor, contractConfig)

	_, err := m.ElasticSearch.DeleteDocument(contractConfig.IndexName, chainDoc.GetDocId(), false)
	if err != nil {
		return fmt.Errorf("failed deleting document: %v, cursor: %v, contract config: %v, error: %v", chainDoc.GetDocId(), cursor, contractConfig, err)
	}
	return m.UpdateCursor(cursor)
}

func (m *DocumentBeat) UpdateCursor(cursor string) error {
	// log.Infof("Updating cursor: %v", cursor)
	_, err := m.ElasticSearch.Upsert(m.Config.CursorIndexName, CursorId, map[string]string{"cursor": cursor})
	if err != nil {
		return fmt.Errorf("failed updating cursor, name: %v, value: %v, error: %v", m.Config.CursorIndexName, cursor, err)
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
	doc, err := m.ElasticSearch.Get(m.Config.CursorIndexName, CursorId)
	if err != nil {
		return "", fmt.Errorf("failed getting cursor, index: %v, id: %v, error: %v", m.Config.CursorIndexName, CursorId, err)
	}
	return doc[CursorProperty].(string), nil
}

func (m *DocumentBeat) CursorExists() (bool, error) {
	log.Infof("Checking if cursor exists")
	exists, err := m.ElasticSearch.DocumentExists(m.Config.CursorIndexName, CursorId)

	if err != nil {
		return false, fmt.Errorf("failed checking if cursor exists, index: %v, id: %v, error: %v", m.Config.CursorIndexName, CursorId, err)
	}
	return exists, nil
}

func (m *DocumentBeat) configureIndexes() error {

	log.Infof("Configuring indexes...")
	for _, contract := range m.Config.Contracts {
		index := contract.IndexName
		exists, err := m.IndexExists(index)
		if err != nil {
			return err
		}
		if !exists {
			log.Infof("Index: %v not exists, creating base index...", index)
			_, err = m.ElasticSearch.UpsertIndex(index, BaseIndex)
			if err != nil {
				return fmt.Errorf("failed creating index: %v for index: %v exists, error: %v", BaseIndex, index, err)
			}
		}
	}
	return nil
}

// func (m *DocumentBeat) configureIndexes() error {

// 	if !m.Config.RequiresSingleTextSearchField() {
// 		log.Infof("No single text field required, not configuring indexes")
// 		return nil
// 	}
// 	log.Infof("Single text field required, configuring indexes...")
// 	for _, contract := range m.Config.Contracts {
// 		index := contract.IndexName
// 		exists, err := m.IndexExists(index)
// 		if err != nil {
// 			return err
// 		}
// 		if exists {
// 			log.Infof("Index: %v exists, updating single search text field mappings...", index)
// 			_, err = m.ElasticSearch.UpdateMappings(index, SingleTextSearchFieldMappings)
// 			if err != nil {
// 				return fmt.Errorf("failed updating mappings: %v for index: %v exists, error: %v", SingleTextSearchFieldMappings, index, err)
// 			}
// 		} else {
// 			log.Infof("Index: %v not exists, creating index with single search text field mappings...", index)
// 			_, err = m.ElasticSearch.UpsertIndex(index, SingleTextSearchFieldIndexConfig)
// 			if err != nil {
// 				return fmt.Errorf("failed creating index: %v for index: %v exists, error: %v", SingleTextSearchFieldIndexConfig, index, err)
// 			}
// 		}
// 	}
// 	return nil
// }

func (m *DocumentBeat) GetDocument(docId, docIndex string) (map[string]interface{}, error) {

	exists, err := m.DocumentExists(docId, docIndex)
	if err != nil {
		return nil, err
	}
	if !exists {
		log.Infof("Document: %v, index: %v  does not exist", docId, docIndex)
		return nil, nil
	}
	log.Infof("Getting document: %v, index: %v", docId, docIndex)
	doc, err := m.ElasticSearch.Get(docIndex, docId)
	if err != nil {
		return nil, fmt.Errorf("failed getting document, index: %v, id: %v, error: %v", docIndex, docId, err)
	}
	return doc, nil
}

func (m *DocumentBeat) DocumentExists(docId, docIndex string) (bool, error) {
	log.Infof("Checking if document: %v exists", docId)
	exists, err := m.ElasticSearch.DocumentExists(docIndex, docId)

	if err != nil {
		return false, fmt.Errorf("failed checking if document exists, index: %v, id: %v, error: %v", docIndex, docId, err)
	}
	return exists, nil
}

func (m *DocumentBeat) DeleteCursorIndex() error {
	return m.DeleteIndex(m.Config.CursorIndexName)
}

func (m *DocumentBeat) CursorIndexExists() (bool, error) {
	return m.IndexExists(m.Config.CursorIndexName)
}

func (m *DocumentBeat) IndexExists(index string) (bool, error) {
	log.Infof("Checking index exists: %v", index)
	exists, err := m.ElasticSearch.IndexExists(index)
	if err != nil {
		return false, fmt.Errorf("failed checking if index: %v exists, error: %v", index, err)
	}
	return exists, nil
}

func (m *DocumentBeat) DeleteIndex(index string) error {
	exists, err := m.IndexExists(index)
	if err != nil {
		return err
	}
	if exists {
		_, err := m.ElasticSearch.DeleteIndex(index)
		if err != nil {
			return fmt.Errorf("failed deleting index: %v, error: %v", index, err)
		}
	}
	return nil
}

func (m *DocumentBeat) ToParsedDoc(doc *domain.ChainDocument) (map[string]interface{}, error) {

	var singleTextField SingleTextSearchField

	values := map[string]interface{}{
		"docId":    doc.GetDocId(),
		"contract": doc.Contract,
	}

	singleTextFieldOp := m.Config.GetSingleTextSearchFieldOp(domain.ContentType_Int64)
	m.processField(doc.ID, "docId_i", values, &singleTextField, singleTextFieldOp)
	singleTextFieldOp = m.Config.GetSingleTextSearchFieldOp(domain.ContentType_Name)
	m.processField(doc.Creator, "creator", values, &singleTextField, singleTextFieldOp)
	singleTextFieldOp = m.Config.GetSingleTextSearchFieldOp(domain.ContentType_Time)
	m.processField(domain.FormatDateTime(doc.CreatedDate), "createdDate", values, &singleTextField, singleTextFieldOp)
	m.processField(domain.FormatDateTime(doc.UpdatedDate), "updatedDate", values, &singleTextField, singleTextFieldOp)
	for i, contentGroup := range doc.ContentGroups {
		contentGroupLabel, err := domain.GetContentGroupLabel(contentGroup)
		if err != nil {
			return nil, fmt.Errorf("failed to get content_group_label for content group: %v in document with ID: %v, err: %v", i, doc.ID, err)
		}
		prefix := domain.GetFieldPrefix(contentGroupLabel)
		for _, content := range contentGroup {
			if content.Label != domain.CGL_ContentGroup {
				name := domain.GetFieldName(prefix, content.Label, content.GetType())
				value, err := content.GetGQLValue()
				if err != nil {
					return nil, fmt.Errorf("failed to get gql value content: %v name for doc with ID: %v, error: %v", name, doc.ID, err)
				}
				m.processField(value, name, values, &singleTextField, m.Config.GetSingleTextSearchFieldOp(content.GetType()))
				if m.Config.AddIntsAsStrings && content.GetType() == domain.ContentType_Int64 {
					if v, ok := values[name]; ok {
						values[fmt.Sprintf("%v_s", name)] = fmt.Sprintf("%v", v)
					}
				}
			}
		}
	}
	if typeName, ok := values[domain.CL_type].(string); ok {
		m.processField(domain.GetObjectTypeName(typeName), "type", values, &singleTextField, m.Config.GetSingleTextSearchFieldOp(domain.ContentType_Name))
		delete(values, domain.CL_type)
	}
	if m.Config.RequiresSingleTextSearchField() {
		values[SingleTextSearchFieldName] = singleTextField.String()
	}
	return values, nil
}

func (m *DocumentBeat) processField(value interface{}, name string, values map[string]interface{}, singleTextField *SingleTextSearchField, op config.SingleTextSearchFieldOp) {
	if op != config.SingleTextSearchFieldOp_Replace {
		values[name] = value
	}
	singleTextField.AddValue(value, op)
}
