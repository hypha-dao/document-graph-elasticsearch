package beat_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/sebastianmontero/document-graph-elasticsearch/beat"
	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"gotest.tools/assert"
)

var contractsConfig config.ContractsConfig
var contract1Config *config.ContractConfig
var contract2Config *config.ContractConfig
var docbeat *beat.DocumentBeat

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

}

func getBaseConfig() *config.Config {
	contract1Config = &config.ContractConfig{
		Name:         "contract1",
		DocTableName: "documents",
		IndexPrefix:  "test1",
	}
	contract1Config.Init()
	contract2Config = &config.ContractConfig{
		Name:         "contract2",
		DocTableName: "documents",
		IndexPrefix:  "test2",
	}
	contract2Config.Init()
	contractsConfig = config.ContractsConfig{
		"contract1": contract1Config,
		"contract2": contract2Config,
	}
	return &config.Config{
		Contracts:         contractsConfig,
		CursorIndexPrefix: "test",
		ElasticEndpoint:   "https://localhost:9200",
		ElasticCA:         "/home/sebastian/vsc-workspace/elastic-helm-charts/elasticsearch/examples/security/elastic-certificate.pem",
		ElasticUser:       "elastic",
		ElasticPassword:   "8GXQlCxXy0p8bSilFMqI",
		AddIntsAsStrings:  true,
		CursorIndexName:   "test-cursor",
	}
}

func afterAll() {
}

func setup(t *testing.T, cfg *config.Config) {

	elasticSearch, err := service.NewElasticSearch(cfg)
	if err != nil {
		log.Fatal(err, "Failed creating elasticSearch client")
	}
	for _, contractConfig := range contractsConfig {
		exists, err := elasticSearch.IndexExists(contractConfig.IndexName)
		assert.NilError(t, err)

		if exists {
			_, err := elasticSearch.DeleteIndex(contractConfig.IndexName)
			assert.NilError(t, err)
		}
	}

	docbeat, err = beat.NewDocumentBeat(elasticSearch, cfg, nil)
	if err != nil {
		log.Fatal(err, "Failed creating docbeat client")
	}
	err = docbeat.DeleteCursorIndex()
	assert.NilError(t, err)

}

func TestClearIndexes(t *testing.T) {
	setup(t, getBaseConfig())
}

func TestOpCycle(t *testing.T) {

	setup(t, getBaseConfig())
	t.Logf("Storing period 1 document")
	period1Id := "21"
	period1IdI, _ := strconv.ParseUint(period1Id, 10, 64)
	periodDoc := getPeriodDoc(period1IdI, 1)
	expectedPeriodDoc := getPeriodValues(period1IdI, 1)
	cursor := "cursor0"
	t.Logf("Storing period 1 document in contract1 index")
	err := docbeat.StoreDocument(periodDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedPeriodDoc, contract1Config.IndexName)
	assertCursor(t, cursor)

	cursor = "cursor1"
	t.Logf("Storing period 1 document in contract2 index")
	err = docbeat.StoreDocument(periodDoc, cursor, contract2Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedPeriodDoc, contract2Config.IndexName)
	assertCursor(t, cursor)

	dhoId := "2"
	dhoIdI, _ := strconv.ParseUint(dhoId, 10, 64)
	dhoDoc := &domain.ChainDocument{
		ID:          dhoIdI,
		CreatedDate: "2020-11-12T18:27:47.000",
		UpdatedDate: "2020-11-12T19:27:47.000",
		Creator:     "dao.hypha",
		Contract:    "contract1",
		ContentGroups: [][]*domain.ChainContent{
			{
				{
					Label: "root_node",
					Value: []interface{}{
						"name",
						"dao.hypha",
					},
				},
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"delete",
					},
				},
				{
					Label: "hvoice_salary_per_phase",
					Value: []interface{}{
						"asset",
						"4133.04 HVOICE",
					},
				},
				{
					Label: "time_share_x100",
					Value: []interface{}{
						"int64",
						"90",
					},
				},
			},
			{
				{
					Label: "root_node",
					Value: []interface{}{
						"name",
						"dao.hypha",
					},
				},
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"details",
					},
				},
				{
					Label: "hvoice_salary_per_phase",
					Value: []interface{}{
						"asset",
						"4133.04 HVOICE",
					},
				},
				{
					Label: "time_share_x100",
					Value: []interface{}{
						"int64",
						"60",
					},
				},
				{
					Label: "str_to_int",
					Value: []interface{}{
						"string",
						"60",
					},
				},
			},
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"name",
						"system",
					},
				},
				{
					Label: "type",
					Value: []interface{}{
						"name",
						"dho",
					},
				},
				{
					Label: "original_approved_date",
					Value: []interface{}{
						"time_point",
						"2021-04-12T05:09:36.5",
					},
				},
			},
		},
	}

	expectedDHODoc := map[string]interface{}{
		"docId":                          dhoId,
		"docId_i":                        dhoIdI,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"creator":                        "dao.hypha",
		"contract":                       "contract1",
		"type":                           "Dho",
		"details_rootNode_n":             "dao.hypha",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_timeShareX100_i":        int64(60),
		"details_timeShareX100_i_s":      "60",
		"details_strToInt_s":             "60",
		"delete_rootNode_n":              "dao.hypha",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_timeShareX100_i":         int64(90),
		"delete_timeShareX100_i_s":       "90",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
	}
	t.Logf("Storing dho document in contract1 index")
	cursor = "cursor2"
	err = docbeat.StoreDocument(dhoDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedDHODoc, contract1Config.IndexName)
	assertCursor(t, cursor)

	t.Logf("Storing dho document in contract2 index")
	cursor = "cursor3"
	err = docbeat.StoreDocument(dhoDoc, cursor, contract2Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedDHODoc, contract2Config.IndexName)
	assertCursor(t, cursor)

	t.Logf("Storing member document")
	member1Id := "31"
	member1IdI, _ := strconv.ParseUint(member1Id, 10, 64)
	memberDoc := getMemberDoc(member1IdI, "member1")
	expectedMemberDoc := getMemberValues(member1IdI, "member1")
	cursor = "cursor4"

	err = docbeat.StoreDocument(memberDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedMemberDoc, contract1Config.IndexName)
	assertCursor(t, cursor)

	t.Logf("Updating dho document")
	dhoDoc = &domain.ChainDocument{
		ID:          dhoIdI,
		CreatedDate: "2020-11-12T18:37:47.000",
		UpdatedDate: "2020-11-12T19:47:47.000",
		Creator:     "dao.hypha1",
		Contract:    "contract2",
		ContentGroups: [][]*domain.ChainContent{
			{
				{
					Label: "root_node",
					Value: []interface{}{
						"name",
						"dao.hypha1",
					},
				},
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"details",
					},
				},
				{
					Label: "time_share_x100",
					Value: []interface{}{
						"int64",
						"80",
					},
				},
				{
					Label: "str_to_int",
					Value: []interface{}{
						"string",
						"70",
					},
				},
			},
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"name",
						"system",
					},
				},
				{
					Label: "type",
					Value: []interface{}{
						"name",
						"dho",
					},
				},
				{
					Label: "original_approved_date",
					Value: []interface{}{
						"time_point",
						"2021-04-12T05:09:36.5",
					},
				},
			},
		},
	}

	expectedDHODoc = map[string]interface{}{
		"docId":                         dhoId,
		"docId_i":                       dhoIdI,
		"createdDate":                   "2020-11-12T18:37:47.000Z",
		"updatedDate":                   "2020-11-12T19:47:47.000Z",
		"creator":                       "dao.hypha1",
		"contract":                      "contract2",
		"type":                          "Dho",
		"details_rootNode_n":            "dao.hypha1",
		"details_timeShareX100_i":       int64(80),
		"details_timeShareX100_i_s":     "80",
		"details_strToInt_s":            "70",
		"system_originalApprovedDate_t": "2021-04-12T05:09:36.5Z",
	}
	cursor = "cursor5"
	err = docbeat.StoreDocument(dhoDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedDHODoc, contract1Config.IndexName)
	assertCursor(t, cursor)

	cursor = "cursor6"
	err = docbeat.DeleteDocument(dhoDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertDocNotExists(t, dhoId, contract1Config.IndexName)
	assertCursor(t, cursor)

	cursor = "cursor5"
	err = docbeat.DeleteDocument(periodDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertDocNotExists(t, period1Id, contract1Config.IndexName)
	assertCursor(t, cursor)

	cursor = "cursor6"
	err = docbeat.DeleteDocument(memberDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertDocNotExists(t, member1Id, contract1Config.IndexName)
	assertCursor(t, cursor)

	cursor = "cursor7"
	err = docbeat.DeleteDocument(dhoDoc, cursor, contract2Config)
	assert.NilError(t, err)
	assertDocNotExists(t, dhoId, contract2Config.IndexName)
	assertCursor(t, cursor)

	cursor = "cursor8"
	err = docbeat.DeleteDocument(periodDoc, cursor, contract2Config)
	assert.NilError(t, err)
	assertDocNotExists(t, period1Id, contract2Config.IndexName)
	assertCursor(t, cursor)

}

func TestSingleSearchTextFieldMappingsNotConfiguredForNoSingleTextField(t *testing.T) {
	setup(t, getBaseConfig())
	exists, err := docbeat.IndexExists(contract1Config.IndexName)
	assert.NilError(t, err)
	assert.Assert(t, !exists, "Index: %v should not exist", contract1Config.IndexName)
}

func TestSingleSearchTextFieldMappingsIsCreatedForSingleTextField(t *testing.T) {
	cfg := getBaseConfig()
	cfg.SingleTextSearchField = map[string]string{
		domain.ContentType_String: string(config.SingleTextSearchFieldOp_Include),
	}
	t.Logf("Mappings should be created for initial setup")
	setup(t, cfg)
	assertSingleSearchTextFieldMappings(t, contract1Config.IndexName, true)
	assertSingleSearchTextFieldMappings(t, contract2Config.IndexName, true)

	t.Logf("Mappings should be updated for already existant indexes")
	_, err := beat.NewDocumentBeat(docbeat.ElasticSearch, cfg, nil)
	assert.NilError(t, err)
	assertSingleSearchTextFieldMappings(t, contract1Config.IndexName, true)
	assertSingleSearchTextFieldMappings(t, contract2Config.IndexName, true)
}

func TestExistantIndexesAreUpdatedWithSingleSearchTextFieldMappingsForSingleTextField(t *testing.T) {
	cfg := getBaseConfig()
	t.Logf("Mappings should be created for initial setup")
	setup(t, cfg)
	assertIndexExists(t, contract1Config.IndexName, false)
	assertIndexExists(t, contract2Config.IndexName, false)

	period1Id := "21"
	period1IdI, _ := strconv.ParseUint(period1Id, 10, 64)
	periodDoc := getPeriodDoc(period1IdI, 1)
	expectedPeriodDoc := getPeriodValues(period1IdI, 1)
	cursor := "cursor0"

	t.Logf("Storing period 1 document in contract1 index to create index")
	err := docbeat.StoreDocument(periodDoc, cursor, contract1Config)
	assert.NilError(t, err)
	assertStoredDoc(t, expectedPeriodDoc, contract1Config.IndexName)
	assertCursor(t, cursor)
	assertIndexExists(t, contract1Config.IndexName, true)
	assertIndexExists(t, contract2Config.IndexName, false)
	assertSingleSearchTextFieldMappings(t, contract1Config.IndexName, false)

	cfg.SingleTextSearchField = map[string]string{
		domain.ContentType_String: string(config.SingleTextSearchFieldOp_Include),
	}
	t.Logf("Mappings should be updated for already existant indexes and created for non existant")
	_, err = beat.NewDocumentBeat(docbeat.ElasticSearch, cfg, nil)
	assert.NilError(t, err)
	assertIndexExists(t, contract1Config.IndexName, true)
	assertIndexExists(t, contract2Config.IndexName, true)
	assertSingleSearchTextFieldMappings(t, contract1Config.IndexName, true)
	assertSingleSearchTextFieldMappings(t, contract2Config.IndexName, true)
}

func TestToParsedDoc(t *testing.T) {

	var err error

	dhoId := "2"
	dhoIdI, _ := strconv.ParseUint(dhoId, 10, 64)
	dhoDoc := &domain.ChainDocument{
		ID:          dhoIdI,
		CreatedDate: "2020-11-12T18:27:47.000",
		UpdatedDate: "2020-11-12T19:27:47.000",
		Creator:     "dao.hypha",
		Contract:    "contract1",
		ContentGroups: [][]*domain.ChainContent{
			{
				{
					Label: "root_node",
					Value: []interface{}{
						"name",
						"dao.hypha",
					},
				},
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"delete",
					},
				},
				{
					Label: "hvoice_salary_per_phase",
					Value: []interface{}{
						"asset",
						"4133.04 HVOICE",
					},
				},
				{
					Label: "time_share_x100",
					Value: []interface{}{
						"int64",
						"90",
					},
				},
				{
					Label: "title",
					Value: []interface{}{
						"string",
						"This is a title",
					},
				},
			},
			{
				{
					Label: "root_node",
					Value: []interface{}{
						"name",
						"dao.hypha",
					},
				},
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"details",
					},
				},
				{
					Label: "hvoice_salary_per_phase",
					Value: []interface{}{
						"asset",
						"4133.04 HVOICE",
					},
				},
				{
					Label: "time_share_x100",
					Value: []interface{}{
						"int64",
						"60",
					},
				},
				{
					Label: "str_to_int",
					Value: []interface{}{
						"string",
						"60",
					},
				},
			},
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"name",
						"system",
					},
				},
				{
					Label: "type",
					Value: []interface{}{
						"name",
						"dho",
					},
				},
				{
					Label: "original_approved_date",
					Value: []interface{}{
						"time_point",
						"2021-04-12T05:09:36.5",
					},
				},
			},
		},
	}

	cfg := getBaseConfig()
	setup(t, cfg)
	if err != nil {
		log.Fatal(err, "Failed creating docbeat client")
	}

	t.Logf("Parsing document with no single search text field and AddIntsAsStrings set to true")

	expectedDoc := map[string]interface{}{
		"docId":                          dhoId,
		"docId_i":                        dhoIdI,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"creator":                        "dao.hypha",
		"contract":                       "contract1",
		"type":                           "Dho",
		"details_rootNode_n":             "dao.hypha",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_timeShareX100_i":        int64(60),
		"details_timeShareX100_i_s":      "60",
		"details_strToInt_s":             "60",
		"delete_rootNode_n":              "dao.hypha",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_timeShareX100_i":         int64(90),
		"delete_timeShareX100_i_s":       "90",
		"delete_title_s":                 "This is a title",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
	}

	actualDoc, err := docbeat.ToParsedDoc(dhoDoc)
	assert.NilError(t, err)
	assertDoc(t, expectedDoc, actualDoc, nil)

	t.Logf("Parsing document with no single search text field and AddIntsAsStrings set to false")
	cfg.AddIntsAsStrings = false

	expectedDoc = map[string]interface{}{
		"docId":                          dhoId,
		"docId_i":                        dhoIdI,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"creator":                        "dao.hypha",
		"contract":                       "contract1",
		"type":                           "Dho",
		"details_rootNode_n":             "dao.hypha",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_timeShareX100_i":        int64(60),
		"details_strToInt_s":             "60",
		"delete_rootNode_n":              "dao.hypha",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_timeShareX100_i":         int64(90),
		"delete_title_s":                 "This is a title",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
	}

	actualDoc, err = docbeat.ToParsedDoc(dhoDoc)
	assert.NilError(t, err)
	assertDoc(t, expectedDoc, actualDoc, nil)

	t.Logf("Parsing document with single search text field formed by [string:include] and AddIntsAsStrings set to false")
	cfg.SingleTextSearchField = map[string]string{
		domain.ContentType_String: string(config.SingleTextSearchFieldOp_Include),
	}

	expectedDoc = map[string]interface{}{
		"docId":                          dhoId,
		"docId_i":                        dhoIdI,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"creator":                        "dao.hypha",
		"contract":                       "contract1",
		"type":                           "Dho",
		"details_rootNode_n":             "dao.hypha",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_timeShareX100_i":        int64(60),
		"details_strToInt_s":             "60",
		"delete_rootNode_n":              "dao.hypha",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_timeShareX100_i":         int64(90),
		"delete_title_s":                 "This is a title",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
		beat.SingleTextSearchFieldName:   "",
	}

	actualDoc, err = docbeat.ToParsedDoc(dhoDoc)
	assert.NilError(t, err)
	assertDoc(t, expectedDoc, actualDoc, []string{"60", "This is a title"})

	t.Logf("Parsing document with single search text field formed by [string:include] and AddIntsAsStrings set to true")
	cfg.AddIntsAsStrings = true

	expectedDoc = map[string]interface{}{
		"docId":                          dhoId,
		"docId_i":                        dhoIdI,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"creator":                        "dao.hypha",
		"contract":                       "contract1",
		"type":                           "Dho",
		"details_rootNode_n":             "dao.hypha",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_timeShareX100_i":        int64(60),
		"details_timeShareX100_i_s":      "60",
		"details_strToInt_s":             "60",
		"delete_rootNode_n":              "dao.hypha",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_timeShareX100_i":         int64(90),
		"delete_timeShareX100_i_s":       "90",
		"delete_title_s":                 "This is a title",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
		beat.SingleTextSearchFieldName:   "",
	}

	actualDoc, err = docbeat.ToParsedDoc(dhoDoc)
	assert.NilError(t, err)
	assertDoc(t, expectedDoc, actualDoc, []string{"60", "This is a title"})

	t.Logf("Parsing document with single search text field formed by [string:include, name:replace] and AddIntsAsStrings set to true")
	cfg.SingleTextSearchField[domain.ContentType_Name] = string(config.SingleTextSearchFieldOp_Replace)

	expectedDoc = map[string]interface{}{
		"docId":                          dhoId,
		"docId_i":                        dhoIdI,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"contract":                       "contract1",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_timeShareX100_i":        int64(60),
		"details_timeShareX100_i_s":      "60",
		"details_strToInt_s":             "60",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_timeShareX100_i":         int64(90),
		"delete_timeShareX100_i_s":       "90",
		"delete_title_s":                 "This is a title",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
		beat.SingleTextSearchFieldName:   "",
	}

	actualDoc, err = docbeat.ToParsedDoc(dhoDoc)
	assert.NilError(t, err)
	assertDoc(t, expectedDoc, actualDoc, []string{"60", "This is a title", "dao.hypha", "dho"})

	t.Logf("Parsing document with single search text field formed by [string:include, name:replace, int:replace] and AddIntsAsStrings set to true")
	cfg.SingleTextSearchField[domain.ContentType_Int64] = string(config.SingleTextSearchFieldOp_Replace)

	expectedDoc = map[string]interface{}{
		"docId":                          dhoId,
		"createdDate":                    "2020-11-12T18:27:47.000Z",
		"updatedDate":                    "2020-11-12T19:27:47.000Z",
		"contract":                       "contract1",
		"details_hvoiceSalaryPerPhase_a": "4133.04 HVOICE",
		"details_strToInt_s":             "60",
		"delete_hvoiceSalaryPerPhase_a":  "4133.04 HVOICE",
		"delete_title_s":                 "This is a title",
		"system_originalApprovedDate_t":  "2021-04-12T05:09:36.5Z",
		beat.SingleTextSearchFieldName:   "",
	}

	actualDoc, err = docbeat.ToParsedDoc(dhoDoc)
	assert.NilError(t, err)
	assertDoc(t, expectedDoc, actualDoc, []string{"60", "This is a title", "dao.hypha", "dho", "90", dhoId})
}

func assertStoredDoc(t *testing.T, doc map[string]interface{}, docIndex string) {
	d, err := docbeat.GetDocument(doc["docId"].(string), docIndex)
	assert.NilError(t, err)
	assertDoc(t, doc, d, nil)
}

func assertDoc(t *testing.T, expected, actual map[string]interface{}, valuesInSingleTextField []string) {
	assert.Equal(t, len(expected), len(actual))
	for k, v := range expected {
		av, ok := actual[k]
		if k != beat.SingleTextSearchFieldName {
			assert.Assert(t, ok, "Expected field: %v not found", k)
			assert.Equal(t, fmt.Sprintf("%v", av), fmt.Sprintf("%v", v))
		}
	}
	if len(valuesInSingleTextField) > 0 {
		textField, ok := actual[beat.SingleTextSearchFieldName].(string)
		assert.Assert(t, ok, "expected single search text field not found")
		for _, v := range valuesInSingleTextField {
			assert.Assert(t, strings.Contains(textField, v), "expected value: %v in single search text field: '%v' not found", v, textField)
		}

	}
}

func assertDocNotExists(t *testing.T, docId, docIndex string) {
	exists, err := docbeat.DocumentExists(docId, docIndex)
	assert.NilError(t, err)
	assert.Assert(t, !exists)
}

func assertCursor(t *testing.T, cursor string) {
	c, err := docbeat.GetCursor()
	assert.NilError(t, err)
	assert.Equal(t, c, cursor)
}

func assertSingleSearchTextFieldMappings(t *testing.T, indexName string, mappingsShouldExist bool) {
	assertIndexExists(t, indexName, true)
	elasticSearch := docbeat.ElasticSearch
	res, err := elasticSearch.GetMappings(indexName)
	assert.NilError(t, err)
	resJSON, err := json.Marshal(res)
	assert.NilError(t, err)
	resJSONStr := string(resJSON)
	fmt.Println("Mappings response: ", resJSONStr)
	assert.Equal(t, strings.Contains(resJSONStr, "\"single_text_search_field\":{"), mappingsShouldExist, "Single text search field mappings for index: %v should exist: %v", indexName, mappingsShouldExist)
	assert.Equal(t, strings.Contains(resJSONStr, "\"type\":\"completion\""), mappingsShouldExist, "Single text search field mappings for index: %v should exist: %v", indexName, mappingsShouldExist)
}

func assertIndexExists(t *testing.T, indexName string, shouldExist bool) {
	exists, err := docbeat.IndexExists(indexName)
	assert.NilError(t, err)
	assert.Equal(t, exists, shouldExist, "Index: %v exists: %v should exist: %v", indexName, exists, shouldExist)
}

func getMemberDoc(docIdI uint64, account string) *domain.ChainDocument {
	return &domain.ChainDocument{
		ID:          docIdI,
		CreatedDate: "2020-11-12T19:27:47.000",
		UpdatedDate: "2020-11-12T19:27:47.000",
		Creator:     account,
		Contract:    "contract1",
		ContentGroups: [][]*domain.ChainContent{
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"details",
					},
				},
				{
					Label: "account",
					Value: []interface{}{
						"name",
						account,
					},
				},
			},
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"name",
						"system",
					},
				},
				{
					Label: "type",
					Value: []interface{}{
						"name",
						"member",
					},
				},
			},
		},
	}
}

func getUserDoc(docIdI uint64, account string) *domain.ChainDocument {
	return &domain.ChainDocument{
		ID:          docIdI,
		CreatedDate: "2020-11-12T19:27:47.000",
		UpdatedDate: "2020-11-12T19:27:47.000",
		Creator:     account,
		Contract:    "contract1",
		ContentGroups: [][]*domain.ChainContent{
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"details",
					},
				},
				{
					Label: "account",
					Value: []interface{}{
						"name",
						account,
					},
				},
			},
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"name",
						"system",
					},
				},
				{
					Label: "type",
					Value: []interface{}{
						"name",
						"user",
					},
				},
			},
		},
	}
}

func getUserValues(docIdI uint64, account string) map[string]interface{} {
	return map[string]interface{}{
		"docId":             strconv.FormatUint(docIdI, 10),
		"docId_i":           docIdI,
		"createdDate":       "2020-11-12T19:27:47.000Z",
		"updatedDate":       "2020-11-12T19:27:47.000Z",
		"creator":           account,
		"contract":          "contract1",
		"type":              "User",
		"details_account_n": account,
	}
}

func getMemberValues(docIdI uint64, account string) map[string]interface{} {
	return map[string]interface{}{
		"docId":             strconv.FormatUint(docIdI, 10),
		"docId_i":           docIdI,
		"createdDate":       "2020-11-12T19:27:47.000Z",
		"updatedDate":       "2020-11-12T19:27:47.000Z",
		"creator":           account,
		"contract":          "contract1",
		"type":              "Member",
		"details_account_n": account,
	}

}

func getPeriodDoc(id uint64, number int64) *domain.ChainDocument {
	return &domain.ChainDocument{
		ID:          id,
		CreatedDate: "2020-11-12T18:27:47.000",
		UpdatedDate: "2020-11-12T19:27:47.000",
		Creator:     "dao.hypha",
		Contract:    "contract1",
		ContentGroups: [][]*domain.ChainContent{
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"string",
						"details",
					},
				},
				{
					Label: "number",
					Value: []interface{}{
						"int64",
						number,
					},
				},
			},
			{
				{
					Label: "content_group_label",
					Value: []interface{}{
						"name",
						"system",
					},
				},
				{
					Label: "type",
					Value: []interface{}{
						"name",
						"period",
					},
				},
			},
		},
	}
}

func getPeriodValues(docId uint64, number int64) map[string]interface{} {
	return map[string]interface{}{
		"docId":              strconv.FormatUint(docId, 10),
		"docId_i":            docId,
		"createdDate":        "2020-11-12T18:27:47.000Z",
		"updatedDate":        "2020-11-12T19:27:47.000Z",
		"creator":            "dao.hypha",
		"contract":           "contract1",
		"type":               "Period",
		"details_number_i":   number,
		"details_number_i_s": fmt.Sprintf("%v", number),
	}
}
