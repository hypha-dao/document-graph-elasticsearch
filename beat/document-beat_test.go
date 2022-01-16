package beat_test

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	"github.com/sebastianmontero/document-graph-elasticsearch/beat"
	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"gotest.tools/assert"
)

var elasticSearch *service.ElasticSearch
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
	docbeat, err = beat.NewDocumentBeat(elasticSearch, nil)
	if err != nil {
		log.Fatal(err, "Failed creating docbeat client")
	}
}

func afterAll() {
}

func setup(t *testing.T) {
	err := docbeat.DeleteCursorIndex()
	assert.NilError(t, err)
	err = docbeat.DeleteDocumentIndex()
	assert.NilError(t, err)
}

func TestClearIndexes(t *testing.T) {

	setup(t)
}

func TestOpCycle(t *testing.T) {

	setup(t)
	t.Logf("Storing period 1 document")
	period1Id := "21"
	period1IdI, _ := strconv.ParseUint(period1Id, 10, 64)
	periodDoc := getPeriodDoc(period1IdI, 1)
	expectedPeriodDoc := getPeriodValues(period1IdI, 1)
	cursor := "cursor0"
	err := docbeat.StoreDocument(periodDoc, cursor)
	assert.NilError(t, err)
	assertDoc(t, expectedPeriodDoc)
	assertCursor(t, cursor)

	t.Logf("Storing dho document")
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
	cursor = "cursor1"
	err = docbeat.StoreDocument(dhoDoc, cursor)
	assert.NilError(t, err)
	assertDoc(t, expectedDHODoc)
	assertCursor(t, cursor)

	t.Logf("Storing member document")
	member1Id := "31"
	member1IdI, _ := strconv.ParseUint(member1Id, 10, 64)
	memberDoc := getMemberDoc(member1IdI, "member1")
	expectedMemberDoc := getMemberValues(member1IdI, "member1")
	cursor = "cursor2_1"

	err = docbeat.StoreDocument(memberDoc, cursor)
	assert.NilError(t, err)
	assertDoc(t, expectedMemberDoc)
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
	cursor = "cursor3"
	err = docbeat.StoreDocument(dhoDoc, cursor)
	assert.NilError(t, err)
	assertDoc(t, expectedDHODoc)
	assertCursor(t, cursor)

	cursor = "cursor4"
	err = docbeat.DeleteDocument(dhoDoc, cursor)
	assert.NilError(t, err)
	assertDocNotExists(t, dhoId)
	assertCursor(t, cursor)

	cursor = "cursor5"
	err = docbeat.DeleteDocument(periodDoc, cursor)
	assert.NilError(t, err)
	assertDocNotExists(t, period1Id)
	assertCursor(t, cursor)

	cursor = "cursor6"
	err = docbeat.DeleteDocument(memberDoc, cursor)
	assert.NilError(t, err)
	assertDocNotExists(t, member1Id)
	assertCursor(t, cursor)

}

func assertDoc(t *testing.T, doc map[string]interface{}) {
	d, err := docbeat.GetDocument(doc["docId"].(string))
	assert.NilError(t, err)
	assert.Equal(t, len(d), len(doc))
	for k, v := range doc {
		av, ok := d[k]
		assert.Assert(t, ok, "Expected field: %v not found", k)
		assert.Equal(t, fmt.Sprintf("%v", av), fmt.Sprintf("%v", v))
	}
}

func assertDocNotExists(t *testing.T, docId string) {
	exists, err := docbeat.DocumentExists(docId)
	assert.NilError(t, err)
	assert.Assert(t, !exists)
}

func assertCursor(t *testing.T, cursor string) {
	c, err := docbeat.GetCursor()
	assert.NilError(t, err)
	assert.Equal(t, c, cursor)
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
