package main

import (
	"encoding/json"
	"os"

	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	"github.com/rs/zerolog"
	"github.com/sebastianmontero/dfuse-firehose-client/dfclient"
	"github.com/sebastianmontero/document-graph-elasticsearch/beat"
	"github.com/sebastianmontero/document-graph-elasticsearch/config"
	"github.com/sebastianmontero/document-graph-elasticsearch/monitoring"
	"github.com/sebastianmontero/document-graph-elasticsearch/monitoring/metrics"
	"github.com/sebastianmontero/document-graph-elasticsearch/service"
	"github.com/sebastianmontero/hypha-document-cache-gql-go/doccache/domain"
	"github.com/sebastianmontero/slog-go/slog"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
)

// Main entry point of the document elastic search stream process, configures the dfuse client and defines the stream handler

var (
	log *slog.Log
)

// Dfuse stream handler, processes the table deltas and calls the correct DocumentBeat methods
// based on the delta type
type deltaStreamHandler struct {
	// Indicates where in the stream we are located
	cursor string
	// Processes the operations indicated by the table deltas and updates elastic search to reflect these changes
	documentBeat *beat.DocumentBeat
	// Stores the initial configuration information
	config *config.Config
}

// Called every time there is a table delta of interest, determines what the operation is and calls the
// corresponding DocumentBeat method
func (m *deltaStreamHandler) OnDelta(delta *dfclient.TableDelta, cursor string, forkStep pbbstream.ForkStep) {
	log.Debugf("On Delta: \nCursor: %v \nFork Step: %v \nDelta %v ", cursor, forkStep, delta)
	contractConfig := m.config.Contracts.Get(delta.Code)
	if contractConfig != nil {
		if contractConfig.DocTableName == delta.TableName {
			chainDoc := &domain.ChainDocument{}
			switch delta.Operation {
			case pbcodec.DBOp_OPERATION_INSERT, pbcodec.DBOp_OPERATION_UPDATE:
				err := json.Unmarshal(delta.NewData, chainDoc)
				if err != nil {
					log.Panicf(err, "Error unmarshalling doc new data: %v", string(delta.NewData))
				}
				log.Tracef("Storing doc: %v ", chainDoc)
				err = m.documentBeat.StoreDocument(chainDoc, cursor, contractConfig)
				if err != nil {
					log.Panicf(err, "Failed to store doc: %v", chainDoc)
				}
				metrics.CreatedDocs.Inc()
			case pbcodec.DBOp_OPERATION_REMOVE:
				err := json.Unmarshal(delta.OldData, chainDoc)
				if err != nil {
					log.Panicf(err, "Error unmarshalling doc old data: %v", string(delta.OldData))
				}
				err = m.documentBeat.DeleteDocument(chainDoc, cursor, contractConfig)
				if err != nil {
					log.Panicf(err, "Failed to delete doc: %v", chainDoc)
				}
				metrics.DeletedDocs.Inc()
			}
		} else if contractConfig.EdgeTableName == delta.TableName {
			switch delta.Operation {
			case pbcodec.DBOp_OPERATION_INSERT, pbcodec.DBOp_OPERATION_REMOVE:
				var (
					deltaData []byte
					deleteOp  bool
				)
				chainEdge := &domain.ChainEdge{}
				if delta.Operation == pbcodec.DBOp_OPERATION_INSERT {
					deltaData = delta.NewData
				} else {
					deltaData = delta.OldData
					deleteOp = true
				}
				err := json.Unmarshal(deltaData, chainEdge)
				if err != nil {
					log.Panicf(err, "Error unmarshalling edge data: %v", chainEdge)
				}
				err = m.documentBeat.MutateEdge(chainEdge, deleteOp, cursor, contractConfig)
				if err != nil {
					log.Panicf(err, "Failed to mutate doc, deleteOp: %v, edge: %v", deleteOp, chainEdge)
				}
				if deleteOp {
					metrics.DeletedEdges.Inc()
				} else {
					metrics.CreatedEdges.Inc()
				}

			case pbcodec.DBOp_OPERATION_UPDATE:
				log.Panicf(nil, "Edge updating is not handled: %v", delta)
			}
		}
	}
	metrics.BlockNumber.Set(float64(delta.Block.Number))
	m.cursor = cursor
}

// Called every certain amount of blocks and its useful to update the cursor when there are
// no deltas of interest for a long time
func (m *deltaStreamHandler) OnHeartBeat(block *pbcodec.Block, cursor string) {
	err := m.documentBeat.UpdateCursor(cursor)
	if err != nil {
		log.Panicf(err, "Failed to update cursor: %v", cursor)
	}
	metrics.BlockNumber.Set(float64(block.Number))
}

// Called when there is an error with the stream connection
func (m *deltaStreamHandler) OnError(err error) {
	log.Error(err, "On Error")
}

// Called when the requested stream completes, should never be called because there is no
// final block
func (m *deltaStreamHandler) OnComplete(lastBlockRef bstream.BlockRef) {
	log.Infof("On Complete Last Block Ref: %v", lastBlockRef)
}

// Loads the configuration file, creates a new dfuse client and configures it with the stream handler
// defined above
func main() {
	log = slog.New(&slog.Config{Pretty: true, Level: zerolog.DebugLevel}, "start-document-beat")
	if len(os.Args) != 2 {
		log.Panic(nil, "Config file has to be specified as the only cmd argument")
	}
	config, err := config.LoadConfig(os.Args[1])
	if err != nil {
		log.Panicf(err, "Unable to load config file: %v", os.Args[1])
	}

	log.Info(config.String())

	go monitoring.SetupEndpoint(config.PrometheusPort)
	if err != nil {
		log.Panic(err, "Error seting up prometheus endpoint")
	}

	client, err := dfclient.NewDfClient(config.FirehoseEndpoint, config.DfuseApiKey, config.DfuseAuthURL, config.EosEndpoint, nil)
	if err != nil {
		log.Panic(err, "Error creating dfclient")
	}

	elasticSearch, err := service.NewElasticSearch(config)
	if err != nil {
		log.Panic(err, "Error creating elastic search client")
	}
	docbeat, err := beat.NewDocumentBeat(elasticSearch, config, nil)
	if err != nil {
		log.Panic(err, "Error creating docbeat client")
	}
	log.Infof("Cursor: %v", docbeat.Cursor)
	deltaRequest := &dfclient.DeltaStreamRequest{
		StartBlockNum:      config.StartBlock,
		StartCursor:        docbeat.Cursor,
		StopBlockNum:       0,
		ForkSteps:          []pbbstream.ForkStep{pbbstream.ForkStep_STEP_NEW, pbbstream.ForkStep_STEP_UNDO},
		ReverseUndoOps:     true,
		HeartBeatFrequency: config.HeartBeatFrequency,
	}
	// deltaRequest.AddTables("eosio.token", []string{"balance"})
	for _, contract := range config.Contracts {
		deltaRequest.AddTables(contract.Name, []string{contract.DocTableName, contract.EdgeTableName})
	}

	client.DeltaStream(deltaRequest, &deltaStreamHandler{
		documentBeat: docbeat,
		config:       config,
	})
}
