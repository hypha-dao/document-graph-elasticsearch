package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CreatedDocs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "document_graph_elasticsearch_created_docs",
		Help: "# of created documents",
	})
	DeletedDocs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "document_graph_elasticsearch_deleted_docs",
		Help: "# of deleted documents",
	})
	BlockNumber = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "document_graph_elasticsearch_block_number",
		Help: "Block Number",
	})
)

// func init() {
// 	setupMetrics()
// }

// func counter(key string) prometheus.Counter {
// 	return metrics[key].(prometheus.Counter)
// }

// func gauge(key string) prometheus.Gauge {
// 	return metrics[key].(prometheus.Gauge)
// }

// func setupMetrics() {

// 	metrics = map[string]interface{}{
// 		"createdDocs": promauto.NewCounter(prometheus.CounterOpts{
// 			Name: "hypha_graph_document_cache_created_docs",
// 			Help: "# of created documents",
// 		}),
// 		"createdEdges": promauto.NewCounter(prometheus.CounterOpts{
// 			Name: "hypha_graph_document_cache_created_edges",
// 			Help: "# of created edges",
// 		}),
// 		"deletedDocs": promauto.NewCounter(prometheus.CounterOpts{
// 			Name: "hypha_graph_document_cache_deleted_docs",
// 			Help: "# of deleted documents",
// 		}),
// 		"deletedEdges": promauto.NewCounter(prometheus.CounterOpts{
// 			Name: "hypha_graph_document_cache_deleted_edges",
// 			Help: "# of deleted edges",
// 		}),
// 		"blockNumber": promauto.NewGauge(prometheus.GaugeOpts{
// 			Name: "hypha_graph_document_cache_block_number",
// 			Help: "Block Number",
// 		}),
// 	}
