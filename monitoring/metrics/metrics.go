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
	CreatedEdges = promauto.NewCounter(prometheus.CounterOpts{
		Name: "document_graph_elasticsearch_created_edges",
		Help: "# of created edges",
	})
	DeletedEdges = promauto.NewCounter(prometheus.CounterOpts{
		Name: "document_graph_elasticsearch_deleted_edges",
		Help: "# of deleted edges",
	})
	BlockNumber = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "document_graph_elasticsearch_block_number",
		Help: "Block Number",
	})
)
