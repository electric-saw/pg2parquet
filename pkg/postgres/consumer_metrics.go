package postgres

import (
	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	opsInsert = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pg2parquet_ops_insert",
		Help: "The total number of insert events",
	})
	opsUpdate = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pg2parquet_ops_update",
		Help: "The total number of update events",
	})

	opsDelete = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pg2parquet_ops_delete",
		Help: "The total number of delete events",
	})

	writeLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pg2parquet_lag_write_bytes",
		Help: "The total lag between server and pg2parquet",
	})

	flushLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pg2parquet_lag_flush_bytes",
		Help: "The total lag between server and pg2parquet",
	})

	applyLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pg2parquet_lag_apply_bytes",
		Help: "The total lag between server and pg2parquet",
	})

	processedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pg2parquet_processed_bytes",
		Help: "The total lag between server and pg2parquet",
	})
)

func updateCountEvents(msg pglogrepl.Message) {
	switch msg.(type) {
	case *pglogrepl.InsertMessage, *pglogrepl.InsertMessageV2:
		opsInsert.Inc()
	case *pglogrepl.UpdateMessage, *pglogrepl.UpdateMessageV2:
		opsUpdate.Inc()
	case *pglogrepl.DeleteMessage, *pglogrepl.DeleteMessageV2:
		opsDelete.Inc()
	}
}
