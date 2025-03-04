package controller

import (
	"math"

	"github.com/electric-saw/pg2parquet/pkg/config"
	"github.com/electric-saw/pg2parquet/pkg/pgoutputv2"

	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

type Manager struct {
	tableBuffer map[uint32]*TableBuffer
	relations   *pgoutputv2.RelationManager
	config      *config.Config
}

func newManager(relations *pgoutputv2.RelationManager, config *config.Config) *Manager {
	return &Manager{
		relations:   relations,
		tableBuffer: make(map[uint32]*TableBuffer),
		config:      config,
	}
}

func (m *Manager) handleTable(relation *pglogrepl.RelationMessage, lsn pglogrepl.LSN) {
	logrus.Infof("Relation received: %s", relation.RelationName)

	var types []*pgoutputv2.ValueType
	types, err := m.relations.Types(relation.RelationID)
	logrus.WithField("types", types)
	if err != nil {
		logrus.WithError(err).Fatalf("can't find types of table %d", relation.RelationID)
	}

	if table, ok := m.tableBuffer[relation.RelationID]; ok {
		if table.refreshTypes(types) {
			err = table.commitTable()
			if err != nil {
				logrus.WithError(err).Fatalf("can't commit table %s", table.name)
			}
		}
	} else {
		m.tableBuffer[relation.RelationID] = NewTable(relation.RelationName, types, lsn, m.config)
	}
}

func (m *Manager) handleRow(relationId uint32, row *pglogrepl.TupleData, lsn pglogrepl.LSN) {
	values, err := m.relations.ParseValues(relationId, row)
	if err != nil {
		logrus.WithError(err).Fatalf("can't find types of table %d", relationId)
	}

	if table, ok := m.tableBuffer[relationId]; ok {
		err = table.WriteRow(values, lsn)
		if err != nil {
			logrus.
				WithError(err).
				WithFields(logrus.Fields{
					"table": table.name,
					"row":   values.GetValues(),
				}).Panicf("can't write fields on parquet")
		}
	} else {
		logrus.Panicf("relation %d not found", relationId)
	}
}

func (m *Manager) minBuffredLSN() pglogrepl.LSN {
	var minLSN pglogrepl.LSN = math.MaxUint64
	for _, table := range m.tableBuffer {
		if table.currentLsn < minLSN {
			minLSN = table.currentLsn
		}
	}
	return minLSN
}

func (m *Manager) minCommitedLSN() pglogrepl.LSN {
	var minLSN pglogrepl.LSN = math.MaxUint64
	for _, table := range m.tableBuffer {
		if table.lastCommitedLSN < minLSN {
			minLSN = table.lastCommitedLSN
		}
	}
	return minLSN
}

func (m *Manager) stop() {
	for _, table := range m.tableBuffer {
		if err := table.commitTable(); err != nil {
			logrus.WithError(err).Panicf("fail on commit table %s", table.name)
		}
	}
	logrus.Info("stop event manager")
}
