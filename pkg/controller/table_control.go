package controller

import (
	"reflect"
	"strings"

	"github.com/electric-saw/pg2parquet/pkg/config"
	"github.com/electric-saw/pg2parquet/pkg/parquet"
	"github.com/electric-saw/pg2parquet/pkg/pgoutputv2"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

type TableBuffer struct {
	name            string
	typ             reflect.Type
	columns         []*pgoutputv2.ValueType
	currentLsn      pglogrepl.LSN
	startLSN        pglogrepl.LSN
	lastCommitedLSN pglogrepl.LSN
	parquet         *parquet.Parquet
	rowCount        int64
	config          *config.Config
}

func NewTable(name string, columns []*pgoutputv2.ValueType, lsn pglogrepl.LSN, config *config.Config) *TableBuffer {
	buf := &TableBuffer{
		name:            name,
		rowCount:        0,
		startLSN:        lsn,
		currentLsn:      lsn,
		lastCommitedLSN: 0,
		typ:             nil,
		config:          config,
	}
	buf.refreshTypes(columns)
	return buf
}

func (tb *TableBuffer) WriteRow(data pgoutputv2.Values, lsn pglogrepl.LSN) error {
	if tb.parquet == nil {
		if err := tb.newPart(lsn); err != nil {
			return err
		}
	}

	dataStruct := reflect.Indirect(reflect.New(tb.typ))
	for i := range data {
		if err := valueToParquet(data[i], dataStruct.Field(i)); err != nil {
			return err
		}
	}

	if err := tb.parquet.WriteRow(dataStruct.Interface()); err != nil {
		return err
	}
	tb.currentLsn = lsn

	tb.rowCount++
	return tb.checkCommit()
}

func (tb *TableBuffer) checkCommit() error {
	totalSize := int64(0)
	for i := range tb.parquet.Pw.Footer.RowGroups {
		totalSize += int64(tb.parquet.Pw.Footer.RowGroups[i].TotalByteSize)
	}

	totalSize += int64(tb.parquet.Pw.Size)

	logrus.WithFields(logrus.Fields{
		"table": tb.name,
		"size":  totalSize,
	}).Debug("buf size")
	if totalSize <= tb.config.Parquet.PartSizeBytes() {
		return nil
	}

	if err := tb.commitTable(); err != nil {
		return err
	}

	tb.parquet = nil

	return nil
}

func (tb *TableBuffer) commitTable() error {
	totalSize := int64(0)

	if tb.parquet == nil || tb.rowCount == 0 {
		return nil
	}

	for i := range tb.parquet.Pw.Footer.RowGroups {
		totalSize += tb.parquet.Pw.Footer.RowGroups[i].TotalByteSize
	}
	totalSize += tb.parquet.Pw.Size
	if totalSize == 0 {
		return nil
	}

	if err := tb.parquet.CloseFile(); err != nil {
		return err
	}

	tb.parquet = nil
	tb.lastCommitedLSN = tb.currentLsn
	return nil
}

func (tb *TableBuffer) refreshTypes(columns []*pgoutputv2.ValueType) bool {
	if columns != nil {
		if cmp.Equal(columns, tb.columns) {
			return false
		}
		newTyp := dummyType(columns)
		tb.typ = newTyp
		tb.columns = columns
		return true
	}
	return false
}

func (tb *TableBuffer) newPart(lsn pglogrepl.LSN) error {
	if err := tb.commitTable(); err != nil {
		return err
	}

	tb.startLSN = lsn
	tb.currentLsn = lsn

	obj := reflect.New(tb.typ).Interface()

	filePath, err := tb.config.Storage.FormatedPath(
		map[string]any{
			"table":             tb.name,
			"start_lsn":         strings.ReplaceAll(lsn.String(), "/", "-"),
			"last_commited_lsn": strings.ReplaceAll(tb.lastCommitedLSN.String(), "/", "-"),
		},
	)
	if err != nil {
		return err
	}

	if parquetFile, err := parquet.NewParquet(filePath, obj, tb.config); err != nil {
		return err
	} else {
		tb.parquet = parquetFile
	}
	return nil
}
