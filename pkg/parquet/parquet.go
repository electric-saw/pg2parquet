package parquet

import (
	"fmt"

	"github.com/electric-saw/pg2parquet/internal/version"
	"github.com/electric-saw/pg2parquet/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type Parquet struct {
	Pw *writer.ParquetWriter
	Fw source.ParquetFile
}

func NewParquet(filePath string, typ any, config *config.Config) (*Parquet, error) {

	fw, err := makeFileWriter(config.Storage, filePath)
	if err != nil {
		return nil, err
	}

	pw, err := writer.NewParquetWriter(fw, typ, 1)
	if err != nil {
		logrus.WithError(err).Fatal("can't create parquet writer")
		return nil, err
	}

	compressionType, err := parquet.CompressionCodecFromString(config.Parquet.CompressionType)
	if err != nil {
		logrus.Warnf("can't parse compression type: %s, using default SNAPPY", config.Parquet.CompressionType)
		compressionType = parquet.CompressionCodec_SNAPPY
	}

	createdBy := fmt.Sprintf("Pg2parquet - version: %s - go version: %s", version.GetVersion(), version.Get().GoVersion)
	pw.Footer.CreatedBy = &createdBy
	pw.CompressionType = compressionType
	pw.PageSize = int64(config.Parquet.PageSizeBytes()) //8K
	pw.RowGroupSize = 64 * 1024 * 1024                  //128M

	return &Parquet{
		Fw: fw,
		Pw: pw,
	}, nil
}

func (p *Parquet) WriteRow(row any) error {
	if err := p.Pw.Write(row); err != nil {
		return err
	}

	if err := p.Pw.Flush(true); err != nil {
		return err
	}

	return nil
}

func (p *Parquet) CloseFile() error {
	if err := p.Pw.WriteStop(); err != nil {
		return err
	}
	if err := p.Fw.Close(); err != nil {
		return err
	}
	return nil
}
