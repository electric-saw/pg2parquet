package parquet

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/electric-saw/pg2parquet/pkg/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsS3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/source"
)

func makeFileWriter(storageConfig *config.Storage, filePath string) (source.ParquetFile, error) {
	switch strings.ToLower(storageConfig.Type) {
	case "local":
		return newLocalStorage(storageConfig, filePath)
	case "s3":
		return newS3Storage(storageConfig, filePath)

	default:
		return nil, fmt.Errorf("can't create parquet file on %s, see your configuration file", storageConfig.Type)
	}
}

func newLocalStorage(storageConfig *config.Storage, filePath string) (source.ParquetFile, error) {
	if err := os.MkdirAll(path.Dir(filePath), os.ModePerm); err != nil {
		return nil, fmt.Errorf("can't ensure path %s: %w", filePath, err)
	}

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {

		return nil, fmt.Errorf("can't open file \"%s\": %w", filePath, err)
	}
	return fw, nil
}

func newS3Storage(storageConfig *config.Storage, filePath string) (source.ParquetFile, error) {
	s3Cfg := aws.NewConfig().
		WithEndpoint(storageConfig.Endpoint).
		WithRegion("us-east-2")
	sess, err := session.NewSession(s3Cfg)

	if err != nil {
		return nil, err
	}

	fw, err := s3.NewS3FileWriterWithClient(context.TODO(), awsS3.New(sess), storageConfig.Bucket, filePath, "", nil) // TODO: use correct acl
	if err != nil {
		return nil, fmt.Errorf("can't create s3 provider: %w", err)
	}
	return fw, nil
}
