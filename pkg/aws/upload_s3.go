package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go/source"
)

// pegar do properties/env
const (
	AWS_S3_BUCKET = "pg-test"
)

func handlerUpload(filename string, file source.ParquetFile, sess *session.Session) error {
	uploader := s3manager.NewUploader(sess)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(AWS_S3_BUCKET),
		Key:    aws.String(filename),
		Body:   file,
	})

	if err != nil {
		return fmt.Errorf("Something went wrong uploading the file %v", err)
	}

	fmt.Printf("Successfully uploaded to %q\n", AWS_S3_BUCKET)

	return nil
}
