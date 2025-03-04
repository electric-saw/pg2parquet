package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

// pegar do properties/env
const (
	AWS_S3_REGION = ""
	// AWS_S3_BUCKET = ""
)

// var sess = connectAWS()
func connectAWS() *session.Session {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(AWS_S3_REGION)})
	if err != nil {
		panic(err)
	}
	return sess
}
