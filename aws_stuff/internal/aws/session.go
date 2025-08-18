package aws

import (
	"net/http"
	"time"

	awsv1 "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// NewSession creates a new AWS session, optionally pointing to LocalStack if endpoint is provided.
func NewSession(region string, localstackEndpoint string) (*session.Session, *awsv1.Config, error) {
	cfg := &awsv1.Config{
		Region:     awsv1.String(region),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}

	if localstackEndpoint != "" {
		cfg.Endpoint = awsv1.String(localstackEndpoint)
		cfg.S3ForcePathStyle = awsv1.Bool(true)
		cfg.Credentials = credentials.NewStaticCredentials("test", "test", "")
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, nil, err
	}
	return sess, cfg, nil
}
