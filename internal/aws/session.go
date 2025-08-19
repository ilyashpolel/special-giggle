package aws

import (
	"context"
	"net/http"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// NewSession creates a new AWS session, optionally pointing to LocalStack if endpoint is provided.
func NewSession(region string, localstackEndpoint string) (awsv2.Config, error) {
	httpClient := &http.Client{Timeout: 30 * time.Second}

	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithHTTPClient(httpClient),
	}

	if localstackEndpoint != "" {
		resolver := awsv2.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...any) (awsv2.Endpoint, error) {
				return awsv2.Endpoint{
					URL:               localstackEndpoint,
					HostnameImmutable: true,
				}, nil
			},
		)

		opts = append(opts,
			config.WithEndpointResolverWithOptions(resolver),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
	}

	return config.LoadDefaultConfig(context.Background(), opts...)
}
