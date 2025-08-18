package repository

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/sqs"

	"aws_stuff/pkg/models"
)

type DynamoRepository interface {
	PutItem(ctx context.Context, item models.Item) error
	ScanCount(ctx context.Context) (int64, error)
	ReplicateItem(ctx context.Context, item models.Item) error
}

type SQSRepository interface {
	Receive(ctx context.Context, queueURL string, maxMessages int64, visibilityTimeoutSeconds int64, waitTimeSeconds int64) ([]*sqs.Message, error)
	Delete(ctx context.Context, queueURL string, receiptHandle string) error
	Send(ctx context.Context, queueURL string, body string) error
}

type SNSRepository interface {
	Publish(ctx context.Context, topicARN string, message string) error
}

type S3Repository interface {
	GetObjectString(ctx context.Context, bucket string, key string) (string, error)
}

type CloudWatchRepository interface {
	PutCountMetric(ctx context.Context, namespace string, metricName string, value float64) error
}

type DynamoDBStreamsRepository interface {
	DescribeStream(ctx context.Context, arn string) (*dynamodbstreams.DescribeStreamOutput, error)
	GetShardIterator(ctx context.Context, shardID string, streamARN string, iteratorType string, seq string) (string, error)
	GetRecords(ctx context.Context, shardIterator string) (*dynamodbstreams.GetRecordsOutput, error)
}
