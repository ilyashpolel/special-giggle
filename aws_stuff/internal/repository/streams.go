package repository

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type StreamsRepo struct {
	client *dynamodbstreams.DynamoDBStreams
}

func NewStreamsRepo(client *dynamodbstreams.DynamoDBStreams) *StreamsRepo {
	return &StreamsRepo{client: client}
}

func (r *StreamsRepo) DescribeStream(ctx context.Context, arn string) (*dynamodbstreams.DescribeStreamOutput, error) {
	return r.client.DescribeStreamWithContext(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: &arn})
}

func (r *StreamsRepo) GetShardIterator(ctx context.Context, shardID string, streamARN string, iteratorType string, seq string) (string, error) {
	in := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           &shardID,
		StreamArn:         &streamARN,
		ShardIteratorType: &iteratorType,
	}
	if seq != "" {
		in.SequenceNumber = &seq
	}
	out, err := r.client.GetShardIteratorWithContext(ctx, in)
	if err != nil {
		return "", err
	}
	return *out.ShardIterator, nil
}

func (r *StreamsRepo) GetRecords(ctx context.Context, shardIterator string) (*dynamodbstreams.GetRecordsOutput, error) {
	return r.client.GetRecordsWithContext(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: &shardIterator})
}
