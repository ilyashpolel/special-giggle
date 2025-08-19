package repository

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type StreamsRepo struct {
	client *dynamodbstreams.Client
}

func NewStreamsRepo(client *dynamodbstreams.Client) *StreamsRepo {
	return &StreamsRepo{client: client}
}

func (r *StreamsRepo) DescribeStream(ctx context.Context, arn string) (*dynamodbstreams.DescribeStreamOutput, error) {
	return r.client.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{StreamArn: &arn})
}

func (r *StreamsRepo) GetShardIterator(ctx context.Context, shardID string, streamARN string, iteratorType string, seq string) (string, error) {
	var shardIteratorMap = map[string]types.ShardIteratorType{
		"TRIM_HORIZON":          types.ShardIteratorTypeTrimHorizon,
		"LATEST":                types.ShardIteratorTypeLatest,
		"AT_SEQUENCE_NUMBER":    types.ShardIteratorTypeAtSequenceNumber,
		"AFTER_SEQUENCE_NUMBER": types.ShardIteratorTypeAfterSequenceNumber,
	}

	itType, ok := shardIteratorMap[iteratorType]
	if !ok {
		return "", fmt.Errorf("unknown shard iterator type: %s", iteratorType)
	}

	in := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           &shardID,
		StreamArn:         &streamARN,
		ShardIteratorType: itType,
	}

	if seq != "" {
		in.SequenceNumber = &seq
	}

	out, err := r.client.GetShardIterator(ctx, in)
	if err != nil {
		return "", err
	}

	return *out.ShardIterator, nil
}

func (r *StreamsRepo) GetRecords(ctx context.Context, shardIterator string) (*dynamodbstreams.GetRecordsOutput, error) {
	return r.client.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{ShardIterator: &shardIterator})
}
