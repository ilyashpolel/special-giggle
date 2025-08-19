package repository

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSRepo struct {
	client *sqs.Client
}

func NewSQSRepo(client *sqs.Client) *SQSRepo {
	return &SQSRepo{client: client}
}

func (r *SQSRepo) Receive(ctx context.Context, queueURL string, maxMessages int32, visibilityTimeoutSeconds int32, waitTimeSeconds int32) ([]types.Message, error) {
	out, err := r.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: maxMessages,
		VisibilityTimeout:   visibilityTimeoutSeconds,
		WaitTimeSeconds:     waitTimeSeconds,
	})
	if err != nil {
		return nil, err
	}

	return out.Messages, nil
}

func (r *SQSRepo) Delete(ctx context.Context, queueURL string, receiptHandle string) error {
	_, err := r.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func (r *SQSRepo) Send(ctx context.Context, queueURL string, body string) error {
	_, err := r.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	})
	return err
}
