package repository

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type SQSRepo struct {
	client *sqs.SQS
}

func NewSQSRepo(client *sqs.SQS) *SQSRepo {
	return &SQSRepo{client: client}
}

func (r *SQSRepo) Receive(ctx context.Context, queueURL string, maxMessages int64, visibilityTimeoutSeconds int64, waitTimeSeconds int64) ([]*sqs.Message, error) {
	out, err := r.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: aws.Int64(maxMessages),
		VisibilityTimeout:   aws.Int64(visibilityTimeoutSeconds),
		WaitTimeSeconds:     aws.Int64(waitTimeSeconds),
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func (r *SQSRepo) Delete(ctx context.Context, queueURL string, receiptHandle string) error {
	_, err := r.client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	return err
}

func (r *SQSRepo) Send(ctx context.Context, queueURL string, body string) error {
	_, err := r.client.SendMessageWithContext(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(body),
	})
	return err
}
