package repository

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type SNSRepo struct {
	client *sns.Client
}

func NewSNSRepo(client *sns.Client) *SNSRepo {
	return &SNSRepo{client: client}
}

func (r *SNSRepo) Publish(ctx context.Context, topicARN string, message string) error {
	_, err := r.client.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String(message),
	})
	return err
}
