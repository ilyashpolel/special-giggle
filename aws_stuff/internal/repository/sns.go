package repository

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
)

type SNSRepo struct {
	client *sns.SNS
}

func NewSNSRepo(client *sns.SNS) *SNSRepo {
	return &SNSRepo{client: client}
}

func (r *SNSRepo) Publish(ctx context.Context, topicARN string, message string) error {
	_, err := r.client.PublishWithContext(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String(message),
	})
	return err
}
