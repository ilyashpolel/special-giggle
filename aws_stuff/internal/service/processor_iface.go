package service

import (
	"context"

	"aws_stuff/pkg/models"
)

type Processor interface {
	HandleSQS(ctx context.Context, body string) error
	HandleSNS(ctx context.Context, message string, forwardQueueURL string) error
	HandleS3Object(ctx context.Context, bucket string, key string) error
	HandleStreamRecord(ctx context.Context, item models.Item) error
	RunTimerTask(ctx context.Context, namespace string) error
}
