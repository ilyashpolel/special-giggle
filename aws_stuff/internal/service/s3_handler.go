package service

import (
	"context"
	"time"

	"aws_stuff/pkg/models"
)

// HandleS3Object reads object content and writes to DynamoDB as payload.
func (s *ProcessorService) HandleS3Object(ctx context.Context, bucket string, key string) error {
	content, err := s.s3.GetObjectString(ctx, bucket, key)
	if err != nil {
		return err
	}
	item := models.Item{
		PK:        "S3#" + bucket + "#" + key,
		SK:        time.Now().UTC().Format(time.RFC3339Nano),
		Payload:   content,
		CreatedAt: time.Now().UTC(),
	}
	return s.dynamo.PutItem(ctx, item)
}
