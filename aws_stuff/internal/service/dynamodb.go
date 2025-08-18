package service

import (
	"context"

	"aws_stuff/pkg/models"
)

func (s *ProcessorService) SaveItem(ctx context.Context, item models.Item) error {
	return s.dynamo.PutItem(ctx, item)
}

// For demo, retrieving count via ScanCount to validate connectivity
func (s *ProcessorService) CountItems(ctx context.Context) (int64, error) {
	return s.dynamo.ScanCount(ctx)
}
