package service

import (
	"context"

	"aws_stuff/pkg/models"
)

// HandleStreamRecord replicates item to replica table (simulated with the same item payload).
func (s *ProcessorService) HandleStreamRecord(ctx context.Context, item models.Item) error {
	return s.dynamo.ReplicateItem(ctx, item)
}
