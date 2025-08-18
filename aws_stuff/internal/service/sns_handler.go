package service

import (
	"context"
)

// HandleSNS logs and forwards SNS payload to another SQS queue via repository Send.
func (s *ProcessorService) HandleSNS(ctx context.Context, message string, forwardQueueURL string) error {
	if forwardQueueURL == "" {
		return nil
	}
	return s.sqs.Send(ctx, forwardQueueURL, message)
}
