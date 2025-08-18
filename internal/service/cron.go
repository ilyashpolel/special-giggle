package service

import "context"

// RunTimerTask scans count and publishes to CloudWatch.
func (s *ProcessorService) RunTimerTask(ctx context.Context, namespace string) error {
	count, err := s.dynamo.ScanCount(ctx)
	if err != nil {
		return err
	}
	return s.cw.PutCountMetric(ctx, namespace, "ItemsCount", float64(count))
}
