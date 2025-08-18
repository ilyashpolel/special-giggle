package repository

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type CloudWatchRepo struct {
	client *cloudwatch.CloudWatch
}

func NewCloudWatchRepo(client *cloudwatch.CloudWatch) *CloudWatchRepo {
	return &CloudWatchRepo{client: client}
}

func (r *CloudWatchRepo) PutCountMetric(ctx context.Context, namespace string, metricName string, value float64) error {
	_, err := r.client.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(namespace),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(metricName),
				Value:      aws.Float64(value),
				Unit:       aws.String("Count"),
			},
		},
	})
	return err
}
