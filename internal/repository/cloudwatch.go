package repository

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

type CloudWatchRepo struct {
	client *cloudwatch.Client
}

func NewCloudWatchRepo(client *cloudwatch.Client) *CloudWatchRepo {
	return &CloudWatchRepo{client: client}
}

func (r *CloudWatchRepo) PutCountMetric(ctx context.Context, namespace string, metricName string, value float64) error {
	_, err := r.client.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String(namespace),
		MetricData: []types.MetricDatum{
			{
				MetricName: aws.String(metricName),
				Value:      aws.Float64(value),
				Unit:       types.StandardUnitCount,
			},
		},
	})
	return err
}
