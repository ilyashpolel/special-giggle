package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/zap"

	"aws_stuff/internal/aws"
	"aws_stuff/internal/config"
	"aws_stuff/internal/logger"
	"aws_stuff/internal/repository"
	"aws_stuff/internal/service"
)

func process(ctx context.Context, proc service.Processor, event events.S3Event, log *zap.Logger) error {
	for _, rec := range event.Records {
		if err := proc.HandleS3Object(ctx, rec.S3.Bucket.Name, rec.S3.Object.Key); err != nil {
			b, _ := json.Marshal(rec)
			log.Error("S3 handle error", zap.String("record", string(b)))
		}
	}
	return nil
}

func handler(ctx context.Context, event events.S3Event) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log, _ := logger.New(cfg.Environment)
	defer log.Sync()

	awsCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		return err
	}

	proc := service.NewProcessorService(
		log,
		repository.NewDynamoRepo(dynamodb.NewFromConfig(awsCfg), cfg.DynamoTable, cfg.DynamoReplicaTable),
		repository.NewSQSRepo(sqs.NewFromConfig(awsCfg)),
		repository.NewSNSRepo(sns.NewFromConfig(awsCfg)),
		repository.NewS3Repo(s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = cfg.LocalstackEndpoint != "" })),
		repository.NewCloudWatchRepo(cloudwatch.NewFromConfig(awsCfg)),
	)

	return process(ctx, proc, event, log)
}

func main() { lambda.Start(handler) }
