package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
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

	sess, baseCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		return err
	}

	proc := service.NewProcessorService(
		log,
		repository.NewDynamoRepo(dynamodb.New(sess, baseCfg), cfg.DynamoTable, cfg.DynamoReplicaTable),
		repository.NewSQSRepo(sqs.New(sess, baseCfg)),
		repository.NewSNSRepo(sns.New(sess, baseCfg)),
		repository.NewS3Repo(s3.New(sess, baseCfg)),
		repository.NewCloudWatchRepo(cloudwatch.New(sess, baseCfg)),
	)

	return process(ctx, proc, event, log)
}

func main() { lambda.Start(handler) }
