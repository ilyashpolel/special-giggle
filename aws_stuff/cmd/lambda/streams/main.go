package main

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"

	"aws_stuff/internal/aws"
	"aws_stuff/internal/config"
	"aws_stuff/internal/logger"
	"aws_stuff/internal/repository"
	"aws_stuff/internal/service"
	"aws_stuff/pkg/models"
)

func process(ctx context.Context, proc service.Processor, event events.DynamoDBEvent) error {
	for _, rec := range event.Records {
		img := rec.Change.NewImage
		if img == nil {
			continue
		}
		item := models.Item{PK: img["pk"].String(), SK: img["sk"].String(), Payload: img["payload"].String()}
		_ = proc.HandleStreamRecord(ctx, item)
	}
	return nil
}

func handler(ctx context.Context, event events.DynamoDBEvent) error {
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

	return process(ctx, proc, event)
}

func main() { lambda.Start(handler) }
