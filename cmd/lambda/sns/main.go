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
)

func process(ctx context.Context, proc service.Processor, event events.SNSEvent, forwardQueueURL string) error {
	for _, rec := range event.Records {
		if err := proc.HandleSNS(ctx, rec.SNS.Message, forwardQueueURL); err != nil {
			return err
		}
	}
	return nil
}

func handler(ctx context.Context, event events.SNSEvent) error {
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

	return process(ctx, proc, event, cfg.SQSForwardQueueURL)
}

func main() { lambda.Start(handler) }
