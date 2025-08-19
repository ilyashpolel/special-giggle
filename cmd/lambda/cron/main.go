package main

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"aws_stuff/internal/aws"
	"aws_stuff/internal/config"
	"aws_stuff/internal/logger"
	"aws_stuff/internal/repository"
	"aws_stuff/internal/service"
)

func process(ctx context.Context, proc service.Processor, namespace string) error {
	return proc.RunTimerTask(ctx, namespace)
}

func handler(ctx context.Context) error {
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

	dynamoClient := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)
	snsClient := sns.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = cfg.LocalstackEndpoint != "" })
	cwClient := cloudwatch.NewFromConfig(awsCfg)

	proc := service.NewProcessorService(
		log,
		repository.NewDynamoRepo(dynamoClient, cfg.DynamoTable, cfg.DynamoReplicaTable),
		repository.NewSQSRepo(sqsClient),
		repository.NewSNSRepo(snsClient),
		repository.NewS3Repo(s3Client),
		repository.NewCloudWatchRepo(cwClient),
	)

	return process(ctx, proc, cfg.CloudWatchNamespace)
}

func main() { lambda.Start(handler) }
