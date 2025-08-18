package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
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

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log, _ := logger.New(cfg.Environment)
	defer log.Sync()

	awsCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		log.Fatal("failed to load aws config", zap.Error(err))
	}

	db := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)
	snsClient := sns.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = true })
	cwClient := cloudwatch.NewFromConfig(awsCfg)

	dynamoRepo := repository.NewDynamoRepo(db, cfg.DynamoTable, cfg.DynamoReplicaTable)
	sqsRepo := repository.NewSQSRepo(sqsClient)
	snsRepo := repository.NewSNSRepo(snsClient)
	s3Repo := repository.NewS3Repo(s3Client)
	cwRepo := repository.NewCloudWatchRepo(cwClient)

	processor := service.NewProcessorService(log, dynamoRepo, sqsRepo, snsRepo, s3Repo, cwRepo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down sqs worker...")
		cancel()
		time.Sleep(time.Duration(cfg.ShutdownGracePeriodSeconds) * time.Second)
		os.Exit(0)
	}()

	queueURL := cfg.SQSQueueURL
	if queueURL == "" {
		log.Fatal("SQS_QUEUE_URL is required")
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := sqsRepo.Receive(ctx, queueURL, 5, 30, 10)
		if err != nil {
			log.Error("receive sqs", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		for _, m := range msgs {
			if m.Body == nil || m.ReceiptHandle == nil {
				continue
			}
			if err := processor.HandleSQS(ctx, awsv2.ToString(m.Body)); err != nil {
				log.Error("process sqs", zap.Error(err))
				continue
			}
			_ = sqsRepo.Delete(ctx, queueURL, awsv2.ToString(m.ReceiptHandle))
		}
	}
}
