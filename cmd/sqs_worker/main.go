package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	awsv1 "github.com/aws/aws-sdk-go/aws"
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

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log, _ := logger.New(cfg.Environment)
	defer log.Sync()

	sess, baseCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		log.Fatal("failed to create aws session", zap.Error(err))
	}

	db := dynamodb.New(sess, baseCfg)
	sqsClient := sqs.New(sess, baseCfg)
	snsClient := sns.New(sess, baseCfg)
	s3Client := s3.New(sess, baseCfg)
	cwClient := cloudwatch.New(sess, baseCfg)

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
			if err := processor.HandleSQS(ctx, awsv1.StringValue(m.Body)); err != nil {
				log.Error("process sqs", zap.Error(err))
				continue
			}
			_ = sqsRepo.Delete(ctx, queueURL, awsv1.StringValue(m.ReceiptHandle))
		}
	}
}
