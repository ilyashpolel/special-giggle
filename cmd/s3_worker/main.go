package main

import (
	"context"
	"encoding/json"
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

// s3Event minimal for S3 notification
type s3Event struct {
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log, _ := logger.New(cfg.Environment)
	defer log.Sync()

	awsCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		log.Fatal("aws config", zap.Error(err))
	}

	db := dynamodb.NewFromConfig(awsCfg)
	sqsClient := sqs.NewFromConfig(awsCfg)
	snsClient := sns.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) { o.UsePathStyle = true })
	cwClient := cloudwatch.NewFromConfig(awsCfg)

	processor := service.NewProcessorService(
		log,
		repository.NewDynamoRepo(db, cfg.DynamoTable, cfg.DynamoReplicaTable),
		repository.NewSQSRepo(sqsClient),
		repository.NewSNSRepo(snsClient),
		repository.NewS3Repo(s3Client),
		repository.NewCloudWatchRepo(cwClient),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down s3 worker...")
		cancel()
		time.Sleep(time.Duration(cfg.ShutdownGracePeriodSeconds) * time.Second)
		os.Exit(0)
	}()

	queueURL := cfg.SQSQueueURL
	if queueURL == "" {
		log.Fatal("SQS_QUEUE_URL is required for S3 worker (S3 notifications via SQS)")
	}

	sqsRepo := repository.NewSQSRepo(sqsClient)

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
			var ev s3Event
			if err := json.Unmarshal([]byte(awsv2.ToString(m.Body)), &ev); err != nil {
				log.Error("parse s3 event", zap.Error(err))
				_ = sqsRepo.Delete(ctx, queueURL, awsv2.ToString(m.ReceiptHandle))
				continue
			}
			for _, r := range ev.Records {
				if err := processor.HandleS3Object(ctx, r.S3.Bucket.Name, r.S3.Object.Key); err != nil {
					log.Error("handle s3 object", zap.Error(err))
				}
			}
			_ = sqsRepo.Delete(ctx, queueURL, awsv2.ToString(m.ReceiptHandle))
		}
	}
}
