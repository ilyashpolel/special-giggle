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

// snsEnvelope minimal struct for SQS subscription payloads
// ref: SNS -> SQS delivers JSON with Message field
// https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html
type snsEnvelope struct {
	Message string `json:"Message"`
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
		log.Info("shutting down sns worker...")
		cancel()
		time.Sleep(time.Duration(cfg.ShutdownGracePeriodSeconds) * time.Second)
		os.Exit(0)
	}()

	inQueue := cfg.SQSQueueURL
	outQueue := cfg.SQSForwardQueueURL
	if inQueue == "" || outQueue == "" {
		log.Fatal("SQS_QUEUE_URL and SQS_FORWARD_QUEUE_URL are required")
	}

	sqsRepo := repository.NewSQSRepo(sqsClient)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msgs, err := sqsRepo.Receive(ctx, inQueue, 5, 30, 10)
		if err != nil {
			log.Error("receive sqs", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		for _, m := range msgs {
			if m.Body == nil || m.ReceiptHandle == nil {
				continue
			}
			var env snsEnvelope
			if err := json.Unmarshal([]byte(awsv2.ToString(m.Body)), &env); err != nil {
				log.Error("parse sns envelope", zap.Error(err))
				_ = sqsRepo.Delete(ctx, inQueue, awsv2.ToString(m.ReceiptHandle))
				continue
			}
			if err := processor.HandleSNS(ctx, env.Message, outQueue); err != nil {
				log.Error("process sns", zap.Error(err))
			}
			_ = sqsRepo.Delete(ctx, inQueue, awsv2.ToString(m.ReceiptHandle))
		}
	}
}
