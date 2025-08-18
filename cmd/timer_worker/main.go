package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		log.Fatal("aws session", zap.Error(err))
	}

	db := dynamodb.New(sess, baseCfg)
	sqsClient := sqs.New(sess, baseCfg)
	snsClient := sns.New(sess, baseCfg)
	s3Client := s3.New(sess, baseCfg)
	cwClient := cloudwatch.New(sess, baseCfg)

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
		log.Info("shutting down timer worker...")
		cancel()
		time.Sleep(time.Duration(cfg.ShutdownGracePeriodSeconds) * time.Second)
		os.Exit(0)
	}()

	ticker := time.NewTicker(time.Duration(cfg.TimerIntervalSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := processor.RunTimerTask(ctx, cfg.CloudWatchNamespace); err != nil {
				log.Error("timer task", zap.Error(err))
			}
		}
	}
}
