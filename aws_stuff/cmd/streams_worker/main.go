package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"go.uber.org/zap"

	"aws_stuff/internal/aws"
	"aws_stuff/internal/config"
	"aws_stuff/internal/logger"
	"aws_stuff/internal/repository"
	"aws_stuff/internal/service"
	"aws_stuff/pkg/models"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	if cfg.DynamoStreamARN == "" {
		panic("DYNAMO_STREAM_ARN is required")
	}
	log, _ := logger.New(cfg.Environment)
	defer log.Sync()

	sess, baseCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		log.Fatal("aws session", zap.Error(err))
	}

	db := dynamodb.New(sess, baseCfg)
	streams := dynamodbstreams.New(sess, baseCfg)
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
	streamsRepo := repository.NewStreamsRepo(streams)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Info("shutting down streams worker...")
		cancel()
		time.Sleep(time.Duration(cfg.ShutdownGracePeriodSeconds) * time.Second)
		os.Exit(0)
	}()

	desc, err := streamsRepo.DescribeStream(ctx, cfg.DynamoStreamARN)
	if err != nil {
		log.Fatal("describe stream", zap.Error(err))
	}
	if desc.StreamDescription == nil || len(desc.StreamDescription.Shards) == 0 {
		log.Warn("no shards in stream")
		return
	}
	shardID := *desc.StreamDescription.Shards[0].ShardId
	iterator, err := streamsRepo.GetShardIterator(ctx, shardID, cfg.DynamoStreamARN, dynamodbstreams.ShardIteratorTypeTrimHorizon, "")
	if err != nil {
		log.Fatal("get shard iterator", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		rec, err := streamsRepo.GetRecords(ctx, iterator)
		if err != nil {
			log.Error("get records", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}
		if len(rec.Records) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}
		for _, r := range rec.Records {
			if r.Dynamodb == nil || r.Dynamodb.NewImage == nil {
				continue
			}
			// very simplified conversion: expect keys pk, sk, payload
			img := r.Dynamodb.NewImage
			item := models.Item{
				PK:      awsString(img["pk"].S),
				SK:      awsString(img["sk"].S),
				Payload: awsString(img["payload"].S),
			}
			b, _ := json.Marshal(item)
			log.Info("stream record", zap.String("item", string(b)))
			_ = processor.HandleStreamRecord(ctx, item)
		}
		iterator = awsString(rec.NextShardIterator)
	}
}

func awsString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
