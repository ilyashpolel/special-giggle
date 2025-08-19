package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
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

	awsCfg, err := aws.NewSession(cfg.AWSRegion, cfg.LocalstackEndpoint)
	if err != nil {
		log.Fatal("failed to load aws config", zap.Error(err))
	}

	db := dynamodb.NewFromConfig(awsCfg)
	streams := dynamodbstreams.NewFromConfig(awsCfg)
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
	iterator, err := streamsRepo.GetShardIterator(ctx, shardID, cfg.DynamoStreamARN, string(types.ShardIteratorTypeTrimHorizon), "")
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
				PK:      img["pk"].(*types.AttributeValueMemberS).Value,
				SK:      img["sk"].(*types.AttributeValueMemberS).Value,
				Payload: img["payload"].(*types.AttributeValueMemberS).Value,
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
