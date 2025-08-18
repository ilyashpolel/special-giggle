package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// AppConfig contains all configuration for the application.
type AppConfig struct {
	Environment                string
	AWSRegion                  string
	LocalstackEndpoint         string
	DynamoTable                string
	DynamoReplicaTable         string
	DynamoStreamARN            string
	SQSQueueURL                string
	SQSForwardQueueURL         string
	SNSForwardTopicARN         string
	S3Bucket                   string
	TimerIntervalSeconds       int
	CloudWatchNamespace        string
	ShutdownGracePeriodSeconds int
}

func Load() (AppConfig, error) {
	v := viper.New()
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("ENVIRONMENT", "local")
	v.SetDefault("AWS_REGION", "us-east-1")
	v.SetDefault("LOCALSTACK_ENDPOINT", "http://localhost:4566")
	v.SetDefault("DYNAMO_TABLE", "app-items")
	v.SetDefault("DYNAMO_REPLICA_TABLE", "app-items-replica")
	v.SetDefault("DYNAMO_STREAM_ARN", "")
	v.SetDefault("SQS_QUEUE_URL", "")
	v.SetDefault("SQS_FORWARD_QUEUE_URL", "")
	v.SetDefault("SNS_FORWARD_TOPIC_ARN", "")
	v.SetDefault("S3_BUCKET", "")
	v.SetDefault("TIMER_INTERVAL_SECONDS", 30)
	v.SetDefault("CLOUDWATCH_NAMESPACE", "AppMetrics")
	v.SetDefault("SHUTDOWN_GRACE_PERIOD_SECONDS", 10)

	_ = v.ReadInConfig() // optional .env

	cfg := AppConfig{
		Environment:                v.GetString("ENVIRONMENT"),
		AWSRegion:                  v.GetString("AWS_REGION"),
		LocalstackEndpoint:         v.GetString("LOCALSTACK_ENDPOINT"),
		DynamoTable:                v.GetString("DYNAMO_TABLE"),
		DynamoReplicaTable:         v.GetString("DYNAMO_REPLICA_TABLE"),
		DynamoStreamARN:            v.GetString("DYNAMO_STREAM_ARN"),
		SQSQueueURL:                v.GetString("SQS_QUEUE_URL"),
		SQSForwardQueueURL:         v.GetString("SQS_FORWARD_QUEUE_URL"),
		SNSForwardTopicARN:         v.GetString("SNS_FORWARD_TOPIC_ARN"),
		S3Bucket:                   v.GetString("S3_BUCKET"),
		TimerIntervalSeconds:       v.GetInt("TIMER_INTERVAL_SECONDS"),
		CloudWatchNamespace:        v.GetString("CLOUDWATCH_NAMESPACE"),
		ShutdownGracePeriodSeconds: v.GetInt("SHUTDOWN_GRACE_PERIOD_SECONDS"),
	}

	// Basic validation
	if cfg.AWSRegion == "" {
		return AppConfig{}, fmt.Errorf("AWS_REGION is required")
	}

	if cfg.TimerIntervalSeconds <= 0 {
		cfg.TimerIntervalSeconds = 30
	}

	// Normalize grace period minimum
	if cfg.ShutdownGracePeriodSeconds < 5 {
		cfg.ShutdownGracePeriodSeconds = 5
	}

	return cfg, nil
}
