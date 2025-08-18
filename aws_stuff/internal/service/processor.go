package service

import (
	"aws_stuff/internal/repository"

	"go.uber.org/zap"
)

type ProcessorService struct {
	log    *zap.Logger
	dynamo repository.DynamoRepository
	sqs    repository.SQSRepository
	sns    repository.SNSRepository
	s3     repository.S3Repository
	cw     repository.CloudWatchRepository
}

func NewProcessorService(
	log *zap.Logger,
	dynamo repository.DynamoRepository,
	sqs repository.SQSRepository,
	sns repository.SNSRepository,
	s3 repository.S3Repository,
	cw repository.CloudWatchRepository,
) *ProcessorService {
	return &ProcessorService{log: log, dynamo: dynamo, sqs: sqs, sns: sns, s3: s3, cw: cw}
}
