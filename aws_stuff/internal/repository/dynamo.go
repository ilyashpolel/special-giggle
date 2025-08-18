package repository

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"aws_stuff/pkg/models"
)

type DynamoRepo struct {
	db           *dynamodb.DynamoDB
	table        string
	replicaTable string
}

func NewDynamoRepo(db *dynamodb.DynamoDB, table string, replicaTable string) *DynamoRepo {
	return &DynamoRepo{db: db, table: table, replicaTable: replicaTable}
}

func (r *DynamoRepo) PutItem(ctx context.Context, item models.Item) error {
	av := map[string]*dynamodb.AttributeValue{
		"pk":         {S: aws.String(item.PK)},
		"sk":         {S: aws.String(item.SK)},
		"payload":    {S: aws.String(item.Payload)},
		"created_at": {S: aws.String(item.CreatedAt.Format(time.RFC3339))},
	}
	_, err := r.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.table),
		Item:      av,
	})
	return err
}

func (r *DynamoRepo) ScanCount(ctx context.Context) (int64, error) {
	out, err := r.db.ScanWithContext(ctx, &dynamodb.ScanInput{TableName: aws.String(r.table), Select: aws.String("COUNT")})
	if err != nil {
		return 0, err
	}
	return aws.Int64Value(out.Count), nil
}

func (r *DynamoRepo) ReplicateItem(ctx context.Context, item models.Item) error {
	if r.replicaTable == "" {
		return nil
	}
	av := map[string]*dynamodb.AttributeValue{
		"pk":         {S: aws.String(item.PK)},
		"sk":         {S: aws.String(item.SK)},
		"payload":    {S: aws.String(item.Payload)},
		"created_at": {S: aws.String(item.CreatedAt.Format(time.RFC3339))},
	}
	_, err := r.db.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.replicaTable),
		Item:      av,
	})
	return err
}
