package repository

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"aws_stuff/pkg/models"
)

type DynamoRepo struct {
	db           *dynamodb.Client
	table        string
	replicaTable string
}

func NewDynamoRepo(db *dynamodb.Client, table string, replicaTable string) *DynamoRepo {
	return &DynamoRepo{db: db, table: table, replicaTable: replicaTable}
}

func (r *DynamoRepo) PutItem(ctx context.Context, item models.Item) error {
	av := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: item.PK},
		"sk":         &types.AttributeValueMemberS{Value: item.SK},
		"payload":    &types.AttributeValueMemberS{Value: item.Payload},
		"created_at": &types.AttributeValueMemberS{Value: item.CreatedAt.Format(time.RFC3339)},
	}

	_, err := r.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.table),
		Item:      av,
	})
	return err
}

func (r *DynamoRepo) ScanCount(ctx context.Context) (int32, error) {
	out, err := r.db.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(r.table),
		Select:    types.SelectCount,
	})
	if err != nil {
		return 0, err
	}

	return out.Count, nil
}

func (r *DynamoRepo) ReplicateItem(ctx context.Context, item models.Item) error {
	if r.replicaTable == "" {
		return nil
	}

	av := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: item.PK},
		"sk":         &types.AttributeValueMemberS{Value: item.SK},
		"payload":    &types.AttributeValueMemberS{Value: item.Payload},
		"created_at": &types.AttributeValueMemberS{Value: item.CreatedAt.Format(time.RFC3339)},
	}

	_, err := r.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.replicaTable),
		Item:      av,
	})
	return err
}
