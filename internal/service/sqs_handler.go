package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"aws_stuff/pkg/models"
)

func parseSQSBody(body string) (models.SQSMessagePayload, error) {
	var p models.SQSMessagePayload
	if strings.TrimSpace(body) == "" {
		return p, errors.New("empty body")
	}
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		return p, err
	}
	if p.ID == "" || p.Content == "" {
		return p, errors.New("missing fields")
	}
	return p, nil
}

// HandleSQS processes a raw SQS message body, stores it to DynamoDB.
func (s *ProcessorService) HandleSQS(ctx context.Context, body string) error {
	payload, err := parseSQSBody(body)
	if err != nil {
		return err
	}
	item := models.Item{
		PK:        "ITEM#" + payload.ID,
		SK:        time.Now().UTC().Format(time.RFC3339Nano),
		Payload:   payload.Content,
		CreatedAt: time.Now().UTC(),
	}
	return s.dynamo.PutItem(ctx, item)
}
