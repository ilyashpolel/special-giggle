package models

import "time"

// Item represents a record stored in DynamoDB.
type Item struct {
	PK        string    `json:"pk"`
	SK        string    `json:"sk"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

// SQSMessagePayload represents a JSON payload sent via SQS.
type SQSMessagePayload struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Content string `json:"content"`
}
