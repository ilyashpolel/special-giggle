package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"go.uber.org/zap"

	"aws_stuff/internal/repository/mocks"
)

func tl() *zap.Logger { l, _ := zap.NewDevelopment(); return l }

func Test_parseSQSBody(t *testing.T) {
	good := map[string]string{"id": "1", "type": "x", "content": "hi"}
	b, _ := json.Marshal(good)
	if _, err := parseSQSBody(string(b)); err != nil {
		t.Fatalf("expected ok, got %v", err)
	}
	if _, err := parseSQSBody(""); err == nil {
		t.Fatalf("expected error for empty")
	}
	if _, err := parseSQSBody("{"); err == nil {
		t.Fatalf("expected error for invalid json")
	}
	if _, err := parseSQSBody("{} "); err == nil {
		t.Fatalf("expected error for missing fields")
	}
}

func Test_HandleSQS(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dyn := mocks.NewMockDynamoRepository(ctrl)
	svc := NewProcessorService(tl(), dyn, nil, nil, nil, nil)
	good := map[string]string{"id": "1", "type": "x", "content": "hi"}
	b, _ := json.Marshal(good)
	dyn.EXPECT().PutItem(gomock.Any(), gomock.Any()).Return(nil)
	if err := svc.HandleSQS(context.Background(), string(b)); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}
