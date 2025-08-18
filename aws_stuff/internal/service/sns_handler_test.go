package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"

	"aws_stuff/internal/repository/mocks"
)

func Test_HandleSNS_NoForward(t *testing.T) {
	svc := NewProcessorService(tl(), nil, nil, nil, nil, nil)
	if err := svc.HandleSNS(context.Background(), "x", ""); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func Test_HandleSNS_Forward(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sqs := mocks.NewMockSQSRepository(ctrl)
	svc := NewProcessorService(tl(), nil, sqs, nil, nil, nil)
	sqs.EXPECT().Send(gomock.Any(), "q", "m").Return(nil)
	if err := svc.HandleSNS(context.Background(), "m", "q"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}
