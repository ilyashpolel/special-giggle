package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"

	"aws_stuff/internal/repository/mocks"
)

func Test_HandleS3Object(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dyn := mocks.NewMockDynamoRepository(ctrl)
	s3 := mocks.NewMockS3Repository(ctrl)
	svc := NewProcessorService(tl(), dyn, nil, nil, s3, nil)
	s3.EXPECT().GetObjectString(gomock.Any(), "b", "k").Return("content", nil)
	dyn.EXPECT().PutItem(gomock.Any(), gomock.Any()).Return(nil)
	if err := svc.HandleS3Object(context.Background(), "b", "k"); err != nil {
		t.Fatalf("unexpected %v", err)
	}
}
