package service

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"

	"aws_stuff/internal/repository/mocks"
)

func Test_RunTimerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	dyn := mocks.NewMockDynamoRepository(ctrl)
	cw := mocks.NewMockCloudWatchRepository(ctrl)
	svc := NewProcessorService(tl(), dyn, nil, nil, nil, cw)
	dyn.EXPECT().ScanCount(gomock.Any()).Return(int32(10), nil)
	cw.EXPECT().PutCountMetric(gomock.Any(), "ns", "ItemsCount", float64(10)).Return(nil)
	if err := svc.RunTimerTask(context.Background(), "ns"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}
