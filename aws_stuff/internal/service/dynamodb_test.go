package service

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"aws_stuff/internal/repository/mocks"
	"aws_stuff/pkg/models"
)

func Test_SaveItem_CountItems(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dyn := mocks.NewMockDynamoRepository(ctrl)
	svc := NewProcessorService(tl(), dyn, nil, nil, nil, nil)

	itm := models.Item{PK: "x", SK: time.Now().UTC().Format(time.RFC3339Nano)}
	dyn.EXPECT().PutItem(gomock.Any(), itm).Return(nil)
	if err := svc.SaveItem(context.Background(), itm); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	dyn.EXPECT().ScanCount(gomock.Any()).Return(int64(5), nil)
	n, err := svc.CountItems(context.Background())
	if err != nil || n != 5 {
		t.Fatalf("unexpected: %v %d", err, n)
	}
}
