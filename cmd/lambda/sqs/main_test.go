package main

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"go.uber.org/zap"

	"aws_stuff/pkg/models"
)

type fakeProc struct{ sqsBodies []string }

func (f *fakeProc) HandleSQS(ctx context.Context, body string) error {
	f.sqsBodies = append(f.sqsBodies, body)
	return nil
}
func (f *fakeProc) HandleSNS(context.Context, string, string) error       { return nil }
func (f *fakeProc) HandleS3Object(context.Context, string, string) error  { return nil }
func (f *fakeProc) HandleStreamRecord(context.Context, models.Item) error { return nil }
func (f *fakeProc) RunTimerTask(context.Context, string) error            { return nil }

func Test_process_SQS(t *testing.T) {
	f := &fakeProc{}
	event := events.SQSEvent{Records: []events.SQSMessage{{Body: "a"}, {Body: "b"}}}
	if err := process(context.Background(), f, event, zap.NewNop()); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(f.sqsBodies) != 2 {
		t.Fatalf("want 2, got %d", len(f.sqsBodies))
	}
}
