package main

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"aws_stuff/pkg/models"
)

type fakeProcSNS struct{ calls int }

func (f *fakeProcSNS) HandleSNS(context.Context, string, string) error       { f.calls++; return nil }
func (f *fakeProcSNS) HandleSQS(context.Context, string) error               { return nil }
func (f *fakeProcSNS) HandleS3Object(context.Context, string, string) error  { return nil }
func (f *fakeProcSNS) HandleStreamRecord(context.Context, models.Item) error { return nil }
func (f *fakeProcSNS) RunTimerTask(context.Context, string) error            { return nil }

func Test_process_SNS(t *testing.T) {
	f := &fakeProcSNS{}
	event := events.SNSEvent{Records: []events.SNSEventRecord{{SNS: events.SNSEntity{Message: "m"}}, {SNS: events.SNSEntity{Message: "m2"}}}}
	if err := process(context.Background(), f, event, "q"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if f.calls != 2 {
		t.Fatalf("want 2, got %d", f.calls)
	}
}
