package main

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"aws_stuff/pkg/models"
)

type fakeProcS3 struct{ calls int }

func (f *fakeProcS3) HandleS3Object(context.Context, string, string) error  { f.calls++; return nil }
func (f *fakeProcS3) HandleSNS(context.Context, string, string) error       { return nil }
func (f *fakeProcS3) HandleSQS(context.Context, string) error               { return nil }
func (f *fakeProcS3) HandleStreamRecord(context.Context, models.Item) error { return nil }
func (f *fakeProcS3) RunTimerTask(context.Context, string) error            { return nil }

func Test_process_S3(t *testing.T) {
	f := &fakeProcS3{}
	event := events.S3Event{Records: []events.S3EventRecord{{S3: events.S3Entity{Bucket: events.S3Bucket{Name: "b"}, Object: events.S3Object{Key: "k"}}}}}
	if err := process(context.Background(), f, event, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("want 1, got %d", f.calls)
	}
}
