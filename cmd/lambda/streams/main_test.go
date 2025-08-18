package main

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"

	"aws_stuff/pkg/models"
)

type fakeProcStreams struct{ calls int }

func (f *fakeProcStreams) HandleStreamRecord(context.Context, models.Item) error {
	f.calls++
	return nil
}
func (f *fakeProcStreams) HandleSNS(context.Context, string, string) error      { return nil }
func (f *fakeProcStreams) HandleSQS(context.Context, string) error              { return nil }
func (f *fakeProcStreams) HandleS3Object(context.Context, string, string) error { return nil }
func (f *fakeProcStreams) RunTimerTask(context.Context, string) error           { return nil }

func Test_process_streams(t *testing.T) {
	f := &fakeProcStreams{}
	attr := func(s string) events.DynamoDBAttributeValue { return events.NewStringAttribute(s) }
	event := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{{Change: events.DynamoDBStreamRecord{NewImage: map[string]events.DynamoDBAttributeValue{
		"pk": attr("p"), "sk": attr("s"), "payload": attr("x"),
	}}}}}
	if err := process(context.Background(), f, event); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if f.calls != 1 {
		t.Fatalf("want 1, got %d", f.calls)
	}
}
