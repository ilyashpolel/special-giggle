package main

import (
	"context"
	"testing"

	"aws_stuff/pkg/models"
)

type fakeCronProc struct{ called bool }

func (f *fakeCronProc) RunTimerTask(context.Context, string) error            { f.called = true; return nil }
func (f *fakeCronProc) HandleSNS(context.Context, string, string) error       { return nil }
func (f *fakeCronProc) HandleSQS(context.Context, string) error               { return nil }
func (f *fakeCronProc) HandleS3Object(context.Context, string, string) error  { return nil }
func (f *fakeCronProc) HandleStreamRecord(context.Context, models.Item) error { return nil }

func Test_process_cron(t *testing.T) {
	f := &fakeCronProc{}
	if err := process(context.Background(), f, "ns"); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if !f.called {
		t.Fatalf("expected called")
	}
}
