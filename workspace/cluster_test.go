package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClusterContextTimeout(t *testing.T) {
	client := &ClusterClient{ 
		opt: ClusterOptions{
			MaxRedirects: 3,
		},
		processFn: func(ctx context.Context, cmd Cmder) error {
			return errors.New("redis: MOVED")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client.backoffFn = func(attempt int) time.Duration {
		return 100 * time.Millisecond
	}

	cmd := &MyCmd{}
	err := client.defaultProcess(ctx, cmd)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if !errors.Is(cmd.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected command error to be context.DeadlineExceeded, got %v", cmd.Err())
	}
}

func TestClusterContextCancellation(t *testing.T) {
	client := &ClusterClient{
		opt: ClusterOptions{
			MaxRedirects: 3,
		},
		processFn: func(ctx context.Context, cmd Cmder) error {
			return errors.New("redis: MOVED")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	client.backoffFn = func(attempt int) time.Duration {
		return 100 * time.Millisecond
	}

	cmd := &MyCmd{}
	
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := client.defaultProcess(ctx, cmd)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if !errors.Is(cmd.Err(), context.Canceled) {
		t.Fatalf("expected command error to be context.Canceled, got %v", cmd.Err())
	}
}

func TestClusterPipelineContextTimeout(t *testing.T) {
	client := &ClusterClient{
		opt: ClusterOptions{
			MaxRedirects: 3,
		},
		pipeFn: func(ctx context.Context, cmds []Cmder) error {
			return errors.New("redis: MOVED")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client.backoffFn = func(attempt int) time.Duration {
		return 100 * time.Millisecond
	}

	cmds := []Cmder{&MyCmd{}}
	err := client.defaultProcessPipeline(ctx, cmds)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if !errors.Is(cmds[0].Err(), context.DeadlineExceeded) {
		t.Fatalf("expected command error to be context.DeadlineExceeded, got %v", cmds[0].Err())
	}
}

func TestClusterTxPipelineContextTimeout(t *testing.T) {
	client := &ClusterClient{
		opt: ClusterOptions{
			MaxRedirects: 3,
		},
		pipeFn: func(ctx context.Context, cmds []Cmder) error {
			return errors.New("redis: MOVED")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client.backoffFn = func(attempt int) time.Duration {
		return 100 * time.Millisecond
	}

	cmds := []Cmder{&MyCmd{}}
	err := client.defaultProcessTxPipeline(ctx, cmds)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if !errors.Is(cmds[0].Err(), context.DeadlineExceeded) {
		t.Fatalf("expected command error to be context.DeadlineExceeded, got %v", cmds[0].Err())
	}
}
