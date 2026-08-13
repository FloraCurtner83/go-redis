package main

import (
	"context"
	"time"
)

type Cmder interface {
	Err() error
	SetErr(error)
}

type MyCmd struct {
	err error
}

func (c *MyCmd) Err() error {
	return c.err
}

func (c *MyCmd) SetErr(err error) {
	c.err = err
}

type ClusterOptions struct {
	MaxRedirects int
}

type ClusterClient struct {
	opt       ClusterOptions
	processFn func(ctx context.Context, cmd Cmder) error
	pipeFn    func(ctx context.Context, cmds []Cmder) error
	backoffFn func(attempt int) time.Duration
}

func (c *ClusterClient) retryBackoff(attempt int) time.Duration {
	if c.backoffFn != nil {
		return c.backoffFn(attempt)
	}
	return time.Duration(attempt) * 10 * time.Millisecond
}

func (c *ClusterClient) defaultProcess(ctx context.Context, cmd Cmder) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if err := ctx.Err(); err != nil {
			cmd.SetErr(err)
			return err
		}

		if attempt > 0 {
			select {
			case <-ctx.Done():
				cmd.SetErr(ctx.Err())
				return ctx.Err()
			case <-time.After(c.retryBackoff(attempt)):
			}
		}

		err := c.processFn(ctx, cmd)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}
