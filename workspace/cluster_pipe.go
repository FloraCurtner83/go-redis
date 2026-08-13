package main

import (
	"context"
	"time"
)

func setCmdsErr(cmds []Cmder, err error) {
	for _, cmd := range cmds {
		cmd.SetErr(err)
	}
}

func (c *ClusterClient) defaultProcessPipeline(ctx context.Context, cmds []Cmder) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if err := ctx.Err(); err != nil {
			setCmdsErr(cmds, err)
			return err
		}

		if attempt > 0 {
			select {
			case <-ctx.Done():
				setCmdsErr(cmds, ctx.Err())
				return ctx.Err()
			case <-time.After(c.retryBackoff(attempt)):
			}
		}

		err := c.pipeFn(ctx, cmds)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}

func (c *ClusterClient) defaultProcessTxPipeline(ctx context.Context, cmds []Cmder) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if err := ctx.Err(); err != nil {
			setCmdsErr(cmds, err)
			return err
		}

		if attempt > 0 {
			select {
			case <-ctx.Done():
				setCmdsErr(cmds, ctx.Err())
				return ctx.Err()
			case <-time.After(c.retryBackoff(attempt)):
			}
		}

		err := c.pipeFn(ctx, cmds)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return lastErr
}
