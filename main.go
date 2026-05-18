package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	// Configure the default logger to output structured JSON directly to stdout.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	urls := []string{
		"https://httpbin.org/post",             // succeeds
		"https://httpbin.org/status/503",       // server error → retried once
		"https://httpbin.org/status/422",       // client error → not retried
		"https://httpbin.org/delay/10",         // too slow → context timeout
		"https://thishostdoesnotexist.invalid", // DNS failure
	}

	// Initialize a reusable HTTP client optimized for high concurrency connection pooling.
	client := &http.Client{
		Timeout: 4 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// 20sec timeout, Clean up background timers immediately upon function exit.
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	// gctx is cancelled if any g.Go func returns a non-nil error.
	// We return nil from all of them so siblings are never cancelled.
	g, gctx := errgroup.WithContext(ctx)

	// Structure to hold the outcome of each URL check.
	type result struct {
		url      string
		status   int
		attempts int
		err      error
	}
	results := make([]result, len(urls))

	for i, url := range urls {
		g.Go(func() error {
			results[i] = func() result {
				body := []byte(`{"event":"test"}`)
				r := result{url: url}

				for attempt := 1; attempt <= 2; attempt++ {
					r.attempts = attempt

					status, err := post(gctx, client, url, body)
					r.status = status
					r.err = err

					// Exit loop if request succeeded or failed with a client-side 4xx error.
					if err == nil && status < 500 {
						break
					}

					// Exit loop immediately if this was the final attempt.
					if attempt == 2 {
						break
					}

					// Pause 500ms before retrying, or abort instantly if context times out.
					select {
					case <-time.After(500 * time.Millisecond):
					case <-gctx.Done():
						r.err = gctx.Err()
						return r
					}
				}
				return r
			}()
			return nil
		})
	}

	_ = g.Wait()

	for _, r := range results {
		attrs := []any{
			slog.String("url", r.url),
			slog.Int("status", r.status),
			slog.Int("attempts", r.attempts),
		}
		switch {
		case r.err != nil:
			slog.Error("failed", append(attrs, slog.String("error", r.err.Error()))...)
		case r.status >= 500:
			slog.Error("failed", append(attrs, slog.String("error", "server error"))...)
		case r.status >= 400:
			slog.Warn("client error", attrs...)
		default:
			slog.Info("ok", attrs...)
		}
	}
}

// post sends one HTTP POST and drains the response body before returning.
func post(ctx context.Context, client *http.Client, url string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// Drain before closing so the TCP connection returns to the pool clean and can be reused.
	// Otherwise, it will be left it a dirty state and the client will have to open a new connection
	// for the next request to the same host.
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}
