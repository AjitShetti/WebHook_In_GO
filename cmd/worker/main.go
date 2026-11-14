package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

const (
	maxAttempts   = 5
	breakerLimit  = 5
	breakerPeriod = 30 * time.Second
	streamName    = "webhook:deliveries"
	consumerGroup = "webhook-workers"
	consumerName  = "worker-1"
)

type endpointBreaker struct {
	mu        sync.Mutex
	failures  int
	openUntil time.Time
}

type deliveryMetrics struct {
	successes        atomic.Int64
	failures         atomic.Int64
	retries          atomic.Int64
	deadLetters      atomic.Int64
	breakerDeferrals atomic.Int64
}

var metrics deliveryMetrics

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	databaseURL := os.Getenv("DATABASE_URL")
	subscriberURL := os.Getenv("SUBSCRIBER_URL")
	secret := os.Getenv("WEBHOOK_SECRET")
	if databaseURL == "" || subscriberURL == "" || secret == "" {
		slog.Error("required environment variable is missing")
		os.Exit(1)
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		fatal(err)
	}
	defer db.Close()

	redisAddress := os.Getenv("REDIS_ADDR")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	defer redisClient.Close()

	startupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(startupContext); err != nil {
		cancel()
		fatal(err)
	}
	if err := redisClient.Ping(startupContext).Err(); err != nil {
		cancel()
		fatal(err)
	}
	if err := ensureConsumerGroup(startupContext, redisClient); err != nil {
		cancel()
		fatal(err)
	}
	cancel()

	go serveMetrics()

	httpClient := &http.Client{Timeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("worker started", "consumer", consumerName)

	subscriberLimiter := rate.NewLimiter(rate.Limit(5), 10)
	breaker := &endpointBreaker{}

	for ctx.Err() == nil {
		if err := runConsumer(ctx, db, redisClient, httpClient, subscriberLimiter, breaker, subscriberURL, secret); err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("worker error", "error", err)
			timer := time.NewTimer(2 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
	slog.Info("worker stopped")
}

func fatal(err error) {
	slog.Error("worker startup failed", "error", err)
	os.Exit(1)
}

func serveMetrics() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprintf(w, "webhook_delivery_success_total %d\n", metrics.successes.Load())
		fmt.Fprintf(w, "webhook_delivery_failure_total %d\n", metrics.failures.Load())
		fmt.Fprintf(w, "webhook_delivery_retry_total %d\n", metrics.retries.Load())
		fmt.Fprintf(w, "webhook_dead_letter_total %d\n", metrics.deadLetters.Load())
		fmt.Fprintf(w, "webhook_circuit_breaker_deferral_total %d\n", metrics.breakerDeferrals.Load())
	})
	if err := http.ListenAndServe(":9090", mux); err != nil && err != http.ErrServerClosed {
		slog.Error("metrics server stopped", "error", err)
	}
}

func ensureConsumerGroup(ctx context.Context, client *redis.Client) error {
	err := client.XGroupCreateMkStream(ctx, streamName, consumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func runConsumer(
	ctx context.Context,
	db *sql.DB,
	redisClient *redis.Client,
	httpClient *http.Client,
	limiter *rate.Limiter,
	breaker *endpointBreaker,
	subscriberURL string,
	secret string,
) error {

	pending, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: consumerName,
		Streams:  []string{streamName, "0"},
		Count:    10,
	}).Result()
	if err == nil {
		hasPendingMessages := false
		for _, stream := range pending {
			if len(stream.Messages) > 0 {
				hasPendingMessages = true
				break
			}
		}
		if hasPendingMessages {
			return processStreams(ctx, db, redisClient, httpClient, limiter, breaker, subscriberURL, secret, pending)
		}
	} else if !errors.Is(err, redis.Nil) {
		return err
	}
	streams, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil // No new message arrived during the wait.
	}
	if err != nil {
		return err
	}

	return processStreams(ctx, db, redisClient, httpClient, limiter, breaker, subscriberURL, secret, streams)
}

func processStreams(
	ctx context.Context,
	db *sql.DB,
	redisClient *redis.Client,
	httpClient *http.Client,
	limiter *rate.Limiter,
	breaker *endpointBreaker,
	subscriberURL string,
	secret string,
	streams []redis.XStream,
) error {
	for _, stream := range streams {
		for _, message := range stream.Messages {
			eventID, ok := message.Values["event_id"].(string)
			if !ok {
				return fmt.Errorf("message %s has no event_id", message.ID)
			}

			delivered, err := deliverOne(
				ctx, db, httpClient, limiter, breaker, eventID, subscriberURL, secret,
			)
			if errors.Is(err, sql.ErrNoRows) {

				err = nil
			}
			if err != nil {
				// Leave it unacknowledged. The same consumer will retry it.
				return fmt.Errorf("process event %s: %w", eventID, err)
			}

			// Acknowledge only after the delivery result or retry was saved in Postgres.
			if _, err := redisClient.XAck(ctx, streamName, consumerGroup, message.ID).Result(); err != nil {
				return fmt.Errorf("acknowledge message %s: %w", message.ID, err)
			}

			if delivered {
				slog.Info("event delivered", "event_id", eventID)
			}
		}
	}

	return nil
}

func deliverOne(
	ctx context.Context,
	db *sql.DB,
	client *http.Client,
	limiter *rate.Limiter,
	breaker *endpointBreaker,
	eventID string,
	subscriberURL string,
	secret string,
) (bool, error) {
	var payload []byte
	var failedAttempts int

	err := db.QueryRow(`
		SELECT payload, attempt_count
		FROM outbox
		WHERE id = $1
		  AND delivered_at IS NULL
		  AND next_attempt_at <= now()
	`, eventID).Scan(&payload, &failedAttempts)
	if err != nil {
		return false, err
	}
	if delay := breaker.remaining(); delay > 0 {
		_, err := db.Exec(`
			UPDATE outbox
			SET next_attempt_at = now() + $2 * interval '1 millisecond', published_at = NULL
			WHERE id = $1
		`, eventID, delay.Milliseconds())
		if err != nil {
			return false, fmt.Errorf("defer delivery while circuit is open: %w", err)
		}
		metrics.breakerDeferrals.Add(1)
		return false, nil
	}
	if err := limiter.Wait(ctx); err != nil {
		return false, fmt.Errorf("wait for rate limit: %w", err)
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		subscriberURL,
		bytes.NewReader(payload),
	)
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Webhook-ID", eventID)
	request.Header.Set("X-Webhook-Signature", "sha256="+sign(payload, secret))

	attemptStarted := time.Now()
	response, sendErr := client.Do(request)
	statusCode := 0
	if sendErr == nil {
		statusCode = response.StatusCode
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			sendErr = fmt.Errorf("subscriber returned HTTP %d", response.StatusCode)
		}
	}
	errorMessage := ""
	if sendErr != nil {
		errorMessage = sendErr.Error()
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO delivery_attempts (
			event_id, attempt_number, status_code, succeeded, error_message, duration_ms
		) VALUES ($1, $2, $3, $4, $5, $6)
	`, eventID, failedAttempts+1, statusCode, sendErr == nil, errorMessage, time.Since(attemptStarted).Milliseconds())
	if err != nil {
		return false, fmt.Errorf("record delivery attempt: %w", err)
	}
	if sendErr != nil {
		metrics.failures.Add(1)
		breaker.recordFailure()
	} else {
		breaker.recordSuccess()
	}

	if sendErr != nil {
		if err := recordFailure(db, eventID, failedAttempts+1, sendErr); err != nil {
			return false, err
		}
		return false, nil
	}

	_, err = db.Exec(`
		UPDATE outbox
		SET delivered_at = now(),
		    attempt_count = $2,
		    last_error = NULL
		WHERE id = $1 AND delivered_at IS NULL
	`, eventID, failedAttempts+1)
	if err != nil {
		return false, fmt.Errorf("mark event delivered: %w", err)
	}
	metrics.successes.Add(1)

	return true, nil
}

func recordFailure(db *sql.DB, id string, failedAttempts int, sendErr error) error {
	if failedAttempts >= maxAttempts {
		if err := moveToDeadLetters(db, id, failedAttempts, sendErr.Error()); err != nil {
			return err
		}
		slog.Warn("event moved to dead letters", "event_id", id, "attempts", failedAttempts)
		metrics.deadLetters.Add(1)
		return nil
	}

	delay := retryDelay(failedAttempts)
	_, err := db.Exec(`
		UPDATE outbox
		SET attempt_count = $2,
		    next_attempt_at = now() + $3 * interval '1 millisecond',
		    last_error = $4,
		    published_at = NULL
		WHERE id = $1
	`, id, failedAttempts, delay.Milliseconds(), sendErr.Error())
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}

	slog.Warn("event failed; retry scheduled", "event_id", id, "delay", delay, "error", sendErr)
	metrics.retries.Add(1)
	return nil
}

func (b *endpointBreaker) remaining() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := time.Until(b.openUntil)
	if remaining <= 0 {
		b.openUntil = time.Time{}
		b.failures = 0
		return 0
	}
	return remaining
}

func (b *endpointBreaker) recordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= breakerLimit {
		b.openUntil = time.Now().Add(breakerPeriod)
		b.failures = 0
		slog.Warn("subscriber circuit opened", "duration", breakerPeriod)
	}
}

func (b *endpointBreaker) recordSuccess() {
	b.mu.Lock()
	b.failures = 0
	b.openUntil = time.Time{}
	b.mu.Unlock()
}

func moveToDeadLetters(db *sql.DB, id string, attempts int, lastError string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO dead_letters (
			event_id, event_type, payload, created_at, attempts, last_error
		)
		SELECT id, event_type, payload, created_at, $2, $3
		FROM outbox
		WHERE id = $1
	`, id, attempts, lastError)
	if err != nil {
		return fmt.Errorf("insert dead letter: %w", err)
	}

	_, err = tx.Exec(`DELETE FROM outbox WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("remove event from outbox: %w", err)
	}

	return tx.Commit()
}

func retryDelay(failedAttempts int) time.Duration {
	delay := time.Second
	for i := 1; i < failedAttempts && delay < time.Minute; i++ {
		delay *= 2
	}
	if delay > time.Minute {
		delay = time.Minute
	}

	jitter := time.Duration(rand.Int63n(int64(delay/4) + 1))
	delay += jitter
	if delay > time.Minute {
		delay = time.Minute
	}
	return delay
}

func sign(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}
