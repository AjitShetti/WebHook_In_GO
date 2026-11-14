package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redis "github.com/redis/go-redis/v9"
)

const streamName = "webhook:deliveries"

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	redisAddress := os.Getenv("REDIS_ADDR")
	if redisAddress == "" {
		redisAddress = "localhost:6379"
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	defer redisClient.Close()

	startupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(startupContext); err != nil {
		log.Fatal(err)
	}

	if err := redisClient.Ping(startupContext).Err(); err != nil {
		log.Fatal(err)
	}

	log.Println("diapatcher started")

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		published, err := publishOne(ctx, db, redisClient)
		cancel()

		if errors.Is(err, sql.ErrNoRows) {
			time.Sleep(time.Second)
			continue
		}

		if err != nil {
			log.Printf("publish event: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if published {
			log.Println("Published one event")
		}
	}
}

func publishOne(
	ctx context.Context,
	db *sql.DB,
	redisClient *redis.Client,
) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var eventID string
	err = tx.QueryRowContext(ctx, `
	SELECT id::text
		FROM outbox
		WHERE delivered_at IS NULL
		  AND published_at IS NULL
		  AND next_attempt_at <= now()
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
		`).Scan(&eventID)
	if err != nil {
		return false, err
	}

	_, err = redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{
			"event_id": eventID,
		},
	}).Result()
	if err != nil {
		return false, err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE outbox
		SET published_at = now()
		WHERE id = $1
		`, eventID)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}
