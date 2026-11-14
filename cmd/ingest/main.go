package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/jackc/pgx/v5/stdlib"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type ingestRequest struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(w http.ResponseWriter, r *http.Request) {
		handleCreateEvent(db, w, r)
	})

	mux.HandleFunc("POST /dead-letters/{id}/replay", func(w http.ResponseWriter, r *http.Request) {
		handleReplay(db, w, r)
	})
	mux.HandleFunc("GET /admin/attempts", func(w http.ResponseWriter, r *http.Request) {
		handleAttemptsPage(db, w, r)
	})

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("ingest listening on :%s", port)
	log.Fatal(server.ListenAndServe())
}

type deliveryAttempt struct {
	EventID      string
	Attempt      int
	AttemptedAt  time.Time
	StatusCode   int
	Succeeded    bool
	ErrorMessage string
	DurationMS   int64
}

var attemptsPage = template.Must(template.New("attempts").Parse(`<!doctype html>
<html><head><meta charset="utf-8"><title>Webhook delivery attempts</title>
<style>body{font:15px system-ui;margin:2rem;color:#17202a}table{border-collapse:collapse;width:100%}th,td{padding:.55rem;border-bottom:1px solid #ddd;text-align:left}code{font-size:.9em}.ok{color:#087443}.failed{color:#b42318}</style></head>
<body><h1>Recent webhook delivery attempts</h1><p>Showing the latest 100 requests sent to subscribers.</p>
<table><thead><tr><th>Event</th><th>Attempt</th><th>Time (UTC)</th><th>HTTP</th><th>Result</th><th>Duration</th><th>Error</th></tr></thead><tbody>
{{range .}}<tr><td><code>{{.EventID}}</code></td><td>{{.Attempt}}</td><td>{{.AttemptedAt.UTC.Format "2006-01-02 15:04:05"}}</td><td>{{if .StatusCode}}{{.StatusCode}}{{else}}—{{end}}</td><td>{{if .Succeeded}}<span class="ok">success</span>{{else}}<span class="failed">failed</span>{{end}}</td><td>{{.DurationMS}} ms</td><td>{{.ErrorMessage}}</td></tr>{{else}}<tr><td colspan="7">No delivery attempts yet.</td></tr>{{end}}
</tbody></table></body></html>`))

func handleAttemptsPage(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	rows, err := db.QueryContext(r.Context(), `
		SELECT event_id::text, attempt_number, attempted_at, status_code,
		       succeeded, error_message, duration_ms
		FROM delivery_attempts
		ORDER BY attempted_at DESC
		LIMIT 100
	`)
	if err != nil {
		http.Error(w, "could not load delivery attempts; apply migration 005", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var attempts []deliveryAttempt
	for rows.Next() {
		var attempt deliveryAttempt
		if err := rows.Scan(
			&attempt.EventID, &attempt.Attempt, &attempt.AttemptedAt,
			&attempt.StatusCode, &attempt.Succeeded, &attempt.ErrorMessage,
			&attempt.DurationMS,
		); err != nil {
			http.Error(w, "could not read delivery attempts", http.StatusInternalServerError)
			return
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "could not read delivery attempts", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := attemptsPage.Execute(w, attempts); err != nil {
		log.Printf("render attempts page: %v", err)
	}
}

func handleCreateEvent(db *sql.DB, w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	defer r.Body.Close()

	var event ingestRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "request must contain valid event JSON", http.StatusBadRequest)
		return
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "request must contain one JSON value", http.StatusBadRequest)
		return
	}

	event.Type = strings.TrimSpace(event.Type)
	if event.Type == "" {
		http.Error(w, "event data is required", http.StatusBadRequest)
		return
	}
	if len(event.Data) == 0 {
		http.Error(w, "event data is required", http.StatusBadRequest)
		return
	}

	var id string

	err := db.QueryRowContext(
		r.Context(),
		`INSERT INTO outbox (event_type, payload)
		VALUES ($1, $2::jsonb)
		RETURNING id`,
		event.Type,
		string(event.Data),
	).Scan(&id)
	if err != nil {
		log.Printf("insert event: %v", err)
		http.Error(w, "could not store event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	if err := json.NewEncoder(w).Encode(map[string]string{
		"id":     id,
		"status": "accepted",
	}); err != nil {
		log.Printf("write response: %v", err)
	}

}

func handleReplay(
	db *sql.DB,
	w http.ResponseWriter,
	r *http.Request,
) {
	id := r.PathValue("id")

	if !looksLikeUUID(id) {
		http.Error(w, "event id must be a UUID", http.StatusBadRequest)
		return
	}

	tx, err := db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, "could not start replay", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	var replayID string

	err = tx.QueryRowContext(r.Context(), `INSERT INTO outbox (
			id, event_type, payload, created_at,
			delivered_at, attempt_count, next_attempt_at, last_error
		)
		SELECT
			event_id, event_type, payload, created_at,
			NULL, 0, now(), NULL
		FROM dead_letters
		WHERE event_id = $1
		RETURNING id::text
	`, id).Scan(&replayID)

	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "dead-letter event not found", http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, "could not replay event", http.StatusInternalServerError)
		return
	}

	_, err = tx.ExecContext(
		r.Context(),
		`DELETE FROM dead_letters WHERE event_id = $1`,
		id,
	)
	if err != nil {
		http.Error(w, "could not finish replay", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "could not finish replay", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"id":     replayID,
		"status": "replay scheduled",
	})
}

func looksLikeUUID(value string) bool {
	if len(value) != 36 {
		return false
	}

	for i, char := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if char != '-' {
				return false
			}
			continue
		}

		isDigit := char >= '0' && char <= '9'
		isLowerHex := char >= 'a' && char <= 'f'
		isUpperHex := char >= 'A' && char <= 'F'

		if !isDigit && !isLowerHex && !isUpperHex {
			return false
		}
	}
	return true
}
