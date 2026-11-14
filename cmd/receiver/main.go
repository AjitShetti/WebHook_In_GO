package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	secret := os.Getenv("WEBHOOK_SECRET")
	if secret == "" {
		log.Fatal("WEBHOOK_SECRET is required")
	}

	http.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "could not read the request body", http.StatusBadRequest)
			return
		}

		defer r.Body.Close()

		signatureHeader := r.Header.Get("X-Webhook-Signature")
		signatureText, ok := strings.CutPrefix(signatureHeader, "sha256=")
		if !ok {
			http.Error(w, "signature is missing or malformed", http.StatusUnauthorized)
			return
		}

		receivedSignature, err := hex.DecodeString(signatureText)
		if err != nil {
			http.Error(w, "signature is malformed", http.StatusUnauthorized)
			return
		}

		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		expectedSignature := mac.Sum(nil)

		if !hmac.Equal(receivedSignature, expectedSignature) {
			http.Error(w, "signature does not match", http.StatusUnauthorized)
			return
		}

		log.Printf("received event %s: %s", r.Header.Get("X-Webhook-ID"), string(body))
		w.WriteHeader(http.StatusNoContent)
	})

	log.Println("receiver listening on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
