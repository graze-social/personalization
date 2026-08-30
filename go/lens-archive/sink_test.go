package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A transient failure must be absorbed in process. Escalating it exits the
// job, and six of those fail the whole archive pass part-way through.
func TestInsertRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newSink(clickhouseConfig{timeout: 5 * time.Second, table: "t", database: "d"}, 5)
	s.http = srv.Client()
	s.baseOverride = srv.URL + "/"

	if err := s.insert(context.Background(), []edge{{follower: "did:plc:a", rkey: "r", op: "create", seq: 1}}); err != nil {
		t.Fatalf("expected the retry to succeed, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3 (two failures then success)", got)
	}
}

// A request the server will reject identically forever must not be retried —
// that only burns the budget transient failures need, and delays the real error.
func TestInsertDoesNotRetryPermanentFailures(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	s := newSink(clickhouseConfig{timeout: 5 * time.Second, table: "t", database: "d"}, 5)
	s.http = srv.Client()
	s.baseOverride = srv.URL + "/"

	err := s.insert(context.Background(), []edge{{follower: "did:plc:a", rkey: "r", op: "create", seq: 1}})
	if err == nil {
		t.Fatal("expected a permanent failure to surface")
	}
	var perm permanentErr
	if !errors.As(err, &perm) {
		t.Fatalf("error was not classified permanent: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1 (no retries on a permanent error)", got)
	}
}

// 429 is the server asking for time, not a malformed request.
func TestTooManyRequestsIsRetryable(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newSink(clickhouseConfig{timeout: 5 * time.Second, table: "t", database: "d"}, 3)
	s.http = srv.Client()
	s.baseOverride = srv.URL + "/"

	if err := s.insert(context.Background(), []edge{{follower: "did:plc:a", rkey: "r", op: "create", seq: 1}}); err != nil {
		t.Fatalf("429 should have been retried: %v", err)
	}
}

// The payload must survive being re-sent. A retry that replays a consumed
// reader writes an empty batch and reports success — losing rows silently,
// which is worse than the failure it was recovering from.
func TestRetryResendsTheFullBody(t *testing.T) {
	var sizes []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		sizes = append(sizes, n)
		if len(sizes) < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newSink(clickhouseConfig{timeout: 5 * time.Second, table: "t", database: "d"}, 3)
	s.http = srv.Client()
	s.baseOverride = srv.URL + "/"

	rows := []edge{
		{follower: "did:plc:a", rkey: "r1", followee: "did:plc:b", op: "create", seq: 1, createdAt: "2026-01-01 00:00:00.000"},
		{follower: "did:plc:c", rkey: "r2", followee: "did:plc:d", op: "create", seq: 2, createdAt: "2026-01-01 00:00:00.000"},
	}
	if err := s.insert(context.Background(), rows); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(sizes) != 2 || sizes[0] == 0 || sizes[0] != sizes[1] {
		t.Fatalf("retry did not resend an identical body: %v", sizes)
	}
}
