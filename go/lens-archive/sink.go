package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Column order is a contract with `graze-lens-fold`'s sink.rs. TabSeparated
// carries no field names, so a column added or reordered on one side lands data
// in the wrong column on the other, silently.
const columns = "follower, rkey, followee, op, seq, created_at"

type edge struct {
	follower  string
	rkey      string
	followee  string
	op        string
	seq       uint64
	createdAt string
}

type clickhouseConfig struct {
	host     string
	port     int
	database string
	user     string
	password string
	secure   bool
	table    string
	timeout  time.Duration
}

func (c clickhouseConfig) baseURL() string {
	scheme := "http"
	if c.secure {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s:%d/", scheme, c.host, c.port)
}

type sink struct {
	cfg        clickhouseConfig
	http       *http.Client
	maxRetries int
	// Set only by tests, to point the sink at an httptest server.
	baseOverride string
}

func newSink(cfg clickhouseConfig, maxRetries int) *sink {
	return &sink{
		cfg:        cfg,
		http:       &http.Client{Timeout: cfg.timeout},
		maxRetries: maxRetries,
	}
}

// permanentErr marks a response the server will reject the same way forever —
// a schema mismatch, bad credentials, malformed rows. Retrying those just burns
// the budget that transient failures need.
type permanentErr struct{ err error }

func (e permanentErr) Error() string { return e.err.Error() }

// insert writes one batch, straight to the base table.
//
// Deliberately not through `follow_edges_buffer`: that buffer exists to coalesce
// the live tail's one-row-at-a-time inserts. A batch here is already tens of
// thousands of rows, so it gains nothing and loses visibility — buffered rows
// are invisible to a plain SELECT until the buffer flushes, and this job's
// output is read back by verification while it runs.
func (s *sink) insert(ctx context.Context, rows []edge) error {
	if len(rows) == 0 {
		return nil
	}

	var body bytes.Buffer
	body.Grow(len(rows) * 128)
	for _, r := range rows {
		body.WriteString(escapeTSV(r.follower))
		body.WriteByte('\t')
		body.WriteString(escapeTSV(r.rkey))
		body.WriteByte('\t')
		body.WriteString(escapeTSV(r.followee))
		body.WriteByte('\t')
		body.WriteString(r.op)
		body.WriteByte('\t')
		body.WriteString(strconv.FormatUint(r.seq, 10))
		body.WriteByte('\t')
		body.WriteString(escapeTSV(r.createdAt))
		body.WriteByte('\n')
	}

	payload := body.Bytes()

	q := url.Values{}
	q.Set("query", fmt.Sprintf("INSERT INTO %s.%s (%s) FORMAT TabSeparated",
		s.cfg.database, s.cfg.table, columns))
	// Server-side batching on top of ours. A bootstrap sustains a far higher
	// insert rate than the live tail, and this cluster's cost is driven by part
	// creation, not row count.
	q.Set("async_insert", "1")
	q.Set("wait_for_async_insert", "1")

	// Retried in process rather than escalated to a pod restart.
	//
	// A ~14-hour job over someone else's network will see transient TCP drops;
	// one did kill an earlier run after 5h35m with "use of closed network
	// connection". The checkpoint meant nothing was lost, but the Job spent one
	// of six restarts on a blip, and six of those would fail the whole archive
	// pass part-way through. A connection reset is not news, so it should not
	// reach the process boundary.
	var last error
	for attempt := 0; ; attempt++ {
		err := s.post(ctx, q, bytes.NewReader(payload))
		if err == nil {
			return nil
		}
		last = err

		var perm permanentErr
		if errors.As(err, &perm) || attempt >= s.maxRetries || ctx.Err() != nil {
			return last
		}

		wait := time.Duration(1<<attempt) * time.Second
		if wait > 30*time.Second {
			wait = 30 * time.Second
		}
		logf("insert failed (attempt %d/%d), retrying in %s: %v",
			attempt+1, s.maxRetries, wait, err)

		select {
		case <-ctx.Done():
			return last
		case <-time.After(wait):
		}
	}
}

func (s *sink) post(ctx context.Context, q url.Values, body io.Reader) error {
	base := s.cfg.baseURL()
	if s.baseOverride != "" {
		base = s.baseOverride
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"?"+q.Encode(), body)
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.cfg.user, s.cfg.password)
	req.Header.Set("Content-Type", "text/plain")

	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 600))
		err := fmt.Errorf("clickhouse insert failed (%s): %s", resp.Status, strings.TrimSpace(string(msg)))
		// 429 and 5xx are the server asking for time. Other 4xx mean the
		// request itself is wrong and will be wrong again next time.
		if resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests {
			return permanentErr{err}
		}
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// escapeTSV mirrors the escaping in graze-lens-fold's sink.rs.
//
// A DID or rkey should never contain a tab, but this is untrusted data from
// other people's servers, and one stray control character would shift every
// later column on that row rather than failing — a corrupted edge that looks
// perfectly well-formed on read.
func escapeTSV(v string) string {
	if !strings.ContainsAny(v, "\t\n\r\\") {
		return v
	}
	r := strings.NewReplacer(
		"\\", "\\\\",
		"\t", "\\t",
		"\n", "\\n",
		"\r", "\\r",
	)
	return r.Replace(v)
}
