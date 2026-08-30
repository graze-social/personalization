package main

import (
	"bytes"
	"context"
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
	cfg  clickhouseConfig
	http *http.Client
}

func newSink(cfg clickhouseConfig) *sink {
	return &sink{
		cfg:  cfg,
		http: &http.Client{Timeout: cfg.timeout},
	}
}

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

	q := url.Values{}
	q.Set("query", fmt.Sprintf("INSERT INTO %s.%s (%s) FORMAT TabSeparated",
		s.cfg.database, s.cfg.table, columns))
	// Server-side batching on top of ours. A bootstrap sustains a far higher
	// insert rate than the live tail, and this cluster's cost is driven by part
	// creation, not row count.
	q.Set("async_insert", "1")
	q.Set("wait_for_async_insert", "1")

	return s.post(ctx, q, &body)
}

func (s *sink) post(ctx context.Context, q url.Values, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.baseURL()+"?"+q.Encode(), body)
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
		return fmt.Errorf("clickhouse insert failed (%s): %s", resp.Status, strings.TrimSpace(string(msg)))
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
