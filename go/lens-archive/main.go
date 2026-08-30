// graze-lens-archive replays Jetstream v2's sealed archive into `follow_edges`.
//
// `graze-lens-fold` only sees follows made since it connected, and walking
// accounts' PDSes one at a time got us a complete graph for 8,868 of them
// before rate limits made it the wrong tool. This is the bulk path: one
// filtered pass over the whole network's follow history.
//
// # Why Go, in an otherwise Rust service
//
// The `.jss` segment format is columnar, zstd-compressed with a rotating shared
// dictionary, and indexed by a footer. Bluesky ship a client that negotiates
// the plan, downloads and decodes segments in parallel, dedupes by seq, and
// handles dictionary rotation. Reimplementing that in Rust to avoid one Go
// binary would be a large piece of format code whose failure mode is silence:
// a decoder that handles the happy path and drops a percentage of records
// produces a subtly wrong graph that nothing reports. This job runs once per
// bootstrap; the live tail stays Rust.
//
// # The number space is the thing to get right
//
// `follow_edges` is a ReplacingMergeTree versioned by `seq`, so `seq` decides
// which row survives for a given (follower, rkey). Three writers feed it and
// they must share one clock:
//
//	live fold     seq = jetstream time_us   (microseconds since epoch)
//	PDS backfill  seq = the rkey's TID      (microseconds since epoch)
//	this job      seq = Event.TimeUS        (microseconds since epoch)
//
// Note what this is NOT: `Event.Seq`, the archive's monotonic event counter,
// which currently runs around 2.5e10 against the others' 1.8e15. Writing that
// would put every archive row five orders of magnitude below every live row, so
// an archive delete would always lose to an older create and quietly resurrect
// unfollowed edges. `Event.TimeUS` is the same wall clock the other two use.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bluesky-social/jetstream"
)

const followCollection = "app.bsky.graph.follow"

type config struct {
	host        string
	apiKey      string
	afterSeq    uint64
	beforeSeq   uint64
	batchRows   int
	concurrency int
	stripes     int
	dryRun      bool
	progressN   uint64
	// Restricts the pass to these DIDs. Two uses: verifying a decode against
	// accounts whose follow counts we already know from the PDS backfill, and
	// repairing a bounded set later without re-reading the whole archive.
	dids []string

	clickhouse clickhouseConfig
	cursorKey  string
	redisURL   string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Resume from the last checkpointed cursor unless one was passed
	// explicitly. A full pass is hours of downloading; a job that restarts from
	// zero on every eviction never finishes.
	checkpoint := newCheckpoint(cfg.redisURL, cfg.cursorKey)
	if cfg.afterSeq == 0 {
		saved, err := checkpoint.load(ctx)
		if err != nil {
			logf("checkpoint unreadable, starting from the beginning: %v", err)
		} else if saved > 0 {
			logf("resuming after seq %d", saved)
			cfg.afterSeq = saved
		}
	}

	opts := []jetstream.Option{
		jetstream.WithAPIKey(cfg.apiKey),
		jetstream.WithKinds([]jetstream.Kind{jetstream.KindCommit}),
		jetstream.WithCollection(followCollection),
		// Archive only. Without this the client cuts over to the live tail at
		// the tip and never returns, and this job would sit forever duplicating
		// what graze-lens-fold already writes.
		jetstream.WithSnapshotOnly(),
		jetstream.WithAfterSeq(cfg.afterSeq),
		jetstream.WithDownloadConcurrency(cfg.concurrency),
		jetstream.WithSegmentStripes(cfg.stripes),
		jetstream.WithBatchSize(cfg.batchRows),
	}
	if cfg.beforeSeq > 0 {
		opts = append(opts, jetstream.WithBeforeSeq(cfg.beforeSeq))
	}
	if len(cfg.dids) > 0 {
		opts = append(opts, jetstream.WithDIDs(cfg.dids))
		logf("restricted to %d did(s)", len(cfg.dids))
	}

	client, err := jetstream.Subscribe(cfg.host, opts...)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	defer client.Close()

	sink := newSink(cfg.clickhouse, envInt("LENS_ARCHIVE_INSERT_RETRIES", 6))
	logf("archive replay starting host=%s afterSeq=%d dryRun=%t", cfg.host, cfg.afterSeq, cfg.dryRun)

	// In dry-run over a DID set, fold the events the way ClickHouse will and
	// report the resulting follow count per account. That number is directly
	// comparable to what the PDS backfill recorded, which is the only
	// end-to-end check that the decode is actually correct rather than merely
	// producing plausible rows.
	verify := cfg.dryRun && len(cfg.dids) > 0
	folded := map[string]map[string]foldState{}

	var (
		rows      []edge
		seen      uint64
		written   uint64
		skipped   uint64
		lastLog   = time.Now()
		lastEmit  uint64
		startedAt = time.Now()
	)

	flush := func(cursor uint64) error {
		if len(rows) == 0 {
			return nil
		}
		if !cfg.dryRun {
			if err := sink.insert(ctx, rows); err != nil {
				return err
			}
		}
		written += uint64(len(rows))
		rows = rows[:0]
		// Checkpointed only after the rows are durable. The reverse order would
		// let a crash between them skip a batch permanently: the next run would
		// resume past events that were never written, and nothing downstream
		// would ever notice the hole.
		if !cfg.dryRun && cursor > 0 {
			if err := checkpoint.save(ctx, cursor); err != nil {
				logf("checkpoint save failed (continuing): %v", err)
			}
		}
		return nil
	}

	for batch, err := range client.Events(ctx) {
		if err != nil {
			// The client streams per-block errors as recoverable: the good
			// prefix is delivered, then the error. Abandoning the run on one bad
			// block would throw away hours of work, so log and keep going —
			// the checkpoint means a rerun re-reads only what is missing.
			logf("recoverable stream error: %v", err)
			continue
		}
		for _, ev := range batch.Events() {
			seen++
			e, ok := edgeFrom(&ev)
			if !ok {
				skipped++
				continue
			}
			if verify {
				byRkey, ok := folded[e.follower]
				if !ok {
					byRkey = map[string]foldState{}
					folded[e.follower] = byRkey
				}
				// Highest seq wins, exactly as the ReplacingMergeTree will.
				if prev, seen := byRkey[e.rkey]; !seen || e.seq >= prev.seq {
					byRkey[e.rkey] = foldState{op: e.op, seq: e.seq}
				}
				continue
			}
			rows = append(rows, e)
		}

		if len(rows) >= cfg.batchRows {
			if err := flush(batch.LastCursor()); err != nil {
				return fmt.Errorf("insert at cursor %d: %w", batch.LastCursor(), err)
			}
		}

		if written-lastEmit >= cfg.progressN || time.Since(lastLog) > 30*time.Second {
			rate := float64(written) / time.Since(startedAt).Seconds()
			logf("progress seen=%d written=%d skipped=%d cursor=%d rate=%.0f rows/s",
				seen, written, skipped, batch.LastCursor(), rate)
			lastEmit, lastLog = written, time.Now()
		}
	}

	if err := flush(0); err != nil {
		return fmt.Errorf("final insert: %w", err)
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	if verify {
		logf("--- fold result (creates surviving per account) ---")
		for _, did := range cfg.dids {
			live, deleted := 0, 0
			for _, st := range folded[did] {
				if st.op == "create" {
					live++
				} else {
					deleted++
				}
			}
			logf("  %s follows=%d unfollowed=%d", did, live, deleted)
		}
	}

	st := client.Stats()
	logf("archive replay complete seen=%d written=%d skipped=%d elapsed=%s stats=%+v",
		seen, written, skipped, time.Since(startedAt).Round(time.Second), st)
	return nil
}

// edgeFrom maps one commit event to a row, mirroring
// `graze-lens-fold`'s event.rs exactly. Divergence between the two is not a
// style question: the same (follower, rkey) written differently by the two
// paths would fold to whichever row happened to win, so the archive's history
// and the live tail's updates have to be the same shape.
func edgeFrom(ev *jetstream.Event) (edge, bool) {
	if ev.Kind != jetstream.KindCommit || ev.Commit == nil {
		return edge{}, false
	}
	c := ev.Commit
	if c.Collection != followCollection || c.Rkey == "" || ev.DID == "" {
		return edge{}, false
	}
	if ev.TimeUS <= 0 {
		// Without a timestamp there is no version, and an unversioned row is
		// worse than a missing one: it would fold to the epoch and lose to
		// everything, including rows it should replace.
		return edge{}, false
	}
	seq := uint64(ev.TimeUS)

	switch c.Operation {
	case jetstream.OpCreate, jetstream.OpUpdate:
		subject, _ := c.Record["subject"].(string)
		if !strings.HasPrefix(subject, "did:") {
			return edge{}, false
		}
		created := microsToClickHouse(seq)
		if raw, ok := c.Record["createdAt"].(string); ok {
			if norm, ok := normalizeTimestamp(raw); ok {
				// The record's own createdAt, not ingest time: on an archive
				// pass every row is witnessed now, so ingest time would flatten
				// years of follow history into one instant.
				created = norm
			}
		}
		return edge{
			follower:  ev.DID,
			rkey:      c.Rkey,
			followee:  subject,
			op:        "create",
			seq:       seq,
			createdAt: created,
		}, true

	case jetstream.OpDelete:
		// A follow delete does not name its subject anywhere on the wire, which
		// is the whole reason this table is keyed on (follower, rkey) rather
		// than (follower, followee).
		return edge{
			follower:  ev.DID,
			rkey:      c.Rkey,
			followee:  "",
			op:        "delete",
			seq:       seq,
			createdAt: microsToClickHouse(seq),
		}, true
	}
	return edge{}, false
}

// normalizeTimestamp mirrors the Rust `normalize_timestamp`, including its
// 2020..=2100 window. Client-supplied `createdAt` is arbitrary text from
// someone else's software; a record claiming 1970 or 9999 would otherwise be
// stored and skew any recency ordering built on this column.
func normalizeTimestamp(raw string) (string, bool) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return "", false
	}
	t = t.UTC()
	if y := t.Year(); y < 2020 || y > 2100 {
		return "", false
	}
	return t.Format("2006-01-02 15:04:05.000"), true
}

func microsToClickHouse(us uint64) string {
	sec := int64(us / 1_000_000)
	nsec := int64((us % 1_000_000) / 1_000 * 1_000_000)
	return time.Unix(sec, nsec).UTC().Format("2006-01-02 15:04:05.000")
}

func loadConfig() (config, error) {
	c := config{
		host:        envOr("JETSTREAM_HOST", "jetstream.us-west.bsky.network"),
		apiKey:      os.Getenv("JETSTREAM_ARCHIVE_TOKEN"),
		batchRows:   envInt("LENS_ARCHIVE_BATCH_ROWS", 50_000),
		concurrency: envInt("LENS_ARCHIVE_DOWNLOAD_CONCURRENCY", 8),
		stripes:     envInt("LENS_ARCHIVE_SEGMENT_STRIPES", 4),
		dryRun:      envBool("LENS_ARCHIVE_DRY_RUN", false),
		progressN:   uint64(envInt("LENS_ARCHIVE_PROGRESS_ROWS", 1_000_000)),
		afterSeq:    uint64(envInt64("LENS_ARCHIVE_AFTER_SEQ", 0)),
		beforeSeq:   uint64(envInt64("LENS_ARCHIVE_BEFORE_SEQ", 0)),
		dids:        envList("LENS_ARCHIVE_DIDS"),
		cursorKey:   envOr("LENS_ARCHIVE_CURSOR_KEY", "lens:archive:cursor"),
		redisURL:    os.Getenv("PERSONALIZATION_REDIS_URL"),
	}
	if c.apiKey == "" {
		return c, errors.New("JETSTREAM_ARCHIVE_TOKEN is required")
	}

	ch := clickhouseConfig{
		host:     os.Getenv("CLICKHOUSE_HOST"),
		port:     envInt("CLICKHOUSE_PORT", 8443),
		database: envOr("CLICKHOUSE_DATABASE", "default"),
		user:     envOr("CLICKHOUSE_USER", "default"),
		password: os.Getenv("CLICKHOUSE_PASSWORD"),
		secure:   envBool("CLICKHOUSE_SECURE", true),
		table:    envOr("LENS_ARCHIVE_TABLE", "follow_edges"),
		timeout:  time.Duration(envInt("LENS_ARCHIVE_INSERT_TIMEOUT_SECONDS", 120)) * time.Second,
	}
	if ch.host == "" && !c.dryRun {
		return c, errors.New("CLICKHOUSE_HOST is required unless LENS_ARCHIVE_DRY_RUN=true")
	}
	c.clickhouse = ch
	return c, nil
}

// foldState is one rkey's winning row, used only by verify mode.
type foldState struct {
	op  string
	seq uint64
}

func envList(k string) []string {
	raw := strings.TrimSpace(os.Getenv(k))
	if raw == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func logf(format string, args ...any) {
	fmt.Printf("%s "+format+"\n", append([]any{time.Now().UTC().Format(time.RFC3339)}, args...)...)
}
