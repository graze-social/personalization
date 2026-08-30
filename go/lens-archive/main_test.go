package main

import (
	"strings"
	"testing"

	"github.com/bluesky-social/jetstream"
)

func commit(did, rkey string, op jetstream.Operation, timeUS int64, rec map[string]any) jetstream.Event {
	return jetstream.Event{
		DID:    did,
		TimeUS: timeUS,
		Kind:   jetstream.KindCommit,
		Commit: &jetstream.Commit{
			Operation:  op,
			Collection: followCollection,
			Rkey:       rkey,
			Record:     rec,
		},
	}
}

// The version column decides which row survives a fold. The live tail and the
// PDS backfill both write microseconds since the epoch; if this job wrote
// Event.Seq (a counter around 2.5e10) instead, every archive row would sort
// five orders of magnitude below every live row and an archive delete would
// lose to an older create — resurrecting follows the user had removed.
func TestSeqIsEventTimeNotTheArchiveCounter(t *testing.T) {
	const timeUS = int64(1_788_000_000_000_000)
	ev := commit("did:plc:a", "rk1", jetstream.OpCreate, timeUS, map[string]any{"subject": "did:plc:b"})
	ev.Seq = 25_321_639_657 // the archive's monotonic counter

	e, ok := edgeFrom(&ev)
	if !ok {
		t.Fatal("expected a row")
	}
	if e.seq != uint64(timeUS) {
		t.Fatalf("seq = %d, want the event time %d", e.seq, timeUS)
	}
	if e.seq == ev.Seq {
		t.Fatal("seq must not be the archive counter")
	}
	// Sanity: the same clock the other writers use lands this century.
	if e.seq < 1_500_000_000_000_000 {
		t.Fatalf("seq %d is not microseconds since the epoch", e.seq)
	}
}

// A follow delete does not name its subject anywhere on the wire. That is the
// whole reason follow_edges is keyed on (follower, rkey); a delete must still
// produce a row, with an empty followee, or the unfollow is simply lost.
func TestDeleteKeepsRkeyAndEmptiesFollowee(t *testing.T) {
	ev := commit("did:plc:a", "rk9", jetstream.OpDelete, 1_788_000_000_000_000, nil)
	e, ok := edgeFrom(&ev)
	if !ok {
		t.Fatal("a delete must produce a row")
	}
	if e.op != "delete" || e.rkey != "rk9" || e.followee != "" {
		t.Fatalf("unexpected delete row: %+v", e)
	}
}

// createdAt is the record's own, not ingest time: on an archive pass every row
// is witnessed now, so ingest time would flatten years of history into one
// instant.
func TestCreatedAtPrefersTheRecord(t *testing.T) {
	ev := commit("did:plc:a", "rk1", jetstream.OpCreate, 1_788_000_000_000_000, map[string]any{
		"subject":   "did:plc:b",
		"createdAt": "2023-04-05T06:07:08.123Z",
	})
	e, _ := edgeFrom(&ev)
	if e.createdAt != "2023-04-05 06:07:08.123" {
		t.Fatalf("createdAt = %q, want the record's own value", e.createdAt)
	}
}

// Client-supplied timestamps are arbitrary text from other people's software.
// Outside the window the fold accepts, fall back to event time rather than
// storing a follow dated 1970 or 9999.
func TestImplausibleCreatedAtFallsBackToEventTime(t *testing.T) {
	for _, raw := range []string{"1970-01-01T00:00:00Z", "9999-01-01T00:00:00Z", "not-a-date"} {
		ev := commit("did:plc:a", "rk1", jetstream.OpCreate, 1_788_000_000_000_000, map[string]any{
			"subject":   "did:plc:b",
			"createdAt": raw,
		})
		e, ok := edgeFrom(&ev)
		if !ok {
			t.Fatalf("%s: expected a row", raw)
		}
		if !strings.HasPrefix(e.createdAt, "2026-") {
			t.Fatalf("%s: createdAt = %q, want the event-time fallback", raw, e.createdAt)
		}
	}
	// ...but a plausible date inside the window is kept, including one the
	// fold's 2020..=2100 window deliberately allows even though it is ahead of
	// now. Matching that window matters more than being stricter here.
	ev := commit("did:plc:a", "rk1", jetstream.OpCreate, 1_788_000_000_000_000, map[string]any{
		"subject": "did:plc:b", "createdAt": "2030-10-18T05:31:12Z",
	})
	e, _ := edgeFrom(&ev)
	if e.createdAt != "2030-10-18 05:31:12.000" {
		t.Fatalf("createdAt = %q, want the in-window value kept", e.createdAt)
	}
}

func TestNonFollowAndMalformedEventsAreSkipped(t *testing.T) {
	cases := map[string]jetstream.Event{
		"wrong collection": {
			DID: "did:plc:a", TimeUS: 1, Kind: jetstream.KindCommit,
			Commit: &jetstream.Commit{Operation: jetstream.OpCreate, Collection: "app.bsky.feed.post", Rkey: "r"},
		},
		"no rkey": {
			DID: "did:plc:a", TimeUS: 1, Kind: jetstream.KindCommit,
			Commit: &jetstream.Commit{Operation: jetstream.OpCreate, Collection: followCollection},
		},
		"no did": {
			TimeUS: 1, Kind: jetstream.KindCommit,
			Commit: &jetstream.Commit{Operation: jetstream.OpCreate, Collection: followCollection, Rkey: "r"},
		},
		"identity event": {DID: "did:plc:a", TimeUS: 1, Kind: jetstream.KindIdentity},
		"subject is not a did": {
			DID: "did:plc:a", TimeUS: 1, Kind: jetstream.KindCommit,
			Commit: &jetstream.Commit{
				Operation: jetstream.OpCreate, Collection: followCollection, Rkey: "r",
				Record: map[string]any{"subject": "not-a-did"},
			},
		},
	}
	for name, ev := range cases {
		if _, ok := edgeFrom(&ev); ok {
			t.Errorf("%s: expected the event to be skipped", name)
		}
	}
}

// An event with no timestamp has no version. Storing it would fold it to the
// epoch, where it loses to everything including rows it ought to replace — so
// it is dropped rather than written unversioned.
func TestEventWithoutATimestampIsDropped(t *testing.T) {
	ev := commit("did:plc:a", "rk1", jetstream.OpCreate, 0, map[string]any{"subject": "did:plc:b"})
	if _, ok := edgeFrom(&ev); ok {
		t.Fatal("an event with no time must be dropped")
	}
}

// TabSeparated carries no field names. One stray control character in
// untrusted input would shift every later column on that row and store a
// corrupted edge that reads back as well-formed.
func TestTSVEscapingProtectsColumnBoundaries(t *testing.T) {
	if got := escapeTSV("did:plc:ok"); got != "did:plc:ok" {
		t.Fatalf("clean value was altered: %q", got)
	}
	for _, bad := range []string{"a\tb", "a\nb", "a\rb", `a\b`} {
		got := escapeTSV(bad)
		if strings.ContainsAny(got, "\t\n\r") {
			t.Fatalf("escaped %q still contains a raw control character: %q", bad, got)
		}
	}
}

// The column list is a cross-language contract with sink.rs; TabSeparated has
// no field names to catch a reorder.
func TestColumnOrderMatchesTheRustSink(t *testing.T) {
	const want = "follower, rkey, followee, op, seq, created_at"
	if columns != want {
		t.Fatalf("columns = %q, want %q (must match graze-lens-fold sink.rs)", columns, want)
	}
}

func TestMicrosToClickHouseFormat(t *testing.T) {
	// UTC, not local: the column is DateTime64(3,'UTC') and the fold formats in
	// UTC, so a job running in a different zone must not shift every row.
	if got := microsToClickHouse(1_788_000_000_123_456); got != "2026-08-29 10:40:00.123" {
		t.Fatalf("microsToClickHouse = %q", got)
	}
}
