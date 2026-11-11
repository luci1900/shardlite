package linear

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var lst = Key{"storage/a", "l1"}

// h builds a history from ops, giving each the next id.
func h(ops ...Op) []Op {
	for i := range ops {
		ops[i].ID = i
		if ops[i].Key == (Key{}) {
			ops[i].Key = lst
		}
	}
	return ops
}

func app(client int, v string, call, ret int64, o Outcome) Op {
	return Op{Client: client, Append: true, Value: v, Call: call, Return: ret, Outcome: o}
}

func read(client int, call, ret int64, seen ...string) Op {
	return Op{Client: client, Call: call, Return: ret, Seen: seen, Outcome: Known}
}

func TestCheckPasses(t *testing.T) {
	cases := map[string][]Op{
		"concurrent appends in order ab": h(
			app(1, "a", 1, 10, Known), app(2, "b", 2, 11, Known), read(3, 20, 21, "a", "b")),
		"concurrent appends in order ba": h(
			app(1, "a", 1, 10, Known), app(2, "b", 2, 11, Known), read(3, 20, 21, "b", "a")),
		"indeterminate append seen": h(
			app(1, "a", 1, 2, Indeterminate), read(2, 5, 6, "a")),
		"indeterminate append never seen": h(
			app(1, "a", 1, 2, Indeterminate), read(2, 5, 6), read(2, 7, 8)),
		"read during an append sees it": h(
			app(1, "a", 1, 10, Known), read(2, 3, 4, "a")),
		"read during an append misses it": h(
			app(1, "a", 1, 10, Known), read(2, 3, 4)),
		"failed append never seen": h(
			app(1, "a", 1, 2, Failed), read(2, 5, 6)),
		"indeterminate read is dropped": h(
			app(1, "a", 1, 2, Known), Op{Client: 2, Call: 3, Return: 4, Outcome: Indeterminate, Seen: []string{"zzz"}}),
		"lists are independent": h(
			app(1, "a", 1, 2, Known), Op{Client: 2, Key: Key{"storage/a", "l2"}, Call: 5, Return: 6, Outcome: Known}),
	}
	for name, ops := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Check(ops, filepath.Join(t.TempDir(), "x.html"), time.Minute); err != nil {
				t.Fatalf("want a pass, got %v", err)
			}
		})
	}
}

// Porcupine rejects these, and the error points at its visualisation.
func TestCheckFailsModel(t *testing.T) {
	cases := map[string][]Op{
		"stale read after an acknowledged append": h(app(1, "a", 1, 2, Known), read(2, 5, 6)),
		"read loses a value an earlier read saw":  h(app(1, "a", 1, 10, Known), read(2, 2, 3, "a"), read(2, 4, 5)),
		"reads disagree on order": h(
			app(1, "a", 1, 2, Known), app(2, "b", 1, 2, Known), read(3, 5, 6, "a", "b"), read(3, 7, 8, "b", "a")),
		"indeterminate append seen, then lost": h(
			app(1, "a", 1, 2, Indeterminate), read(2, 5, 6, "a"), read(2, 7, 8)),
		"acknowledged append missing from the final read": h(
			app(1, "a", 1, 2, Known), app(1, "b", 3, 4, Known), read(2, 10, 11, "a")),
		"one bad list among good ones": h(
			Op{Client: 1, Key: Key{"storage/a", "l0"}, Append: true, Value: "x", Call: 1, Return: 2, Outcome: Known},
			app(1, "a", 1, 2, Known), read(2, 5, 6)),
	}
	for name, ops := range cases {
		t.Run(name, func(t *testing.T) {
			html := filepath.Join(t.TempDir(), "x.html")
			err := Check(ops, html, time.Minute)
			if !errors.Is(err, ErrViolation) || !strings.Contains(err.Error(), html) {
				t.Fatalf("want a violation pointing at %s, got %v", html, err)
			}
			if b, err := os.ReadFile(html); err != nil || len(b) == 0 {
				t.Fatalf("no visualisation: %v", err)
			}
		})
	}
}

// The rules catch these before Porcupine, and the error names the list and the
// operations. A value appended twice is the test's mistake, not a violation.
func TestCheckFailsRules(t *testing.T) {
	cases := map[string]struct {
		ops       []Op
		violation bool
		want      []string // the error must name each
	}{
		"failed append appears": {
			h(app(1, "a", 1, 2, Failed), read(2, 5, 6, "a")), true, []string{"op 0", "op 1", "failed"}},
		"value read twice": {
			h(app(1, "a", 1, 2, Known), read(2, 5, 6, "a", "a")), true, []string{"op 1", `"a"`}},
		"value appended twice": {
			h(app(1, "a", 1, 2, Known), app(2, "a", 3, 4, Known)), false, []string{"op 0", "op 1"}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := Check(c.ops, filepath.Join(t.TempDir(), "x.html"), time.Minute)
			if err == nil {
				t.Fatal("want an error")
			}
			if errors.Is(err, ErrViolation) != c.violation {
				t.Errorf("a violation: %v, want %v: %v", errors.Is(err, ErrViolation), c.violation, err)
			}
			for _, w := range append(c.want, "storage/a", "l1") {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("the error lacks %q: %v", w, err)
				}
			}
		})
	}
}

func TestRecorder(t *testing.T) {
	r := NewRecorder()
	a := r.Begin(1, lst, "", true, "a")
	b := r.Begin(2, lst, "", false, "")
	r.End(b, Known, nil)
	r.End(a, Known, nil)
	ops := r.Ops()
	if len(ops) != 2 || !(ops[0].Call <= ops[1].Call && ops[1].Call <= ops[1].Return && ops[1].Return <= ops[0].Return) {
		t.Fatalf("times not from one clock: %+v", ops)
	}
	r.Begin(3, lst, "", true, "c") // never ended
	if got := r.Ops()[2].Outcome; got != Indeterminate {
		t.Fatalf("unended op is %v", got)
	}
}

func TestRelayRule(t *testing.T) {
	bKey := Key{"storage/b", "l1"}
	ops := []Op{{ID: 0, Key: bKey, Via: "storage/a", Append: true, Value: "v1", Outcome: Known}}
	finals := map[Key][]string{bKey: {"v1"}}
	if err := CheckRelay(ops, finals, map[string][]string{"storage/a": {"v1"}}); err != nil {
		t.Fatal(err)
	}
	// Not in B's final list: nothing to check.
	if err := CheckRelay(ops, map[Key][]string{bKey: nil}, nil); err != nil {
		t.Fatal(err)
	}
	err := CheckRelay(ops, finals, map[string][]string{"storage/a": {"other"}})
	if err == nil {
		t.Fatal("want a violation")
	}
	for _, w := range []string{"storage/b", "l1", "storage/a", "v1", "op 0"} {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("report lacks %q: %v", w, err)
		}
	}
}

func TestReminderRule(t *testing.T) {
	acked := []Reminder{{"storage/a", "k1"}}
	ok := map[string]Restored{"storage/a": {Set: []string{"k1", "k2"}, Fired: []string{"k1", "k2"}}}
	if err := CheckReminders(acked, ok); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		acked    []Reminder
		restored map[string]Restored
		want     string
	}{
		"acknowledged set with no firing": {acked,
			map[string]Restored{"storage/a": {Set: []string{"k1"}}}, `"k1" of storage/a`},
		"acknowledged set absent from the restore": {acked,
			map[string]Restored{"storage/a": {}}, `"k1" of storage/a`},
		"set seen only in the restore": {nil,
			map[string]Restored{"storage/b": {Set: []string{"k9"}}}, `"k9" of storage/b`},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			err := CheckReminders(c.acked, c.restored)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a report naming %s, got %v", c.want, err)
			}
		})
	}
}

func TestAnswer(t *testing.T) {
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	cases := []struct {
		name   string
		status int // 0 for no response
		reason string
		err    error
		want   Outcome
	}{
		{"ok", 200, "", nil, Known},
		{"a refused connection", 0, "", refused, Failed},
		{"no answer in time", 0, "", context.DeadlineExceeded, Indeterminate},
		{"a reset connection", 0, "", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}, Indeterminate},
		{"a body cut short", 0, "", io.ErrUnexpectedEOF, Indeterminate},
		{"app-unreachable", 502, "app-unreachable", nil, Failed},
		{"moved", 503, "moved", nil, Failed},
		{"draining", 503, "draining", nil, Failed},
		{"not-ready", 503, "not-ready", nil, Failed},
		{"unknown-shard", 404, "unknown-shard", nil, Failed},
		{"bad-request", 400, "bad-request", nil, Failed},
		{"hops", 508, "hops", nil, Failed},
		{"the app's own 502, such as a failed relay", 502, "", nil, Indeterminate},
		{"app-failed", 502, "app-failed", nil, Indeterminate},
		{"owner-failed", 502, "owner-failed", nil, Indeterminate},
		{"internal", 502, "internal", nil, Indeterminate},
		{"owner-unreachable", 503, "owner-unreachable", nil, Indeterminate},
		{"lease-lost", 503, "lease-lost", nil, Indeterminate},
		{"sync-unmet", 503, "sync-unmet", nil, Indeterminate},
		{"bad-file", 503, "bad-file", nil, Indeterminate},
		{"timeout", 504, "timeout", nil, Indeterminate},
		{"not-active", 404, "not-active", nil, Indeterminate},
		{"a 500 with no reason", 500, "", nil, Indeterminate},
	}
	for _, c := range cases {
		var resp *http.Response
		if c.err == nil {
			resp = &http.Response{StatusCode: c.status, Header: http.Header{}}
			if c.reason != "" {
				resp.Header.Set("Shardlite-Error", c.reason)
			}
		}
		if got := Answer(resp, c.err); got != c.want {
			t.Errorf("%s: Answer = %v, want %v", c.name, got, c.want)
		}
	}
}
