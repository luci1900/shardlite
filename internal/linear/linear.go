// Package linear checks that a shard behaves as one copy of its database. Clients
// record every call with its start and end, and the checker judges the history
// with Porcupine against a model of a list you append to: an append adds a unique
// value and a read returns the whole list.
//
// An answer becomes one of three outcomes (see Answer). A known operation has a
// result. A failed one never reached the app, so it is dropped, and a failed
// append whose value shows up in a read is a violation. An indeterminate one may
// or may not have happened: an indeterminate read is dropped and an indeterminate
// append stays in the history with no end (unless no read saw it, see toHistory).
package linear

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Outcome says what the client knows about an operation once it stopped waiting.
type Outcome int

const (
	// Indeterminate is the zero value, so an operation whose client never recorded
	// an end counts as one that never returned.
	Indeterminate Outcome = iota
	// Known means the node answered 200: an append happened, a read has its result.
	Known
	// Failed means the node said the app never had the request.
	Failed
)

func (o Outcome) String() string {
	switch o {
	case Known:
		return "known"
	case Failed:
		return "failed"
	case Indeterminate:
		return "indeterminate"
	}
	return fmt.Sprintf("outcome(%d)", int(o))
}

// failedReasons are the Shardlite-Error reasons that mean the app never had the
// request (docs/guide.md, Status codes). The node's own constants are unexported
// and this package must not depend on the node.
var failedReasons = map[string]bool{
	"app-unreachable": true,
	"moved":           true,
	"draining":        true,
	"not-ready":       true,
	"unknown-shard":   true,
	"bad-request":     true,
	"hops":            true,
}

// Answer turns what a client got into an outcome: a response, or the error of a
// request that got none (or whose body could not be read). A connection that
// could not be made sent nothing, so it failed. Any other error is
// indeterminate, as is every error response the node didn't mark as never
// reaching the app, including the 502 an app gives for a failed call a relayed
// append made (the inner call's reason never reaches the client).
func Answer(resp *http.Response, err error) Outcome {
	if err != nil {
		if op, ok := errors.AsType[*net.OpError](err); ok && op.Op == "dial" {
			return Failed
		}
		return Indeterminate
	}
	if resp.StatusCode == http.StatusOK {
		return Known
	}
	if failedReasons[resp.Header.Get("Shardlite-Error")] {
		return Failed
	}
	return Indeterminate
}

// Key names one list of one shard. A history is checked per key.
type Key struct{ Shard, List string }

func (k Key) String() string { return k.Shard + " list " + k.List }

// Op is one append or read of a list. For a relayed append, Shard and List name
// the list appended to and Via names the shard whose relay route carried it.
type Op struct {
	ID      int
	Client  int
	Key     Key
	Via     string // the shard that relayed the append, empty for a direct one
	Append  bool   // an append of Value, else a read
	Value   string // the appended value
	Seen    []string
	Call    int64 // nanoseconds on the recorder's clock
	Return  int64
	Outcome Outcome
}

func (o Op) String() string {
	var s string
	if o.Append {
		s = "append " + o.Value
		if o.Via != "" {
			s += " via " + o.Via
		}
	} else {
		s = "read"
		if o.Outcome == Known {
			s += " -> [" + strings.Join(o.Seen, " ") + "]"
		}
	}
	return fmt.Sprintf("op %d (client %d, %s, %s, %s)", o.ID, o.Client, s, o.Key, o.Outcome)
}

// Recorder collects the history of concurrent clients. It stamps every call and
// return from one monotonic clock, so real-time order is exact.
type Recorder struct {
	start time.Time
	mu    sync.Mutex
	ops   []*Op
}

func NewRecorder() *Recorder { return &Recorder{start: time.Now()} }

func (r *Recorder) now() int64 { return int64(time.Since(r.start)) }

// Begin records the start of an operation and returns it, to be ended with End.
// An operation never ended counts as one that never returned.
func (r *Recorder) Begin(client int, key Key, via string, isAppend bool, value string) *Op {
	op := &Op{Client: client, Key: key, Via: via, Append: isAppend, Value: value}
	r.mu.Lock()
	defer r.mu.Unlock()
	op.ID = len(r.ops)
	op.Call = r.now()
	r.ops = append(r.ops, op)
	return op
}

// End records the answer to op. For a read, seen is the list it returned.
func (r *Recorder) End(op *Op, outcome Outcome, seen []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	op.Return = r.now()
	op.Outcome = outcome
	op.Seen = seen
}

// Ops returns a copy of the history so far.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	for i, op := range r.ops {
		out[i] = *op
	}
	return out
}

var (
	// ErrViolation is a history that breaks the promise: Porcupine found no
	// linearization, or a read saw what it never could have.
	ErrViolation = errors.New("linear: the history is not linearizable")
	// ErrTimedOut is a check Porcupine could not finish, which never passes.
	ErrTimedOut = errors.New("linear: the check timed out")
)

type input struct {
	ID     int
	Key    Key
	Append bool
	Value  string
}

type output struct{ Seen []string }

// An empty list is "", and every value adds itself and a NUL, so a state is
// comparable and Equal is ==.
const sep = "\x00"

func join(values []string) string {
	var b strings.Builder
	for _, v := range values {
		b.WriteString(v)
		b.WriteString(sep)
	}
	return b.String()
}

// listModel is a list you append to. Porcupine checks each list of each shard on
// its own (linearizability is local), so a state is one list. An append that
// never returned has a nil output and takes its step like any other.
var listModel = porcupine.Model{
	Partition: partition,
	Init:      func() interface{} { return "" },
	Step: func(state, in, out interface{}) (bool, interface{}) {
		s, i := state.(string), in.(input)
		if i.Append {
			return true, s + i.Value + sep
		}
		return join(out.(output).Seen) == s, s
	},
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		if i.Append {
			return fmt.Sprintf("append %s (op %d)", i.Value, i.ID)
		}
		return fmt.Sprintf("read -> [%s] (op %d)", strings.Join(out.(output).Seen, " "), i.ID)
	},
	DescribeState: func(state interface{}) string {
		return "[" + strings.Join(strings.Split(strings.TrimSuffix(state.(string), sep), sep), " ") + "]"
	},
}

// Check judges a history, list by list. A rule Porcupine can't check fails with
// an error naming the list and the operations. Otherwise Porcupine checks the
// lists in parallel within one timeout and, if the history fails or the check
// runs out of time, Check writes its visualisation to htmlPath, which shows the
// failing lists, and the error names the file. A violation is ErrViolation and
// a check that ran out of time ErrTimedOut.
func Check(ops []Op, htmlPath string, timeout time.Duration) error {
	groups := map[Key][]Op{}
	for _, op := range ops {
		groups[op.Key] = append(groups[op.Key], op)
	}
	for _, k := range sortedKeys(groups) {
		if err := checkRules(k, groups[k]); err != nil {
			return err
		}
	}
	res, info := porcupine.CheckOperationsVerbose(listModel, toHistory(ops), timeout)
	var err error
	switch res {
	case porcupine.Ok:
		return nil
	case porcupine.Illegal:
		err = ErrViolation
	case porcupine.Unknown:
		err = fmt.Errorf("%w after %s", ErrTimedOut, timeout)
	}
	if verr := porcupine.VisualizePath(listModel, info, htmlPath); verr != nil {
		return fmt.Errorf("%w (and writing the visualisation failed: %v)", err, verr)
	}
	return fmt.Errorf("%w, see %s", err, htmlPath)
}

func sortedKeys[V any](m map[Key]V) []Key {
	keys := make([]Key, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys
}

// toHistory turns the operations Porcupine judges into its history: failed
// operations and indeterminate reads are dropped, and an indeterminate append
// ends after everything else. An indeterminate append whose value no read saw
// is dropped too: it can take effect after every other operation, which
// changes no read, so the history is linearizable with it exactly when it is
// without it. Each one left would make Porcupine try it at every place, and a
// node that is down leaves hundreds.
func toHistory(ops []Op) []porcupine.Operation {
	end := int64(0)
	seen := map[Key]map[string]bool{}
	for _, op := range ops {
		end = max(end, op.Call, op.Return)
		if !op.Append && op.Outcome == Known {
			if seen[op.Key] == nil {
				seen[op.Key] = map[string]bool{}
			}
			for _, v := range op.Seen {
				seen[op.Key][v] = true
			}
		}
	}
	end++
	var history []porcupine.Operation
	for _, op := range ops {
		switch op.Outcome {
		case Failed:
			continue
		case Known:
		case Indeterminate:
			if !op.Append || !seen[op.Key][op.Value] {
				continue
			}
		}
		h := porcupine.Operation{ClientId: op.Client, Call: op.Call, Return: op.Return,
			Input: input{ID: op.ID, Key: op.Key, Append: op.Append, Value: op.Value}, Output: output{Seen: op.Seen}}
		if op.Outcome == Indeterminate {
			h.Return = end
		}
		history = append(history, h)
	}
	return history
}

// partition splits a history by list, in the order of the lists' keys, so the
// same history always gives the same partitions.
func partition(history []porcupine.Operation) [][]porcupine.Operation {
	groups := map[Key][]porcupine.Operation{}
	for _, h := range history {
		k := h.Input.(input).Key
		groups[k] = append(groups[k], h)
	}
	var parts [][]porcupine.Operation
	for _, k := range sortedKeys(groups) {
		parts = append(parts, groups[k])
	}
	return parts
}

// checkRules finds what Porcupine can't see or can't explain: values appended
// twice, a failed append that a read saw, and a read that lists a value twice.
func checkRules(k Key, ops []Op) error {
	appended := map[string]Op{}
	for _, op := range ops {
		if !op.Append {
			continue
		}
		if prev, ok := appended[op.Value]; ok {
			return fmt.Errorf("list %s: value %q appended by two operations, the check needs unique values: %s and %s", k, op.Value, prev, op)
		}
		appended[op.Value] = op
	}
	for _, op := range ops {
		if op.Append || op.Outcome != Known {
			continue
		}
		seen := map[string]bool{}
		for _, v := range op.Seen {
			if seen[v] {
				return fmt.Errorf("%w: list %s: a read saw value %q twice: %s", ErrViolation, k, v, op)
			}
			seen[v] = true
			if a, ok := appended[v]; ok && a.Outcome == Failed {
				return fmt.Errorf("%w: list %s: a read saw a value whose append failed (the app never had it): %s saw it, failed %s", ErrViolation, k, op, a)
			}
		}
	}
	return nil
}

// CheckRelay checks that every relayed value in the final list it was appended to
// is in the restore of the shard that relayed it, since the caller's gate holds a
// call until the caller's commits are durable. finals gives each list's final
// list and restored each relaying shard's values (the tokens its relay route
// recorded) as restored from blob storage.
func CheckRelay(ops []Op, finals map[Key][]string, restored map[string][]string) error {
	have := map[string]map[string]bool{}
	for shard, values := range restored {
		have[shard] = map[string]bool{}
		for _, v := range values {
			have[shard][v] = true
		}
	}
	var missing []string
	for _, op := range ops {
		if op.Via == "" || !op.Append {
			continue
		}
		in := false
		for _, v := range finals[op.Key] {
			if v == op.Value {
				in = true
			}
		}
		if in && !have[op.Via][op.Value] {
			missing = append(missing, fmt.Sprintf("list %s has relayed value %q that the restore of %s lacks: %s", op.Key, op.Value, op.Via, op))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s", strings.Join(missing, "\n"))
	}
	return nil
}

// Reminder names a reminder key of a shard.
type Reminder struct{ Shard, Key string }

// Restored is what a restore of a shard holds for the reminder rule: the keys
// whose set it recorded and the keys whose firing it recorded.
type Restored struct{ Set, Fired []string }

// CheckReminders checks that every reminder fired at least once in the restore.
// That holds for every key whose set was acknowledged and for every key whose set
// is in its shard's restore (a set the client never heard back about may still
// have happened).
func CheckReminders(acked []Reminder, restored map[string]Restored) error {
	need := map[Reminder]string{}
	for _, r := range acked {
		need[r] = "its set was acknowledged"
	}
	fired := map[Reminder]bool{}
	for shard, rs := range restored {
		for _, k := range rs.Set {
			r := Reminder{shard, k}
			if _, ok := need[r]; !ok {
				need[r] = "its set is in the restore"
			}
		}
		for _, k := range rs.Fired {
			fired[Reminder{shard, k}] = true
		}
	}
	var missing []string
	for r, why := range need {
		if !fired[r] {
			missing = append(missing, fmt.Sprintf("reminder %q of %s never fired (%s)", r.Key, r.Shard, why))
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return fmt.Errorf("%s", strings.Join(missing, "\n"))
	}
	return nil
}
