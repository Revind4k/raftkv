// Package lincheck checks whether a history of key-value operations is
// linearizable: whether there is one order of all operations that respects real
// time (an op that returned before another started comes first) and in which
// every get returns the value left by the puts and appends before it.
//
// It implements the Wing & Gong search with the memoization Lowe added (the same
// approach as Porcupine and Knossos). Keys are independent, so each key's
// history is checked on its own, which keeps the search small.
package lincheck

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// Pending marks an op that never returned. It may have taken effect at any point
// after its call, or not at all; placing it after everything else covers "not
// at all".
const Pending = math.MaxInt64

type Operation struct {
	Client int
	Kind   string // "get", "put", "append"
	Key    string
	Value  string // argument of put and append
	Output string // result of get
	Call   int64  // logical timestamps; distinct across the whole history
	Return int64
}

func (o Operation) String() string {
	switch o.Kind {
	case "get":
		return fmt.Sprintf("client %d: get(%s) -> %q", o.Client, o.Key, o.Output)
	default:
		return fmt.Sprintf("client %d: %s(%s, %q)", o.Client, o.Kind, o.Key, o.Value)
	}
}

// Check returns nil if the history is linearizable. Otherwise the error names
// the first key that is not, with that key's operations in call order.
func Check(history []Operation) error {
	byKey := map[string][]Operation{}
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !checkKey(byKey[k]) {
			ops := byKey[k]
			sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
			var b strings.Builder
			for _, op := range ops {
				ret := "pending"
				if op.Return != Pending {
					ret = fmt.Sprint(op.Return)
				}
				fmt.Fprintf(&b, "\n  [%d, %s] %s", op.Call, ret, op)
			}
			return fmt.Errorf("history for key %q is not linearizable:%s", k, b.String())
		}
	}
	return nil
}

func step(state string, op Operation) (string, bool) {
	switch op.Kind {
	case "get":
		return state, op.Output == state
	case "put":
		return op.Value, true
	case "append":
		return state + op.Value, true
	}
	return state, false
}

// event is a call or a return in a doubly linked list sorted by time. A call
// points to its matching return.
type event struct {
	id         int
	call       bool
	time       int64
	match      *event
	prev, next *event
}

func checkKey(ops []Operation) bool {
	events := make([]*event, 0, 2*len(ops))
	for i, op := range ops {
		ret := &event{id: i, time: op.Return}
		events = append(events, &event{id: i, call: true, time: op.Call, match: ret}, ret)
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].time < events[j].time })
	head := &event{}
	prev := head
	for _, e := range events {
		prev.next, e.prev = e, prev
		prev = e
	}

	// lift removes a call and its return from the list; unlift puts them back.
	lift := func(e *event) {
		e.prev.next = e.next
		if e.next != nil {
			e.next.prev = e.prev
		}
		r := e.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(e *event) {
		r := e.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		e.prev.next = e
		if e.next != nil {
			e.next.prev = e
		}
	}

	type frame struct {
		e     *event
		state string
	}
	var stack []frame
	linearized := make([]uint64, (len(ops)+63)/64)
	seen := map[string]bool{} // (set of linearized ops, state) pairs already explored
	key := func(state string) string {
		var b strings.Builder
		for _, w := range linearized {
			fmt.Fprintf(&b, "%x.", w)
		}
		b.WriteString(state)
		return b.String()
	}

	state := ""
	e := head.next
	for head.next != nil {
		if e.call {
			if next, ok := step(state, ops[e.id]); ok {
				linearized[e.id/64] |= 1 << (e.id % 64)
				if k := key(next); !seen[k] {
					seen[k] = true
					stack = append(stack, frame{e, state})
					state = next
					lift(e)
					e = head.next
					continue
				}
				linearized[e.id/64] &^= 1 << (e.id % 64)
			}
			e = e.next
			continue
		}
		// Reached a return whose call is not linearized yet: every op that could
		// go next has been tried. Undo the last choice and try the one after it.
		if len(stack) == 0 {
			return false
		}
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = top.state
		linearized[top.e.id/64] &^= 1 << (top.e.id % 64)
		unlift(top.e)
		e = top.e.next
	}
	return true
}
