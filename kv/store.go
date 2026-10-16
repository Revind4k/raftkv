// Package kv is a key-value store replicated with package raft: the state
// machine (Store), an HTTP server that runs one cluster member, and a client.
package kv

// Op is one client command. Every op, reads included, goes through the Raft log,
// so each one takes effect at a single point in the log order. That is what
// makes the store linearizable.
type Op struct {
	Kind  string `json:"kind"` // "get", "put" or "append"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	// Client and Seq identify the op for duplicate detection. A client retries a
	// timed-out op with the same Seq, and the store answers the retry from its
	// session table instead of running the op twice. Client 0 opts out.
	Client uint64 `json:"client,omitempty"`
	Seq    uint64 `json:"seq,omitempty"`
}

type session struct {
	seq   uint64
	value string
}

// Store is the replicated state machine. Every replica applies the same
// committed ops in the same order, so every replica's Store ends up identical.
type Store struct {
	data     map[string]string
	sessions map[uint64]session
	// SkipDedupe runs retried ops again. It exists so the tests can show that
	// the linearizability checker catches the resulting double appends.
	SkipDedupe bool
}

func NewStore() *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}}
}

// Apply runs op and returns its result: the value for a get, "" otherwise.
func (s *Store) Apply(op Op) string {
	if op.Client != 0 && !s.SkipDedupe {
		if ss, ok := s.sessions[op.Client]; ok && op.Seq <= ss.seq {
			return ss.value // a retry of an op that already ran
		}
	}
	var v string
	switch op.Kind {
	case "get":
		v = s.data[op.Key]
	case "put":
		s.data[op.Key] = op.Value
	case "append":
		s.data[op.Key] += op.Value
	}
	if op.Client != 0 {
		s.sessions[op.Client] = session{op.Seq, v}
	}
	return v
}
