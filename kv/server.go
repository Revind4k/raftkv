package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/Revind4k/raftkv/raft"
)

var (
	ErrNotLeader = errors.New("not the leader")
	// ErrLost means the op's log slot was taken by another leader's entry, or
	// this node lost leadership while waiting. The op may or may not have run;
	// retry with the same Seq and the session table makes it run at most once.
	ErrLost = errors.New("leadership changed before the op committed")
)

type ServerConfig struct {
	ID      int
	Peers   map[int]string // member ID -> base URL, including this node
	Storage raft.Storage
	Tick    time.Duration // one Raft tick; election timeout is 10-20 ticks
	Logger  *log.Logger
}

// Server runs one cluster member. One goroutine owns the raft.Node and the
// Store; HTTP handlers talk to it over channels.
type Server struct {
	cfg     ServerConfig
	node    *raft.Node
	store   *Store
	recv    chan raft.Message
	props   chan *proposal
	status  chan chan raft.Status
	outbox  map[int]chan raft.Message
	waiting map[uint64]*proposal // by log index
	stop    chan struct{}
	done    chan struct{}
	httpc   *http.Client
	last    raft.Status // for logging role changes
}

type proposal struct {
	op    Op
	index uint64
	term  uint64
	reply chan result // buffered, so the loop never blocks on a gone handler
}

type result struct {
	value string
	lead  int
	err   error
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	var ids []int
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	node, err := raft.New(raft.Config{
		ID: cfg.ID, Peers: ids, Storage: cfg.Storage,
		Rand: rand.New(rand.NewSource(time.Now().UnixNano())),
	})
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg: cfg, node: node, store: NewStore(),
		recv:    make(chan raft.Message, 1024),
		props:   make(chan *proposal, 256),
		status:  make(chan chan raft.Status),
		outbox:  map[int]chan raft.Message{},
		waiting: map[uint64]*proposal{},
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		httpc:   &http.Client{Timeout: 500 * time.Millisecond},
	}
	for id, url := range cfg.Peers {
		if id == cfg.ID {
			continue
		}
		ch := make(chan raft.Message, 256)
		s.outbox[id] = ch
		go s.sender(url, ch)
	}
	go s.loop()
	return s, nil
}

// Close stops the server. It does not close Storage.
func (s *Server) Close() {
	close(s.stop)
	<-s.done
}

func (s *Server) loop() {
	defer close(s.done)
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.node.Tick()
		case m := <-s.recv:
			s.node.Step(m)
		case p := <-s.props:
			data, _ := json.Marshal(p.op)
			idx, term, ok := s.node.Propose(data)
			if !ok {
				p.reply <- result{err: ErrNotLeader, lead: s.node.Status().Lead}
				break
			}
			p.index, p.term = idx, term
			s.waiting[idx] = p
		case ch := <-s.status:
			ch <- s.node.Status()
		}
		s.process()
	}
}

func (s *Server) process() {
	if st := s.node.Status(); st.State != s.last.State || st.Term != s.last.Term && st.State == raft.Leader {
		s.cfg.Logger.Printf("%s in term %d (leader %d, commit %d)", st.State, st.Term, st.Lead, st.Commit)
		s.last = st
	}
	rd := s.node.Ready()
	for _, m := range rd.Messages {
		select {
		case s.outbox[m.To] <- m:
		default: // peer is slow or down; Raft retries on the next heartbeat
		}
	}
	for _, e := range rd.Committed {
		var v string
		if e.Data != nil {
			var op Op
			if err := json.Unmarshal(e.Data, &op); err != nil {
				panic(fmt.Sprintf("kv: undecodable entry %d: %v", e.Index, err))
			}
			v = s.store.Apply(op)
		}
		if p, ok := s.waiting[e.Index]; ok {
			delete(s.waiting, e.Index)
			if p.term == e.Term {
				p.reply <- result{value: v}
			} else {
				p.reply <- result{err: ErrLost}
			}
		}
	}
	if len(s.waiting) > 0 && s.node.Status().State != raft.Leader {
		for idx, p := range s.waiting {
			p.reply <- result{err: ErrLost}
			delete(s.waiting, idx)
		}
	}
}

func (s *Server) sender(url string, ch chan raft.Message) {
	for {
		select {
		case <-s.stop:
			return
		case m := <-ch:
			b, _ := json.Marshal(m)
			resp, err := s.httpc.Post(url+"/raft", "application/json", bytes.NewReader(b))
			if err == nil {
				resp.Body.Close()
			}
		}
	}
}

// Do runs op through the log and returns its result once it is committed and
// applied on this node.
func (s *Server) Do(ctx context.Context, op Op) (string, int, error) {
	p := &proposal{op: op, reply: make(chan result, 1)}
	select {
	case s.props <- p:
	case <-ctx.Done():
		return "", 0, ctx.Err()
	case <-s.stop:
		return "", 0, errors.New("server stopped")
	}
	select {
	case r := <-p.reply:
		return r.value, r.lead, r.err
	case <-ctx.Done():
		return "", 0, ctx.Err()
	}
}

func (s *Server) Status() raft.Status {
	ch := make(chan raft.Status, 1)
	select {
	case s.status <- ch:
		return <-ch
	case <-s.stop:
		return raft.Status{}
	}
}

// Handler serves POST /raft (peer messages), POST /kv (client ops) and
// GET /status.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft", func(w http.ResponseWriter, r *http.Request) {
		var m raft.Message
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case s.recv <- m:
		default: // overloaded: dropping is safe, the sender will retry
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /kv", func(w http.ResponseWriter, r *http.Request) {
		var op Op
		if err := json.NewDecoder(r.Body).Decode(&op); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if op.Kind != "get" && op.Kind != "put" && op.Kind != "append" {
			http.Error(w, `kind must be "get", "put" or "append"`, http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		v, lead, err := s.Do(ctx, op)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case err == nil:
			json.NewEncoder(w).Encode(map[string]string{"value": v})
		case errors.Is(err, ErrNotLeader):
			w.WriteHeader(http.StatusMisdirectedRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": err.Error(), "leader": lead})
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		}
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Status())
	})
	return mux
}
