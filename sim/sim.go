// Package sim runs a whole raftkv cluster inside one goroutine on a simulated
// network and clock. Everything random (message delay, loss, duplication,
// partitions, crashes, client ops, election timeouts) comes from one seed, so
// any failure the simulator finds replays exactly from that seed.
//
// While it runs it checks Raft's safety properties after every tick. At the end
// it heals the cluster, checks that every client op finished (liveness), and
// checks that the clients' history is linearizable.
package sim

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/Revind4k/raftkv/kv"
	"github.com/Revind4k/raftkv/lincheck"
	"github.com/Revind4k/raftkv/raft"
)

type Config struct {
	Seed    int64
	Nodes   int
	Clients int
	Keys    int
	Ticks   int // faults and new client ops happen only during these ticks

	MaxDelay      int     // each message takes 1..MaxDelay ticks
	DropRate      float64 // per message
	DupRate       float64 // per message
	PartitionRate float64 // per tick: split the cluster in two at random
	HealRate      float64 // per tick, while partitioned
	CrashRate     float64 // per tick: crash a node (never more than a minority at once)
	RestartRate   float64 // per tick, per crashed node

	ClientTimeout int // ticks before a client retries on another node
	MaxEntries    int // per AppendEntries message; 0 = raft's default
	Bugs          raft.BugInjection
	SkipDedupe    bool
}

func DefaultConfig(seed int64) Config {
	return Config{
		Seed: seed, Nodes: 5, Clients: 5, Keys: 3, Ticks: 3000,
		MaxDelay: 3, DropRate: 0.05, DupRate: 0.02,
		PartitionRate: 0.005, HealRate: 0.02, CrashRate: 0.003, RestartRate: 0.01,
		ClientTimeout: 40,
	}
}

const healTicks = 3000

type Stats struct {
	Ops, Messages, Elections, Crashes, Partitions, Retries int
	MaxTerm                                                uint64
}

type Result struct {
	Violation string // empty if the run passed every check
	Stats     Stats
	History   []lincheck.Operation
}

type node struct {
	id      int
	storage *raft.MemoryStorage
	raft    *raft.Node
	store   *kv.Store
	up      bool
	waiting map[[2]uint64]int // (client, seq) -> client index, for ops proposed here
}

type packet struct {
	at int
	m  raft.Message
}

type request struct {
	op      kv.Op
	hist    int // index into history
	node    int // node the op was last sent to, 0 if not yet accepted by a leader
	sentAt  int
	attempt int
}

type client struct {
	id     uint64
	target int // node this client thinks is leader
	seq    uint64
	cur    *request
	nextAt int
}

type sim struct {
	cfg     Config
	rng     *rand.Rand
	now     int
	stamp   int64 // logical clock for history call/return times
	nodes   []*node
	net     []packet
	group   []int // partition side of each node; equal sides can talk
	clients []*client
	history []lincheck.Operation
	stats   Stats

	leaderOf map[uint64]int         // term -> leader seen in that term
	applied  map[uint64][2]any      // index -> (term, data) first applied anywhere
	lastSeen map[int]raft.StateType // previous state, to count elections
	failure  string
}

func Run(cfg Config) (res Result) {
	s := &sim{
		cfg:      cfg,
		rng:      rand.New(rand.NewSource(cfg.Seed)),
		group:    make([]int, cfg.Nodes+1),
		leaderOf: map[uint64]int{},
		applied:  map[uint64][2]any{},
		lastSeen: map[int]raft.StateType{},
	}
	defer func() {
		// raft panics on internal invariant breaks (two leaders in a term,
		// overwriting a committed entry). Under bug injection that is a finding.
		if r := recover(); r != nil {
			res = Result{Violation: fmt.Sprintf("panic at tick %d: %v", s.now, r), Stats: s.stats, History: s.history}
		}
	}()
	for i := 1; i <= cfg.Nodes; i++ {
		n := &node{id: i, storage: raft.NewMemoryStorage()}
		s.nodes = append(s.nodes, n)
		s.start(n)
	}
	for i := 0; i < cfg.Clients; i++ {
		s.clients = append(s.clients, &client{id: uint64(i + 1), target: 1 + s.rng.Intn(cfg.Nodes)})
	}

	for s.now = 0; s.now < cfg.Ticks && s.failure == ""; s.now++ {
		s.faults()
		s.tick(true)
	}
	// Liveness: heal everything and give the cluster time to finish every op.
	s.cfg.DropRate, s.cfg.DupRate = 0, 0
	for i := range s.group {
		s.group[i] = 0
	}
	for _, n := range s.nodes {
		if !n.up {
			s.start(n)
		}
	}
	// Catch-up moves one AppendEntries batch per round trip, so a follower that
	// spent the run crashed may be hundreds of batches behind.
	end := s.now + healTicks
	for ; s.now < end && s.failure == "" && !s.quiet(); s.now++ {
		s.tick(false)
	}
	if s.failure == "" && !s.quiet() {
		s.failure = s.describeStuck()
	}
	if s.failure == "" {
		s.checkLogMatching()
	}
	if s.failure == "" {
		if err := lincheck.Check(s.history); err != nil {
			s.failure = "linearizability: " + err.Error()
		}
	}
	return Result{Violation: s.failure, Stats: s.stats, History: s.history}
}

func (s *sim) start(n *node) {
	peers := make([]int, s.cfg.Nodes)
	for i := range peers {
		peers[i] = i + 1
	}
	r, err := raft.New(raft.Config{
		ID: n.id, Peers: peers, Storage: n.storage, Bugs: s.cfg.Bugs, MaxEntries: s.cfg.MaxEntries,
		Rand: rand.New(rand.NewSource(s.rng.Int63())),
	})
	if err != nil {
		panic(err)
	}
	// The state machine is rebuilt from the log as entries commit again.
	n.raft, n.store, n.up = r, kv.NewStore(), true
	n.store.SkipDedupe = s.cfg.SkipDedupe
	n.waiting = map[[2]uint64]int{}
}

func (s *sim) faults() {
	c := s.cfg
	partitioned := false
	for _, g := range s.group {
		partitioned = partitioned || g != 0
	}
	if !partitioned && s.rng.Float64() < c.PartitionRate {
		for i := 1; i <= c.Nodes; i++ {
			s.group[i] = s.rng.Intn(2)
		}
		s.stats.Partitions++
	} else if partitioned && s.rng.Float64() < c.HealRate {
		for i := range s.group {
			s.group[i] = 0
		}
	}
	down := 0
	for _, n := range s.nodes {
		if !n.up {
			down++
		}
	}
	if s.rng.Float64() < c.CrashRate && down+1 <= (c.Nodes-1)/2 {
		if n := s.nodes[s.rng.Intn(c.Nodes)]; n.up {
			n.up = false
			s.stats.Crashes++
		}
	}
	for _, n := range s.nodes {
		if !n.up && s.rng.Float64() < c.RestartRate {
			s.start(n)
		}
	}
}

func (s *sim) tick(newOps bool) {
	for _, n := range s.nodes {
		if n.up {
			n.raft.Tick()
		}
	}
	// Deliver due packets. Their order within a tick follows the random delays.
	rest := s.net[:0]
	var due []packet
	for _, p := range s.net {
		if p.at <= s.now {
			due = append(due, p)
		} else {
			rest = append(rest, p)
		}
	}
	s.net = rest
	for _, p := range due {
		n := s.nodes[p.m.To-1]
		if n.up && s.group[p.m.From] == s.group[p.m.To] {
			n.raft.Step(p.m)
		}
	}
	for i, c := range s.clients {
		s.drive(i, c, newOps)
	}
	for _, n := range s.nodes {
		if n.up {
			s.process(n)
		}
	}
	s.checkLeaders()
	if s.now%50 == 0 {
		s.checkLogMatching()
	}
}

// drive moves one client forward: start an op, retry it, or wait.
func (s *sim) drive(i int, c *client, newOps bool) {
	if c.cur == nil {
		if !newOps || s.now < c.nextAt {
			return
		}
		c.seq++
		op := kv.Op{Key: fmt.Sprintf("k%d", s.rng.Intn(s.cfg.Keys)), Client: c.id, Seq: c.seq}
		switch r := s.rng.Intn(10); {
		case r < 4:
			op.Kind = "get"
		case r < 6:
			op.Kind, op.Value = "put", fmt.Sprintf("p%d.%d;", c.id, c.seq)
		default:
			op.Kind, op.Value = "append", fmt.Sprintf("a%d.%d;", c.id, c.seq)
		}
		s.stamp++
		s.history = append(s.history, lincheck.Operation{
			Client: int(c.id), Kind: op.Kind, Key: op.Key, Value: op.Value, Call: s.stamp, Return: lincheck.Pending,
		})
		c.cur = &request{op: op, hist: len(s.history) - 1}
		s.stats.Ops++
	}
	r := c.cur
	if r.node != 0 && s.now-r.sentAt < s.cfg.ClientTimeout {
		return // waiting for the leader it reached to apply the op
	}
	if r.node != 0 {
		s.stats.Retries++ // timed out: maybe the leader crashed or lost its term
		c.target = 1 + s.rng.Intn(s.cfg.Nodes)
	}
	r.node = 0
	n := s.nodes[c.target-1]
	if !n.up {
		c.target = 1 + s.rng.Intn(s.cfg.Nodes)
		return
	}
	data, _ := json.Marshal(r.op)
	if _, _, ok := n.raft.Propose(data); !ok {
		if lead := n.raft.Status().Lead; lead != 0 {
			c.target = lead
		} else {
			c.target = 1 + s.rng.Intn(s.cfg.Nodes)
		}
		return
	}
	r.node, r.sentAt = n.id, s.now
	r.attempt++
	n.waiting[[2]uint64{r.op.Client, r.op.Seq}] = i
}

func (s *sim) process(n *node) {
	rd := n.raft.Ready()
	for _, m := range rd.Messages {
		s.stats.Messages++
		if s.rng.Float64() < s.cfg.DropRate {
			continue
		}
		copies := 1
		if s.rng.Float64() < s.cfg.DupRate {
			copies = 2
		}
		for ; copies > 0; copies-- {
			s.net = append(s.net, packet{at: s.now + 1 + s.rng.Intn(s.cfg.MaxDelay), m: m})
		}
	}
	for _, e := range rd.Committed {
		// State machine safety: every node applies the same entry at each index.
		want, seen := s.applied[e.Index]
		if !seen {
			s.applied[e.Index] = [2]any{e.Term, string(e.Data)}
		} else if want != [2]any{e.Term, string(e.Data)} {
			s.fail("state machine safety: node %d applied (term %d, %q) at index %d, another node applied (term %d, %q)",
				n.id, e.Term, e.Data, e.Index, want[0], want[1])
			return
		}
		if e.Data == nil {
			continue // leader no-op
		}
		var op kv.Op
		if err := json.Unmarshal(e.Data, &op); err != nil {
			panic(err)
		}
		v := n.store.Apply(op)
		key := [2]uint64{op.Client, op.Seq}
		i, ok := n.waiting[key]
		if !ok {
			continue
		}
		delete(n.waiting, key)
		c := s.clients[i]
		if c.cur == nil || c.cur.op.Seq != op.Seq {
			continue // an older attempt of an op that already returned
		}
		s.stamp++
		h := &s.history[c.cur.hist]
		h.Return, h.Output = s.stamp, v
		c.cur = nil
		c.nextAt = s.now + s.rng.Intn(5)
	}
}

func (s *sim) fail(format string, args ...any) {
	if s.failure == "" {
		s.failure = fmt.Sprintf("tick %d: ", s.now) + fmt.Sprintf(format, args...)
	}
}

// checkLeaders enforces election safety: at most one leader per term, ever.
func (s *sim) checkLeaders() {
	for _, n := range s.nodes {
		if !n.up {
			continue
		}
		st := n.raft.Status()
		if st.Term > s.stats.MaxTerm {
			s.stats.MaxTerm = st.Term
		}
		if st.State == raft.Leader {
			if s.lastSeen[n.id] != raft.Leader {
				s.stats.Elections++
			}
			if other, ok := s.leaderOf[st.Term]; ok && other != n.id {
				s.fail("election safety: nodes %d and %d were both leader in term %d", other, n.id, st.Term)
			}
			s.leaderOf[st.Term] = n.id
		}
		s.lastSeen[n.id] = st.State
	}
}

// checkLogMatching: if two logs hold an entry with the same index and term, they
// are identical up to and including it.
func (s *sim) checkLogMatching() {
	logs := make([][]raft.Entry, len(s.nodes))
	for i, n := range s.nodes {
		logs[i] = n.raft.Entries()
	}
	for a := 0; a < len(logs); a++ {
		for b := a + 1; b < len(logs); b++ {
			la, lb := logs[a], logs[b]
			i := min(len(la), len(lb)) - 1
			for i >= 0 && la[i].Term != lb[i].Term {
				i--
			}
			for ; i >= 0; i-- {
				if la[i].Term != lb[i].Term || string(la[i].Data) != string(lb[i].Data) {
					s.fail("log matching: nodes %d and %d differ at index %d below a matching entry", a+1, b+1, i+1)
					return
				}
			}
		}
	}
}

// quiet: no client op outstanding and every node has applied the same prefix.
func (s *sim) quiet() bool {
	for _, c := range s.clients {
		if c.cur != nil {
			return false
		}
	}
	commit := s.nodes[0].raft.Status().Commit
	for _, n := range s.nodes {
		st := n.raft.Status()
		if st.Commit != commit || st.LastIndex != commit {
			return false
		}
	}
	return true
}

func (s *sim) describeStuck() string {
	msg := fmt.Sprintf("liveness: cluster did not finish all ops within %d healed ticks;", healTicks)
	for _, n := range s.nodes {
		st := n.raft.Status()
		msg += fmt.Sprintf(" n%d{%s term=%d commit=%d last=%d up=%v}", n.id, st.State, st.Term, st.Commit, st.LastIndex, n.up)
		if st.State == raft.Leader {
			msg += fmt.Sprintf(" match=%v next=%v", st.Match, st.Next)
		}
	}
	return msg
}
