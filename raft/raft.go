// Package raft implements the Raft consensus algorithm (leader election and log
// replication) as a deterministic state machine.
//
// A Node does no I/O and starts no goroutines. The caller feeds it time (Tick),
// messages from peers (Step) and client commands (Propose), then drains Ready to
// get the messages to send and the entries that are now committed. Persistent
// state is written to Storage before Ready can hand out a message that depends on
// it. Because nothing inside depends on wall-clock time or scheduling, the same
// inputs always produce the same outputs, which is what lets the simulator in
// package sim replay any failure from its seed.
package raft

import (
	"fmt"
	"math/rand"
)

type StateType int

const (
	Follower StateType = iota
	Candidate
	Leader
)

func (s StateType) String() string {
	return [...]string{"follower", "candidate", "leader"}[s]
}

func (s StateType) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Entry is one slot in the replicated log. A nil Data marks the no-op entry a new
// leader appends so it can commit entries left over from earlier terms.
type Entry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	Data  []byte `json:"data,omitempty"`
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
)

func (t MsgType) String() string {
	return [...]string{"Vote", "VoteResp", "App", "AppResp"}[t]
}

// Message covers both Raft RPCs (RequestVote, AppendEntries) and their replies.
type Message struct {
	Type MsgType `json:"type"`
	From int     `json:"from"`
	To   int     `json:"to"`
	Term uint64  `json:"term"`

	// MsgVote: the candidate's last log position.
	LastIndex uint64 `json:"lastIndex,omitempty"`
	LastTerm  uint64 `json:"lastTerm,omitempty"`

	// MsgApp. MsgAppResp echoes PrevIndex so the leader can drop stale rejections.
	PrevIndex uint64  `json:"prevIndex,omitempty"`
	PrevTerm  uint64  `json:"prevTerm,omitempty"`
	Entries   []Entry `json:"entries,omitempty"`
	Commit    uint64  `json:"commit,omitempty"`

	// Replies. OK is "vote granted" or "entries accepted". On an accepted append,
	// Match is the follower's last index that now matches the leader. On a
	// rejected one it is a hint: the leader should retry with PrevIndex = Match.
	OK    bool   `json:"ok,omitempty"`
	Match uint64 `json:"match,omitempty"`
}

// HardState is what Raft must persist before answering any message.
type HardState struct {
	Term uint64 `json:"term"`
	Vote int    `json:"vote"` // 0 = voted for nobody this term
}

// BugInjection turns off individual safety rules so the tests can show that the
// simulator detects the resulting violations. Never set in a real deployment.
type BugInjection struct {
	// Grant votes without checking that the candidate's log is at least as up to
	// date as ours (Raft §5.4.1). A stale node can then win and overwrite
	// committed entries.
	SkipVoteLogCheck bool
	// Commit entries from earlier terms by counting replicas (the Figure 8 bug,
	// Raft §5.4.2). Correct Raft only counts replicas for current-term entries.
	CommitPriorTerms bool
}

type Config struct {
	ID    int
	Peers []int // every member's ID, including ID
	// A follower that hears nothing for a random [ElectionTick, 2*ElectionTick)
	// ticks starts an election. The leader sends heartbeats every HeartbeatTick.
	ElectionTick  int
	HeartbeatTick int
	MaxEntries    int // per AppendEntries message
	Storage       Storage
	Rand          *rand.Rand
	Bugs          BugInjection
}

type Status struct {
	ID        int       `json:"id"`
	Term      uint64    `json:"term"`
	State     StateType `json:"state"`
	Lead      int       `json:"lead"`
	Commit    uint64    `json:"commit"`
	LastIndex uint64    `json:"lastIndex"`
	// Leader only: the highest index known to be replicated on each peer.
	Match map[int]uint64 `json:"match,omitempty"`
	Next  map[int]uint64 `json:"next,omitempty"`
}

// Ready holds the work a Node has produced since the last call: messages to send
// and newly committed entries to apply, in log order.
type Ready struct {
	Messages  []Message
	Committed []Entry
}

type Node struct {
	cfg   Config
	id    int
	peers []int // excludes id

	term  uint64
	vote  int
	state StateType
	lead  int

	log     []Entry // log[0] is a sentinel at index 0, term 0, so log[i].Index == i
	commit  uint64
	applied uint64 // highest index handed out through Ready

	elapsed int // ticks since the last reset; for the leader, since the last heartbeat
	timeout int // randomized election timeout

	votes map[int]bool
	next  map[int]uint64
	match map[int]uint64

	msgs []Message
}

func New(c Config) (*Node, error) {
	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}
	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 1
	}
	if c.MaxEntries == 0 {
		c.MaxEntries = 64
	}
	if c.Rand == nil {
		c.Rand = rand.New(rand.NewSource(int64(c.ID)))
	}
	if c.HeartbeatTick >= c.ElectionTick {
		return nil, fmt.Errorf("raft: heartbeat tick %d must be below election tick %d", c.HeartbeatTick, c.ElectionTick)
	}
	hs, ents, err := c.Storage.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{cfg: c, id: c.ID, term: hs.Term, vote: hs.Vote, log: []Entry{{}}}
	for _, p := range c.Peers {
		if p != c.ID {
			n.peers = append(n.peers, p)
		}
	}
	if len(n.peers) != len(c.Peers)-1 {
		return nil, fmt.Errorf("raft: peers %v must contain id %d exactly once", c.Peers, c.ID)
	}
	for i, e := range ents {
		if e.Index != uint64(i+1) {
			return nil, fmt.Errorf("raft: storage entry %d has index %d", i+1, e.Index)
		}
	}
	n.log = append(n.log, ents...)
	n.resetElection()
	return n, nil
}

func (n *Node) lastIndex() uint64 { return uint64(len(n.log) - 1) }
func (n *Node) lastTerm() uint64  { return n.log[len(n.log)-1].Term }
func (n *Node) quorum(c int) bool { return c > (len(n.peers)+1)/2 }

func (n *Node) Status() Status {
	st := Status{ID: n.id, Term: n.term, State: n.state, Lead: n.lead, Commit: n.commit, LastIndex: n.lastIndex()}
	if n.state == Leader {
		st.Match, st.Next = map[int]uint64{}, map[int]uint64{}
		for _, p := range n.peers {
			st.Match[p], st.Next[p] = n.match[p], n.next[p]
		}
	}
	return st
}

// Entries returns a copy of the whole log, for invariant checks.
func (n *Node) Entries() []Entry { return append([]Entry(nil), n.log[1:]...) }

func (n *Node) Ready() Ready {
	rd := Ready{Messages: n.msgs}
	n.msgs = nil
	if n.commit > n.applied {
		rd.Committed = append([]Entry(nil), n.log[n.applied+1:n.commit+1]...)
		n.applied = n.commit
	}
	return rd
}

func (n *Node) Tick() {
	n.elapsed++
	if n.state == Leader {
		if n.elapsed >= n.cfg.HeartbeatTick {
			n.elapsed = 0
			n.broadcastAppend()
		}
		return
	}
	if n.elapsed >= n.timeout {
		n.campaign()
	}
}

// Propose appends data to the log if this node is the leader. The entry is
// committed only if it is later delivered in Ready at the same index and term.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	if n.state != Leader {
		return 0, 0, false
	}
	if data == nil {
		data = []byte{} // nil is reserved for leader no-ops
	}
	e := Entry{Index: n.lastIndex() + 1, Term: n.term, Data: data}
	n.appendEntries([]Entry{e})
	n.maybeCommit()
	n.broadcastAppend()
	return e.Index, e.Term, true
}

func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.term:
		lead := 0
		if m.Type == MsgApp {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		// Answer a stale request so its sender learns the newer term and steps
		// down. Stale replies are dropped.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From})
		case MsgApp:
			n.send(Message{Type: MsgAppResp, To: m.From, PrevIndex: m.PrevIndex})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		if n.state != Candidate {
			return
		}
		n.votes[m.From] = m.OK
		granted := 0
		for _, ok := range n.votes {
			if ok {
				granted++
			}
		}
		if n.quorum(granted) {
			n.becomeLeader()
		}
	case MsgApp:
		if n.state == Leader {
			panic(fmt.Sprintf("raft: node %d and node %d are both leader in term %d", n.id, m.From, n.term))
		}
		n.becomeFollower(m.Term, m.From)
		n.resetElection()
		n.handleAppend(m)
	case MsgAppResp:
		if n.state == Leader {
			n.handleAppResp(m)
		}
	}
}

func (n *Node) send(m Message) {
	m.From, m.Term = n.id, n.term
	n.msgs = append(n.msgs, m)
}

func (n *Node) persistHardState() {
	if err := n.cfg.Storage.SaveHardState(HardState{Term: n.term, Vote: n.vote}); err != nil {
		// Continuing without durable state could double-vote after a restart.
		panic(fmt.Sprintf("raft: persist hard state: %v", err))
	}
}

// appendEntries replaces everything from ents[0].Index onward with ents.
func (n *Node) appendEntries(ents []Entry) {
	first := ents[0].Index
	if first <= n.commit {
		panic(fmt.Sprintf("raft: node %d would overwrite committed index %d (commit %d)", n.id, first, n.commit))
	}
	if err := n.cfg.Storage.Append(ents); err != nil {
		panic(fmt.Sprintf("raft: persist entries: %v", err))
	}
	n.log = append(n.log[:first], ents...)
}

func (n *Node) resetElection() {
	n.elapsed = 0
	n.timeout = n.cfg.ElectionTick + n.cfg.Rand.Intn(n.cfg.ElectionTick)
}

func (n *Node) becomeFollower(term uint64, lead int) {
	if term != n.term {
		n.term, n.vote = term, 0
		n.persistHardState()
	}
	if n.state == Leader {
		n.resetElection() // elapsed was counting heartbeats
	}
	n.state, n.lead = Follower, lead
}

func (n *Node) campaign() {
	n.state, n.lead = Candidate, 0
	n.term++
	n.vote = n.id
	n.persistHardState()
	n.resetElection()
	n.votes = map[int]bool{n.id: true}
	if n.quorum(1) {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		n.send(Message{Type: MsgVote, To: p, LastIndex: n.lastIndex(), LastTerm: n.lastTerm()})
	}
}

func (n *Node) becomeLeader() {
	n.state, n.lead, n.elapsed = Leader, n.id, 0
	n.next, n.match = map[int]uint64{}, map[int]uint64{}
	for _, p := range n.peers {
		n.next[p] = n.lastIndex() + 1
	}
	// A leader may only commit by counting replicas of its own term's entries,
	// so it appends a no-op right away to get prior-term entries committed.
	n.appendEntries([]Entry{{Index: n.lastIndex() + 1, Term: n.term}})
	n.maybeCommit()
	n.broadcastAppend()
}

func (n *Node) handleVote(m Message) {
	upToDate := m.LastTerm > n.lastTerm() || (m.LastTerm == n.lastTerm() && m.LastIndex >= n.lastIndex())
	if n.cfg.Bugs.SkipVoteLogCheck {
		upToDate = true
	}
	ok := (n.vote == 0 || n.vote == m.From) && upToDate
	if ok {
		n.vote = m.From
		n.persistHardState()
		n.resetElection()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, OK: ok})
}

func (n *Node) handleAppend(m Message) {
	reject := func(hint uint64) {
		n.send(Message{Type: MsgAppResp, To: m.From, PrevIndex: m.PrevIndex, Match: hint})
	}
	if m.PrevIndex > n.lastIndex() {
		reject(n.lastIndex())
		return
	}
	if t := n.log[m.PrevIndex].Term; t != m.PrevTerm {
		// Skip back over the whole conflicting term instead of one entry per
		// round trip. Never point below commit: that prefix matches the leader.
		i := m.PrevIndex
		for i > n.commit+1 && n.log[i-1].Term == t {
			i--
		}
		reject(i - 1)
		return
	}
	for j, e := range m.Entries {
		// Keep entries that already match. A delayed, shorter AppendEntries must
		// not truncate entries a newer one already added.
		if e.Index <= n.lastIndex() && n.log[e.Index].Term == e.Term {
			continue
		}
		n.appendEntries(m.Entries[j:])
		break
	}
	last := m.PrevIndex + uint64(len(m.Entries))
	if c := min(m.Commit, last); c > n.commit {
		n.commit = c
	}
	n.send(Message{Type: MsgAppResp, To: m.From, PrevIndex: m.PrevIndex, OK: true, Match: last})
}

func (n *Node) handleAppResp(m Message) {
	p := m.From
	if m.OK {
		if m.Match > n.match[p] {
			n.match[p] = m.Match
			n.next[p] = m.Match + 1
			n.maybeCommit()
			if n.next[p] <= n.lastIndex() {
				n.sendAppend(p)
			}
		}
		return
	}
	if m.PrevIndex != n.next[p]-1 {
		return // answer to an older probe
	}
	n.next[p] = max(n.match[p]+1, min(n.next[p]-1, m.Match+1))
	n.sendAppend(p)
}

func (n *Node) maybeCommit() {
	for i := n.lastIndex(); i > n.commit; i-- {
		if n.log[i].Term != n.term && !n.cfg.Bugs.CommitPriorTerms {
			break // terms only decrease going back, so nothing below qualifies either
		}
		count := 1
		for _, p := range n.peers {
			if n.match[p] >= i {
				count++
			}
		}
		if n.quorum(count) {
			n.commit = i
			n.broadcastAppend()
			return
		}
	}
}

func (n *Node) broadcastAppend() {
	for _, p := range n.peers {
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(p int) {
	prev := n.next[p] - 1
	end := min(n.lastIndex(), prev+uint64(n.cfg.MaxEntries))
	n.send(Message{
		Type:      MsgApp,
		To:        p,
		PrevIndex: prev,
		PrevTerm:  n.log[prev].Term,
		Entries:   append([]Entry(nil), n.log[prev+1:end+1]...),
		Commit:    n.commit,
	})
}
