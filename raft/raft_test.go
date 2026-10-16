package raft

import (
	"math/rand"
	"testing"
)

// cluster delivers every message instantly, in order, with no faults. The
// randomized tests live in package sim; these pin down specific behaviors.
type cluster struct {
	nodes map[int]*Node
	store map[int]*MemoryStorage
	cut   map[int]bool // isolated nodes
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{nodes: map[int]*Node{}, store: map[int]*MemoryStorage{}, cut: map[int]bool{}}
	for id := 1; id <= n; id++ {
		c.store[id] = NewMemoryStorage()
		c.start(t, id, n)
	}
	return c
}

func (c *cluster) start(t *testing.T, id, n int) {
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i + 1
	}
	node, err := New(Config{ID: id, Peers: peers, Storage: c.store[id], Rand: rand.New(rand.NewSource(int64(id)))})
	if err != nil {
		t.Fatal(err)
	}
	c.nodes[id] = node
}

// run ticks every node and delivers messages until nothing is in flight.
func (c *cluster) run(ticks int) {
	for i := 0; i < ticks; i++ {
		for id := 1; id <= len(c.nodes); id++ {
			c.nodes[id].Tick()
		}
		c.flush()
	}
}

func (c *cluster) flush() {
	for again := true; again; {
		again = false
		for id := 1; id <= len(c.nodes); id++ {
			for _, m := range c.nodes[id].Ready().Messages {
				again = true
				if !c.cut[m.From] && !c.cut[m.To] {
					c.nodes[m.To].Step(m)
				}
			}
		}
	}
}

func (c *cluster) leader(t *testing.T) *Node {
	t.Helper()
	var lead *Node
	for id, n := range c.nodes {
		if n.state == Leader && !c.cut[id] {
			if lead != nil && lead.term == n.term {
				t.Fatalf("two leaders in term %d", n.term)
			}
			if lead == nil || n.term > lead.term {
				lead = n
			}
		}
	}
	if lead == nil {
		t.Fatal("no leader")
	}
	return lead
}

func TestElectsOneLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.run(30)
	l := c.leader(t)
	for _, n := range c.nodes {
		if n.term != l.term || n.lead != l.id {
			t.Fatalf("node %d: term %d lead %d, want term %d lead %d", n.id, n.term, n.lead, l.term, l.id)
		}
	}
}

func TestReplicatesAndCommits(t *testing.T) {
	c := newCluster(t, 3)
	c.run(30)
	l := c.leader(t)
	idx, _, ok := l.Propose([]byte("x"))
	if !ok {
		t.Fatal("leader refused proposal")
	}
	c.flush()
	for _, n := range c.nodes {
		if n.commit < idx {
			c.run(1) // followers learn the commit index on the next heartbeat
		}
		if n.commit < idx || string(n.log[idx].Data) != "x" {
			t.Fatalf("node %d: commit %d, entry %q", n.id, n.commit, n.log[idx].Data)
		}
	}
}

func TestFollowerRefusesProposal(t *testing.T) {
	c := newCluster(t, 3)
	c.run(30)
	l := c.leader(t)
	for _, n := range c.nodes {
		if n != l {
			if _, _, ok := n.Propose([]byte("x")); ok {
				t.Fatal("follower accepted a proposal")
			}
		}
	}
}

// A minority leader can append but never commit; once the partition heals its
// uncommitted entries are replaced by the majority's.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	c.run(30)
	old := c.leader(t)
	c.cut[old.id] = true
	idx, _, _ := old.Propose([]byte("lost"))
	c.run(50)
	if old.commit >= idx {
		t.Fatal("isolated leader committed")
	}
	nl := c.leader(t)
	if nl == old {
		t.Fatal("majority did not elect a new leader")
	}
	nl.Propose([]byte("kept"))
	c.flush()
	delete(c.cut, old.id)
	c.run(30)
	for _, e := range old.log[1:] {
		if string(e.Data) == "lost" {
			t.Fatal("old leader kept its uncommitted entry")
		}
	}
	if old.state == Leader {
		t.Fatal("old leader did not step down")
	}
}

// A node with a stale log must not win an election (Raft §5.4.1).
func TestStaleNodeCannotWin(t *testing.T) {
	c := newCluster(t, 3)
	c.run(30)
	l := c.leader(t)
	var stale *Node
	for _, n := range c.nodes {
		if n != l {
			stale = n
			break
		}
	}
	c.cut[stale.id] = true
	l.Propose([]byte("a"))
	c.flush()
	c.run(2)
	delete(c.cut, stale.id)
	// Force the stale node to campaign right away.
	stale.campaign()
	c.flush()
	if stale.state == Leader {
		t.Fatal("node missing a committed entry won the election")
	}
}

func TestRestartKeepsVoteAndLog(t *testing.T) {
	c := newCluster(t, 3)
	c.run(30)
	l := c.leader(t)
	l.Propose([]byte("a"))
	c.flush()
	term, vote, n := l.term, l.vote, len(l.log)
	c.start(t, l.id, 3)
	r := c.nodes[l.id]
	if r.term != term || r.vote != vote || len(r.log) != n {
		t.Fatalf("after restart term=%d vote=%d len=%d, want %d %d %d", r.term, r.vote, len(r.log), term, vote, n)
	}
	if r.state != Follower || r.commit != 0 {
		t.Fatal("restarted node should start as follower with commit 0")
	}
	c.run(30)
	if r.commit < uint64(n-1) {
		t.Fatalf("restarted node did not relearn commit: %d", r.commit)
	}
}

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1)
	c.run(30)
	l := c.leader(t)
	idx, _, _ := l.Propose([]byte("solo"))
	rd := l.Ready()
	if l.commit != idx || len(rd.Committed) == 0 || string(rd.Committed[len(rd.Committed)-1].Data) != "solo" {
		t.Fatalf("single node did not commit its own entry: %+v", rd)
	}
}
