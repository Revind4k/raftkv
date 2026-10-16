package kv

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Revind4k/raftkv/raft"
)

// member is one real cluster node: a Server on a loopback TCP port with its log
// in a temp directory.
type member struct {
	id      int
	dir     string
	addr    string
	ln      net.Listener
	srv     *Server
	http    *http.Server
	storage *raft.FileStorage
}

func (m *member) start(t *testing.T, peers map[int]string) {
	t.Helper()
	st, err := raft.OpenFileStorage(m.dir)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := NewServer(ServerConfig{ID: m.id, Peers: peers, Storage: st, Tick: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", m.addr)
	if err != nil {
		t.Fatal(err)
	}
	m.ln, m.srv, m.storage = ln, srv, st
	m.http = &http.Server{Handler: srv.Handler()}
	go m.http.Serve(ln)
}

// kill stops the node abruptly: no goodbye to peers, only fsynced state survives.
func (m *member) kill() {
	m.http.Close()
	m.srv.Close()
	m.storage.Close()
}

func startCluster(t *testing.T, n int) ([]*member, map[int]string) {
	peers := map[int]string{}
	var ms []*member
	for id := 1; id <= n; id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		ln.Close()
		peers[id] = "http://" + addr
		ms = append(ms, &member{id: id, dir: t.TempDir(), addr: addr})
	}
	for _, m := range ms {
		m.start(t, peers)
	}
	t.Cleanup(func() {
		for _, m := range ms {
			if m.srv != nil {
				m.kill()
			}
		}
	})
	return ms, peers
}

func leaderOf(t *testing.T, ms []*member) *member {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for _, m := range ms {
			if m.srv != nil && m.srv.Status().State == raft.Leader {
				return m
			}
		}
	}
	t.Fatal("no leader elected")
	return nil
}

func TestClusterOverHTTP(t *testing.T) {
	ms, peers := startCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := NewClient(peers)

	for i := 0; i < 20; i++ {
		if err := c.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Append(ctx, "log", "a"); err != nil {
		t.Fatal(err)
	}

	// Kill the leader. The other two elect a new one and keep serving.
	old := leaderOf(t, ms)
	old.kill()
	old.srv = nil
	if err := c.Append(ctx, "log", "b"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if v, err := c.Get(ctx, fmt.Sprintf("k%d", i)); err != nil || v != fmt.Sprint(i) {
			t.Fatalf("k%d = %q, %v after failover", i, v, err)
		}
	}

	// Restart it from its log on disk, then kill a different node: the
	// restarted one must now be part of every quorum, so reads only succeed if
	// it recovered its log and caught up.
	old.start(t, peers)
	time.Sleep(200 * time.Millisecond)
	var other *member
	for _, m := range ms {
		if m != old {
			other = m
			break
		}
	}
	other.kill()
	other.srv = nil
	if err := c.Append(ctx, "log", "c"); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(ctx, "log"); err != nil || v != "abc" {
		t.Fatalf("log = %q, %v; want \"abc\" (each append exactly once)", v, err)
	}
}

func TestNotLeaderRedirect(t *testing.T) {
	ms, _ := startCluster(t, 3)
	l := leaderOf(t, ms)
	for _, m := range ms {
		if m == l {
			continue
		}
		_, lead, err := m.srv.Do(context.Background(), Op{Kind: "get", Key: "x"})
		if err != ErrNotLeader || lead != l.id {
			t.Fatalf("follower %d: err=%v lead=%d, want ErrNotLeader and %d", m.id, err, lead, l.id)
		}
	}
}

func TestStoreDedupe(t *testing.T) {
	s := NewStore()
	s.Apply(Op{Kind: "append", Key: "k", Value: "x", Client: 7, Seq: 1})
	s.Apply(Op{Kind: "append", Key: "k", Value: "x", Client: 7, Seq: 1}) // retry
	if v := s.Apply(Op{Kind: "get", Key: "k", Client: 7, Seq: 2}); v != "x" {
		t.Fatalf("retried append ran twice: %q", v)
	}
	// A retried get returns the answer it got the first time.
	s.Apply(Op{Kind: "put", Key: "k", Value: "y", Client: 8, Seq: 1})
	if v := s.Apply(Op{Kind: "get", Key: "k", Client: 7, Seq: 2}); v != "x" {
		t.Fatalf("retried get = %q, want the original \"x\"", v)
	}
}

// BenchmarkPut measures one client's sequential put latency on a 3-node
// loopback cluster. Each put is an HTTP round trip to the leader, one fsync on
// the leader and on a follower, and a replication round trip.
func BenchmarkPut(b *testing.B) {
	t := &testing.T{}
	ms, peers := startCluster(t, 3)
	defer func() {
		for _, m := range ms {
			m.kill()
		}
	}()
	c := NewClient(peers)
	ctx := context.Background()
	leaderOf(t, ms)
	c.Put(ctx, "warm", "up")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Put(ctx, "k", fmt.Sprint(i)); err != nil {
			b.Fatal(err)
		}
	}
}
