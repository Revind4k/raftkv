// Command raftkv runs one member of a raftkv cluster.
//
//	raftkv -id 1 -peers 1=http://127.0.0.1:9001,2=http://127.0.0.1:9002,3=http://127.0.0.1:9003 -data data/1
//
// The node listens on the address of its own entry in -peers.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Revind4k/raftkv/kv"
	"github.com/Revind4k/raftkv/raft"
)

func main() {
	id := flag.Int("id", 0, "this node's ID")
	peersFlag := flag.String("peers", "", "every member as id=url, comma separated")
	data := flag.String("data", "", "directory for the Raft log and state")
	tick := flag.Duration("tick", 10*time.Millisecond, "Raft tick; elections time out after 10-20 ticks")
	flag.Parse()

	peers, err := parsePeers(*peersFlag)
	if err != nil || *id == 0 || peers[*id] == "" || *data == "" {
		fmt.Fprintln(os.Stderr, "usage: raftkv -id N -peers 1=http://host:port,... -data DIR")
		os.Exit(2)
	}
	u, err := url.Parse(peers[*id])
	if err != nil {
		log.Fatal(err)
	}
	st, err := raft.OpenFileStorage(*data)
	if err != nil {
		log.Fatal(err)
	}
	logger := log.New(os.Stderr, fmt.Sprintf("[node %d] ", *id), log.LstdFlags)
	srv, err := kv.NewServer(kv.ServerConfig{ID: *id, Peers: peers, Storage: st, Tick: *tick, Logger: logger})
	if err != nil {
		log.Fatal(err)
	}
	hs := &http.Server{Addr: u.Host, Handler: srv.Handler()}
	go func() {
		logger.Printf("listening on %s", u.Host)
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	hs.Close()
	srv.Close()
	st.Close()
}

func parsePeers(s string) (map[int]string, error) {
	peers := map[int]string{}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(part, "=")
		id, err := strconv.Atoi(k)
		if !ok || err != nil {
			return nil, fmt.Errorf("bad peer %q", part)
		}
		peers[id] = strings.TrimSuffix(v, "/")
	}
	return peers, nil
}
