package kv

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"
)

// Client sends ops to the cluster. It finds the leader by following redirects,
// and retries failed or timed-out ops with the same Seq, so each op runs at most
// once even if a retry reaches a different leader. A Client is not safe for
// concurrent use: one op at a time, like a session.
type Client struct {
	servers map[int]string
	ids     []int
	leader  int
	id      uint64
	seq     uint64
	httpc   *http.Client
}

func NewClient(servers map[int]string) *Client {
	var b [8]byte
	rand.Read(b[:])
	c := &Client{servers: servers, id: binary.LittleEndian.Uint64(b[:]) | 1, httpc: &http.Client{Timeout: 3 * time.Second}}
	for id := range servers {
		c.ids = append(c.ids, id)
	}
	sort.Ints(c.ids)
	c.leader = c.ids[0]
	return c
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.do(ctx, Op{Kind: "get", Key: key})
}

func (c *Client) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, Op{Kind: "put", Key: key, Value: value})
	return err
}

func (c *Client) Append(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, Op{Kind: "append", Key: key, Value: value})
	return err
}

func (c *Client) do(ctx context.Context, op Op) (string, error) {
	c.seq++
	op.Client, op.Seq = c.id, c.seq
	body, _ := json.Marshal(op)
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", fmt.Errorf("%w (last error: %v)", err, lastErr)
		}
		v, lead, err := c.try(ctx, c.servers[c.leader], body)
		if err == nil {
			return v, nil
		}
		lastErr = err
		if lead != 0 && lead != c.leader {
			c.leader = lead // redirected: retry at once
			continue
		}
		// No hint (election in progress, node down): try the next server.
		c.leader = c.ids[(indexOf(c.ids, c.leader)+1)%len(c.ids)]
		select {
		case <-time.After(time.Duration(min(attempt+1, 10)) * 20 * time.Millisecond):
		case <-ctx.Done():
		}
	}
}

func (c *Client) try(ctx context.Context, url string, body []byte) (string, int, error) {
	req, _ := http.NewRequestWithContext(ctx, "POST", url+"/kv", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpc.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Value  string `json:"value"`
		Error  string `json:"error"`
		Leader int    `json:"leader"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode != http.StatusOK {
		return "", out.Leader, fmt.Errorf("%s: %d %s", url, resp.StatusCode, out.Error)
	}
	return out.Value, 0, nil
}

func indexOf(xs []int, x int) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return 0
}
