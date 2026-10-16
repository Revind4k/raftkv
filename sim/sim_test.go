package sim

import (
	"flag"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Revind4k/raftkv/raft"
)

var seeds = flag.Int("seeds", 200, "number of seeds per cluster size in TestSimulation")

// TestSimulation runs many seeded fault schedules against 3- and 5-node clusters.
// A failure prints the seed; rerun just that one with
// go test ./sim -run 'TestSimulation/nodes=5/seed=17' -v
func TestSimulation(t *testing.T) {
	n := *seeds
	if testing.Short() {
		n = 20
	}
	for _, nodes := range []int{3, 5} {
		t.Run(fmt.Sprintf("nodes=%d", nodes), func(t *testing.T) {
			var total Stats
			for seed := int64(1); seed <= int64(n); seed++ {
				t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
					cfg := DefaultConfig(seed)
					cfg.Nodes = nodes
					res := Run(cfg)
					if res.Violation != "" {
						t.Fatal(res.Violation)
					}
					total.Ops += res.Stats.Ops
					total.Elections += res.Stats.Elections
					total.Crashes += res.Stats.Crashes
					total.Partitions += res.Stats.Partitions
					total.Retries += res.Stats.Retries
					total.Messages += res.Stats.Messages
				})
			}
			t.Logf("%d seeds: %d client ops checked, %d elections, %d crashes, %d partitions, %d client retries, %d messages",
				n, total.Ops, total.Elections, total.Crashes, total.Partitions, total.Retries, total.Messages)
		})
	}
}

func TestDeterministic(t *testing.T) {
	a, b := Run(DefaultConfig(42)), Run(DefaultConfig(42))
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different runs")
	}
	if len(a.History) == 0 {
		t.Fatal("no client ops ran")
	}
}

// The remaining tests switch off one safety mechanism each and check that the
// simulator catches the result. A checker that never fails proves nothing.
func findViolation(t *testing.T, name string, maxSeeds int64, mutate func(*Config)) {
	t.Helper()
	for seed := int64(1); seed <= maxSeeds; seed++ {
		cfg := DefaultConfig(seed)
		mutate(&cfg)
		// A liveness failure is not proof the injected bug was caught.
		if res := Run(cfg); res.Violation != "" && !strings.HasPrefix(res.Violation, "liveness") {
			t.Logf("%s caught at seed %d: %.300s", name, seed, res.Violation)
			return
		}
	}
	t.Fatalf("%s: no violation found in %d seeds", name, maxSeeds)
}

func TestCatchesSkippedVoteLogCheck(t *testing.T) {
	findViolation(t, "vote without log check", 300, func(c *Config) { c.Bugs = raft.BugInjection{SkipVoteLogCheck: true} })
}

func TestCatchesPriorTermCommit(t *testing.T) {
	// Figure 8 needs a majority to hold an old-term entry without the new
	// leader's no-op after it, then a crash at just the wrong moment. Ship one
	// entry per message and crash often; even so it takes a few hundred seeds.
	findViolation(t, "prior-term commit by count", 1500, func(c *Config) {
		c.Bugs = raft.BugInjection{CommitPriorTerms: true}
		c.Nodes, c.Ticks, c.MaxEntries = 3, 2000, 1
		c.CrashRate, c.RestartRate = 0.05, 0.15
		c.PartitionRate, c.HealRate = 0.005, 0.02
	})
}

func TestCatchesMissingDedupe(t *testing.T) {
	findViolation(t, "no duplicate detection", 300, func(c *Config) { c.SkipDedupe = true })
}
