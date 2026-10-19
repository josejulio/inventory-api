package ephemeral

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestRateProducerDoesNotWaitForCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		starts := 0
		blocked := make(chan struct{})
		done := make(chan struct{})
		go func() {
			scheduleRate(context.Background(), 6000, 50*time.Millisecond, 16, func(context.Context, int, time.Time) {
				mu.Lock()
				starts++
				mu.Unlock()
				<-blocked
			})
			close(done)
		}()
		time.Sleep(70 * time.Millisecond)
		mu.Lock()
		count := starts
		mu.Unlock()
		close(blocked)
		<-done
		if count != 5 {
			t.Fatalf("producer should start five independently scheduled writes, got %d", count)
		}
	})
}

func TestRateProducerReportsCapacityMisses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blocked := make(chan struct{})
		done := make(chan rateSchedule)
		go func() {
			done <- scheduleRate(context.Background(), 6000, 50*time.Millisecond, 1, func(context.Context, int, time.Time) { <-blocked })
		}()
		time.Sleep(70 * time.Millisecond)
		close(blocked)
		r := <-done
		if r.Scheduled != 5 || r.Missed != 4 {
			t.Fatalf("producer shortfall must be explicit: %+v", r)
		}
	})
}

func TestRateProgressUsesCorrelatedCompletions(t *testing.T) {
	start := time.Now()
	state := rateState{requests: map[int]rateRequest{
		0: {accepted: start, txid: "one"},
		1: {accepted: start.Add(time.Second), txid: "two"},
	}, completions: map[string]time.Time{"one": start.Add(2 * time.Second), "unrelated": start}}
	accepted, completed := state.counts(start.Add(3 * time.Second))
	if accepted != 2 || completed != 1 {
		t.Fatalf("unrelated completion must not drain backlog: %d/%d", accepted, completed)
	}
}
