package ephemeral

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
)

// RateProgress records cumulative accepted writes and correlated completions.
type RateProgress struct {
	Seconds   float64 `json:"seconds"`
	Accepted  int     `json:"accepted"`
	Completed int     `json:"completed"`
	Backlog   int     `json:"backlog"`
}

// RateRound reports an independently paced create workload, not consumer capacity.
type RateRound struct {
	TargetPerMinute       int            `json:"target_per_minute"`
	Start                 time.Time      `json:"measurement_start"`
	End                   time.Time      `json:"measurement_end"`
	Scheduled             int            `json:"scheduled"`
	ProducerMissed        int            `json:"producer_missed"`
	Accepted              int            `json:"accepted_during_measurement"`
	Completed             int            `json:"completed_during_measurement"`
	AcceptedPerMinute     float64        `json:"accepted_per_minute"`
	CompletedPerMinute    float64        `json:"completed_per_minute"`
	PeakBacklog           int            `json:"peak_backlog"`
	BacklogAtEnd          int            `json:"backlog_at_end"`
	PendingAfterDrain     int            `json:"pending_after_drain"`
	DrainSeconds          float64        `json:"drain_seconds"`
	VerifiedRelationships int            `json:"verified_relationships"`
	API                   Stats          `json:"api"`
	Consumer              Stats          `json:"consumer"`
	Progress              []RateProgress `json:"progress"`
	Failures              []string       `json:"failures,omitempty"`
	resources             []rateRequest
}

type rateSchedule struct {
	Start, End time.Time
	Scheduled  int
	Missed     int
}

// scheduleRate starts independent requests at fixed times. Full producer slots or
// scheduling delays are recorded as misses; consumer completion never paces it.
func scheduleRate(ctx context.Context, perMinute int, duration time.Duration, concurrency int, send func(context.Context, int, time.Time)) rateSchedule {
	r := rateSchedule{Start: time.Now()}
	r.End = r.Start.Add(duration)
	count := int(math.Ceil(float64(perMinute) * duration.Minutes()))
	slots := make(chan struct{}, concurrency)
	var workers sync.WaitGroup
	for index := 0; index < count; index++ {
		due := r.Start.Add(time.Duration(index) * time.Minute / time.Duration(perMinute))
		timer := time.NewTimer(max(0, time.Until(due)))
		select {
		case <-ctx.Done():
			timer.Stop()
			workers.Wait()
			return r
		case <-timer.C:
		}
		r.Scheduled++
		if time.Since(due) >= time.Minute/time.Duration(perMinute) {
			r.Missed++
			continue
		}
		select {
		case slots <- struct{}{}:
			workers.Add(1)
			go func(index int, due time.Time) {
				defer workers.Done()
				defer func() { <-slots }()
				send(ctx, index, due)
			}(index, due)
		default:
			r.Missed++
		}
	}
	timer := time.NewTimer(max(0, time.Until(r.End)))
	select {
	case <-ctx.Done():
		timer.Stop()
	case <-timer.C:
	}
	workers.Wait()
	return r
}

type rateRequest struct {
	id, workspace, txid string
	started, accepted   time.Time
	api                 time.Duration
	err                 error
}

type rateState struct {
	mu          sync.Mutex
	requests    map[int]rateRequest
	completions map[string]time.Time
	listenError error
}

// counts excludes unrelated notifications and includes only writes accepted by at.
// Callers hold mu while requests or notifications can still be arriving.
func (s *rateState) counts(at time.Time) (accepted, completed int) {
	for _, request := range s.requests {
		if request.accepted.IsZero() || request.accepted.After(at) {
			continue
		}
		accepted++
		if completion, ok := s.completions[request.txid]; ok && !completion.After(at) {
			completed++
		}
	}
	return
}

func runRateRound(ctx context.Context, t *transport, f *fixtures, rate int) (RateRound, error) {
	r := RateRound{TargetPerMinute: rate}
	listener, err := f.listen(ctx)
	if err != nil {
		return r, err
	}
	defer listener.close()
	listenCtx, stopListener := context.WithCancel(ctx)
	defer stopListener()
	state := rateState{requests: map[int]rateRequest{}, completions: map[string]time.Time{}}
	listening := make(chan struct{})
	go func() {
		defer close(listening)
		for {
			notification, err := listener.conn.WaitForNotification(listenCtx)
			if err != nil {
				if listenCtx.Err() == nil {
					state.mu.Lock()
					state.listenError = fmt.Errorf("consumer notification stream failed")
					state.mu.Unlock()
				}
				return
			}
			state.mu.Lock()
			if len(state.completions) >= 110000 {
				state.listenError = fmt.Errorf("consumer notification tracking limit exceeded")
				state.mu.Unlock()
				return
			}
			if _, exists := state.completions[notification.Payload]; !exists {
				state.completions[notification.Payload] = time.Now()
			}
			state.mu.Unlock()
		}
	}()
	// API calls and their DB transaction-ID lookups occupy producer slots only.
	// No slot waits for consumer completion or relationship verification.
	scheduled := scheduleRate(ctx, rate, t.c.RateDuration, 32, func(ctx context.Context, index int, due time.Time) {
		request := rateRequest{id: uuid.NewString(), workspace: uuid.NewString(), started: time.Now()}
		err := t.report(ctx, request.id, request.workspace, index)
		request.api = time.Since(request.started)
		if err == nil {
			request.accepted = time.Now()
		}
		state.mu.Lock()
		state.requests[index] = request
		state.mu.Unlock()
		if err == nil {
			lookupCtx, cancel := context.WithTimeout(ctx, t.c.RequestTimeout)
			request.txid, err = f.transactionID(lookupCtx, t.c.Instance, request.id)
			cancel()
		}
		request.err = err
		state.mu.Lock()
		state.requests[index] = request
		state.mu.Unlock()
	})
	r.Start, r.End, r.Scheduled, r.ProducerMissed = scheduled.Start.UTC(), scheduled.End.UTC(), scheduled.Scheduled, scheduled.Missed
	if ctx.Err() != nil {
		stopListener()
		<-listening
		return r, ctx.Err()
	}
	deadline := time.Now().Add(t.c.PropagationTimeout)
	for {
		state.mu.Lock()
		accepted, completed := state.counts(time.Now())
		listenError := state.listenError
		state.mu.Unlock()
		if accepted == completed || listenError != nil || time.Now().After(deadline) {
			break
		}
		timer := time.NewTimer(t.c.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			break
		}
	}
	stopListener()
	<-listening
	r.DrainSeconds = time.Since(scheduled.End).Seconds()
	r.Accepted, r.Completed = state.counts(scheduled.End)
	accepted, completed := state.counts(time.Now())
	r.BacklogAtEnd = r.Accepted - r.Completed
	r.PendingAfterDrain = accepted - completed
	r.AcceptedPerMinute = float64(r.Accepted) / t.c.RateDuration.Minutes()
	r.CompletedPerMinute = float64(r.Completed) / t.c.RateDuration.Minutes()
	for second := time.Second; second <= t.c.RateDuration; second += time.Second {
		accepted, completed := state.counts(scheduled.Start.Add(second))
		r.Progress = append(r.Progress, RateProgress{Seconds: second.Seconds(), Accepted: accepted, Completed: completed, Backlog: accepted - completed})
		r.PeakBacklog = max(r.PeakBacklog, accepted-completed)
	}
	var api, consumer []time.Duration
	for _, request := range state.requests {
		if request.err != nil {
			r.Failures = append(r.Failures, "API write or transaction-ID lookup failed")
		}
		if !request.accepted.IsZero() {
			r.resources = append(r.resources, request)
			api = append(api, request.api)
		}
		if completion, ok := state.completions[request.txid]; ok {
			consumer = append(consumer, completion.Sub(request.started))
		}
	}
	r.API, r.Consumer = summarize(api), summarize(consumer)
	if state.listenError != nil {
		r.Failures = append(r.Failures, state.listenError.Error())
	}
	if r.ProducerMissed > 0 || r.Accepted != r.Scheduled {
		r.Failures = append(r.Failures, "producer did not sustain the scheduled arrival rate")
	}
	if r.PendingAfterDrain > 0 {
		r.Failures = append(r.Failures, "consumer backlog did not drain before timeout")
	}
	// Graph checks happen after the production/drain window. Each reported host
	// has its own identity, so no later update can hide an earlier report.
	for _, request := range state.requests {
		if request.accepted.IsZero() {
			continue
		}
		if err := t.wait(ctx, request.id, request.workspace, true); err != nil {
			r.Failures = append(r.Failures, "post-round relationship verification failed")
			break
		}
		r.VerifiedRelationships++
	}
	return r, ctx.Err()
}

func runRateRounds(ctx context.Context, t *transport, f *fixtures, slot, runID string) error {
	if len(t.c.MessageRates) == 0 {
		return nil
	}
	local := *t
	local.c.Instance = fmt.Sprintf("perf-%s-rate", runID)
	scope := Fixture{Instance: local.c.Instance, Background: t.c.Background, Targets: []string{}}
	if err := f.seed(ctx, scope, t.c.PayloadBytes); err != nil {
		return err
	}
	if err := f.verifySeed(ctx, scope); err != nil {
		return err
	}
	if err := f.analyze(ctx); err != nil {
		return err
	}
	advance := func(ctx context.Context) error { return local.fixtureWarmup(ctx, f, uuid.NewString()) }
	if err := f.drain(ctx, slot, max(t.c.PropagationTimeout, 2*time.Minute), t.c.PollInterval, advance); err != nil {
		return err
	}
	var rounds []RateRound
	var resources []rateRequest
	failed := false
	for index, rate := range t.c.MessageRates {
		fmt.Printf("Rate round %d: %d independently paced creates/minute for %s\n", index+1, rate, t.c.RateDuration)
		round, err := runRateRound(ctx, &local, f, rate)
		resources = append(resources, round.resources...)
		rounds = append(rounds, round)
		data, marshalError := json.MarshalIndent(map[string]any{"rounds": rounds, "scope": "independently paced API creates; correlated consumer completion; graph checks after production"}, "", "  ")
		if marshalError != nil {
			return marshalError
		}
		if writeError := os.WriteFile(filepath.Join(t.c.ResultsDir, "rate-rounds.json"), data, 0644); writeError != nil {
			return writeError
		}
		if err != nil {
			return err
		}
		failed = failed || len(round.Failures) > 0
		if round.PendingAfterDrain > 0 {
			break
		} // Do not carry an undrained queue into another round.
	}
	// All cleanup traffic is outside every production window.
	if err := cleanupRateResources(ctx, &local, resources); err != nil {
		return err
	}
	if err := f.drain(ctx, slot, max(t.c.PropagationTimeout, 2*time.Minute), t.c.PollInterval, advance); err != nil {
		return err
	}
	if err := f.cleanup(ctx, scope.Instance); err != nil {
		return err
	}
	if failed {
		return fmt.Errorf("rate rounds had producer, consumer or verification failures; see rate-rounds.json")
	}
	return nil
}

func cleanupRateResources(ctx context.Context, t *transport, resources []rateRequest) error {
	jobs := make(chan rateRequest)
	var workers sync.WaitGroup
	var mu sync.Mutex
	var failure error
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for request := range jobs {
				err := t.remove(ctx, request.id)
				if err == nil {
					err = t.wait(ctx, request.id, request.workspace, false)
				}
				if err != nil {
					mu.Lock()
					failure = fmt.Errorf("rate fixture API cleanup or graph absence check failed")
					mu.Unlock()
				}
			}
		}()
	}
	for _, request := range resources {
		jobs <- request
	}
	close(jobs)
	workers.Wait()
	return failure
}
