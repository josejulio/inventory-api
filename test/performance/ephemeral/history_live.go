package ephemeral

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// seedLive copies the API-established representation rather than inventing a
// payload. Depth includes the original version zero; all rows remain live in
// generation zero and reference the same validated common representation.
func (f *fixtures) seedLive(ctx context.Context, scope Fixture, depth int) error {
	if depth < 2 || len(scope.Targets) == 0 || scope.Instance == "" {
		return fmt.Errorf("invalid live history scope")
	}
	// Explicit parent-row locks preserve the API-established state atomically.
	// Serializable predicate locks on the growing history index can exhaust the
	// shared lock pool during bulk setup; setup does not require SSI tracking.
	tx, err := f.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, id := range scope.Targets {
		var reporterID string
		err = tx.QueryRow(ctx, `SELECT rr.id::text FROM reporter_resources rr JOIN resource r ON r.id=rr.resource_id JOIN reporter_representations rp ON rp.reporter_resource_id=rr.id AND rp.version=rr.representation_version AND rp.generation=rr.generation JOIN common_representations cr ON cr.resource_id=r.id AND cr.version=rp.common_version WHERE rr.local_resource_id=$1 AND rr.reporter_instance_id=$2 AND rr.reporter_type='hbi' AND rr.resource_type='host' AND rr.representation_version=0 AND rr.generation=0 AND NOT rr.tombstone AND NOT rp.tombstone AND rp.data IS NOT NULL AND rp.transaction_id IS NOT NULL AND rp.transaction_id<>'' AND rp.common_version=r.common_version AND (SELECT count(*) FROM reporter_representations WHERE reporter_resource_id=rr.id)=1 FOR UPDATE OF rr,r`, id, scope.Instance).Scan(&reporterID)
		if err != nil {
			return fmt.Errorf("invalid API live history precondition: %w", err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO reporter_representations (reporter_resource_id,version,generation,data,common_version,reporter_version,transaction_id,tombstone,created_at) SELECT rp.reporter_resource_id,v,rp.generation,rp.data,rp.common_version,rp.reporter_version,$3||':history:'||$1||':'||v,false,rp.created_at FROM reporter_representations rp CROSS JOIN generate_series(1,$2::int) v WHERE rp.reporter_resource_id=$1::uuid AND rp.version=0 AND rp.generation=0`, reporterID, depth-1, scope.Instance)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE reporter_resources SET representation_version=$2 WHERE id=$1::uuid`, reporterID, depth-1); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// verifyLive joins version zero once per target, avoiding a correlated source
// lookup for every historical row while checking payload and metadata equality.
func (f *fixtures) verifyLive(ctx context.Context, scope Fixture, depth int) error {
	for _, id := range scope.Targets {
		var valid bool
		err := f.pool.QueryRow(ctx, `SELECT rr.representation_version=$3-1 AND rr.generation=0 AND NOT rr.tombstone AND (SELECT count(*)=$3 AND min(version)=0 AND max(version)=$3-1 AND bool_and(generation=0 AND NOT tombstone AND data IS NOT NULL AND transaction_id IS NOT NULL AND transaction_id<>'' AND common_version=r.common_version AND data=original.data AND reporter_version IS NOT DISTINCT FROM original.reporter_version) FROM reporter_representations WHERE reporter_resource_id=rr.id) AND EXISTS (SELECT 1 FROM common_representations WHERE resource_id=r.id AND version=r.common_version) FROM reporter_resources rr JOIN resource r ON r.id=rr.resource_id JOIN reporter_representations original ON original.reporter_resource_id=rr.id AND original.version=0 AND original.generation=0 WHERE rr.reporter_instance_id=$1 AND rr.local_resource_id=$2 AND rr.reporter_type='hbi' AND rr.resource_type='host'`, scope.Instance, id, depth).Scan(&valid)
		if err != nil {
			return err
		}
		if !valid {
			return fmt.Errorf("invalid live history depth or current state")
		}
	}
	return nil
}

// runHistoryLive bounds replacement batches to a configured number of rounds. Every
// batch has its own measurement window and deletes each target exactly once.
func runHistoryLive(ctx context.Context, t *transport, f *fixtures, slot, runID string, workers int, baseline *Baseline) error {
	c := t.c
	if workers == 4 && c.HistoryDepthFourWorkers != 0 {
		c.HistoryDepth = c.HistoryDepthFourWorkers
	}
	started := time.Now()
	phase := "starting"
	batchNumber := 0
	totalBatches := (c.Rounds + c.HistoryBatchRounds - 1) / c.HistoryBatchRounds
	progress := func(next, details string) {
		phase = next
		log.Printf("scenario=history_live workers=%d batch=%d/%d elapsed=%s phase=%s %s", workers, batchNumber, totalBatches, time.Since(started).Round(time.Millisecond), phase, details)
	}
	progress("starting", fmt.Sprintf("rounds=%d depth=%d background=%d", c.Rounds, c.HistoryDepth, c.Background))
	local := *t
	local.c.Instance = fmt.Sprintf("perf-%s-history_live-%d", runID, workers)
	scope := Fixture{Instance: local.c.Instance, Background: c.Background, Targets: []string{}}
	r := Result{RunID: runID, Instance: scope.Instance, Scenario: "history_live", Fixture: &scope, Protocol: c.Protocol, Workers: workers, Profile: c.Profile, Rounds: c.Rounds, PayloadBytes: c.PayloadBytes, RunnerRevision: c.RunnerRevision, Operations: map[string]Operation{}, CompletionSignal: "PostgreSQL consumer_notifications transaction ID for reports; relationship observation and persisted tombstone for deletes", LatencyLimits: "informational; no thresholds", WriteVisibility: "MINIMIZE_LATENCY", CheckConsistency: "CheckForUpdate", PollMS: c.PollInterval.Milliseconds(), RequestTimeoutMS: c.RequestTimeout.Milliseconds(), PropagationTimeoutMS: c.PropagationTimeout.Milliseconds()}
	api, pipeline, completion := map[string][]time.Duration{}, map[string][]time.Duration{}, map[string][]time.Duration{}
	var mu sync.Mutex
	fail := func(err error) {
		mu.Lock()
		r.Failures = append(r.Failures, err.Error())
		r.FailureKind = "correctness"
		mu.Unlock()
	}
	// Preserve partial samples even when a later setup or cleanup fails.
	execute := func() error {
		setup := time.Now()
		setupPending := true
		var cleanup time.Time
		// Preserve time spent in a phase even when it fails before measurement.
		defer func() {
			if setupPending {
				r.SetupSeconds += time.Since(setup).Seconds()
			}
			if !cleanup.IsZero() {
				r.CleanupSeconds += time.Since(cleanup).Seconds()
			}
		}()
		progress("background_setup", fmt.Sprintf("resources=%d", scope.Background))
		if err := f.seed(ctx, scope, c.PayloadBytes); err != nil {
			return err
		}
		if err := f.verifySeed(ctx, scope); err != nil {
			return err
		}
		r.SetupSeconds += time.Since(setup).Seconds()
		setupPending = false
		for round := 0; round < c.Rounds; round += c.HistoryBatchRounds {
			batchNumber++
			batchRounds := min(c.HistoryBatchRounds, c.Rounds-round)
			progress("target_setup", fmt.Sprintf("rounds=%d-%d/%d targets=%d", round+1, round+batchRounds, c.Rounds, workers*batchRounds))
			batch := Fixture{Instance: scope.Instance, Targets: []string{}}
			workspaces := make([]string, workers*batchRounds)
			setup = time.Now()
			setupPending = true
			for w := 0; w < workers*batchRounds; w++ {
				id, ws := uuid.NewString(), uuid.NewString()
				batch.Targets = append(batch.Targets, id)
				workspaces[w] = ws
				listener, err := f.listen(ctx)
				if err != nil {
					return err
				}
				err = local.report(ctx, id, ws, 0)
				if err == nil {
					txid, e := f.transactionID(ctx, scope.Instance, id)
					err = e
					if err == nil {
						waitCtx, cancel := context.WithTimeout(ctx, c.PropagationTimeout)
						err = listener.wait(waitCtx, txid)
						cancel()
					}
				}
				listener.close()
				if err == nil {
					err = local.wait(ctx, id, ws, true)
				}
				if err != nil {
					return fmt.Errorf("live API setup: %w", err)
				}
			}
			progress("history_seed", fmt.Sprintf("targets=%d depth=%d rows=%d", len(batch.Targets), c.HistoryDepth, len(batch.Targets)*c.HistoryDepth))
			if err := f.seedLive(ctx, batch, c.HistoryDepth); err != nil {
				return err
			}
			progress("history_verify", "")
			if err := f.verifyLive(ctx, batch, c.HistoryDepth); err != nil {
				return err
			}
			scope.HistoryDepth = c.HistoryDepth
			scope.History = c.HistoryDepth * workers * batchRounds
			scope.Targets = append([]string(nil), batch.Targets...)
			r.SeededHistoryRows += int64(scope.History)
			progress("vacuum_analyze", "")
			if err := f.analyze(ctx); err != nil {
				return err
			}
			advance := func(ctx context.Context) error { return local.fixtureWarmup(ctx, f, runID+"-live-barrier") }
			progress("setup_cdc_drain", "")
			if err := f.drain(ctx, slot, max(c.PropagationTimeout, 2*time.Minute), c.PollInterval, advance); err != nil {
				return err
			}
			m, err := f.metadata(ctx)
			if err != nil {
				return err
			}
			if r.Database == nil || m.Bytes > r.Database.Bytes {
				r.Database = &m
			}
			listeners := make([]*completionListener, workers)
			for w := range listeners {
				listener, err := f.listen(ctx)
				if err != nil {
					for _, opened := range listeners {
						if opened != nil {
							opened.close()
						}
					}
					return err
				}
				listeners[w] = listener
			}
			r.SetupSeconds += time.Since(setup).Seconds()
			setupPending = false
			progress("measurement", fmt.Sprintf("rounds=%d-%d/%d", round+1, round+batchRounds, c.Rounds))
			begin := time.Now()
			if r.MeasurementStart.IsZero() {
				r.MeasurementStart = begin.UTC()
			}
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					listener := listeners[w]
					var err error
					for batchRound := 0; batchRound < batchRounds; batchRound++ {
						index := batchRound*workers + w
						id, ws := batch.Targets[index], workspaces[index]
						version := c.HistoryDepth - 1
						ops := []string{}
						for u := 0; u < c.Updates; u++ {
							ops = append(ops, "workspace_update", "reporter_update")
						}
						ops = append(ops, "delete")
						for step, op := range ops {
							oldWS := ws
							if op == "workspace_update" {
								ws = uuid.NewString()
							}
							before := time.Now()
							if op == "delete" {
								err = local.remove(ctx, id)
							} else {
								err = local.report(ctx, id, ws, step+1)
							}
							elapsed := time.Since(before)
							if err != nil {
								fail(fmt.Errorf("%s request: %w", op, err))
								return
							}
							mu.Lock()
							api[op] = append(api[op], elapsed)
							mu.Unlock()
							waitCtx, cancel := context.WithDeadline(ctx, before.Add(c.PropagationTimeout))
							if op != "delete" {
								txid, e := f.transactionID(waitCtx, scope.Instance, id)
								err = e
								if err == nil {
									err = listener.wait(waitCtx, txid)
								}
								if err == nil {
									mu.Lock()
									completion[op] = append(completion[op], time.Since(before))
									mu.Unlock()
								}
							}
							if err == nil {
								err = local.wait(waitCtx, id, ws, op != "delete")
							}
							if err == nil && op == "workspace_update" {
								err = local.wait(waitCtx, id, oldWS, false)
							}
							cancel()
							if err != nil {
								fail(fmt.Errorf("%s completion: %w", op, err))
								return
							}
							version++
							if err = f.verifyState(ctx, scope.Instance, id, version, 0, op == "delete"); err != nil {
								fail(err)
								return
							}
							mu.Lock()
							pipeline[op] = append(pipeline[op], time.Since(before))
							mu.Unlock()
						}
					}
				}(w)
			}
			wg.Wait()
			end := time.Now()
			for _, listener := range listeners {
				listener.close()
			}
			r.CleanupSeconds += time.Since(end).Seconds()
			r.MeasurementEnd = end.UTC()
			r.MeasurementSeconds += end.Sub(begin).Seconds()
			r.MeasurementWindows = append(r.MeasurementWindows, MeasurementWindow{Start: begin.UTC(), End: end.UTC()})
			if len(r.Failures) > 0 {
				return fmt.Errorf("live history batch correctness failure")
			}
			progress("batch_cleanup", fmt.Sprintf("measurement_duration=%s rounds_measured=%d/%d", end.Sub(begin).Round(time.Millisecond), round+batchRounds, c.Rounds))
			cleanup = time.Now()
			if err := f.drain(ctx, slot, max(c.PropagationTimeout, 2*time.Minute), c.PollInterval, advance); err != nil {
				return err
			}
			// The next batch gets new identities; retain background rows while removing
			// only this batch's completed targets. Barriers remain until final cleanup.
			if err := f.cleanupTargets(ctx, scope.Instance, batch.Targets); err != nil {
				return err
			}
			r.CleanupSeconds += time.Since(cleanup).Seconds()
			cleanup = time.Time{}
		}
		progress("final_cleanup", "")
		cleanup = time.Now()
		if err := f.cleanup(ctx, scope.Instance); err != nil {
			return err
		}
		r.CleanupSeconds += time.Since(cleanup).Seconds()
		cleanup = time.Time{}
		return nil
	}
	if err := execute(); err != nil {
		log.Printf("scenario=history_live workers=%d batch=%d/%d phase=%s failed: %v", workers, batchNumber, totalBatches, phase, err)
		fail(err)
	}
	n := 0
	for _, op := range scenarioOperations("history_live", false) {
		operation := r.Operations[op]
		operation.API = summarize(api[op])
		operation.Propagation = summarize(pipeline[op])
		operation.Completion = summarize(completion[op])
		r.Operations[op] = operation
		n += len(api[op])
	}
	if r.MeasurementSeconds > 0 {
		r.Throughput = float64(n) / r.MeasurementSeconds
	}
	if len(r.Failures) == 0 {
		if err := validateFixtureResult(r, c); err != nil {
			fail(err)
		}
	}
	progress("result_validation", "")
	if len(r.Failures) == 0 && baseline != nil {
		r.LatencyLimits = "calibrated baseline; small profile skips latency gates"
		if err := checkBaseline(r, *baseline); err != nil {
			r.FailureKind = "latency"
			r.Failures = append(r.Failures, err.Error())
		}
	}
	if err := writeResult(c.ResultsDir, r); err != nil {
		return err
	}
	if len(r.Failures) > 0 {
		progress("failed", fmt.Sprintf("kind=%s", r.FailureKind))
		return fmt.Errorf("history_live %s failure: %s", r.FailureKind, r.Failures[0])
	}
	progress("complete", fmt.Sprintf("measured_calls=%d measurement_seconds=%.2f", n, r.MeasurementSeconds))
	return nil
}
