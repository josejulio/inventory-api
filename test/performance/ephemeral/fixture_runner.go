package ephemeral

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

// The policy is a sanitized projection of the deployed KafkaConnector, not a
// user assertion. Refuse table CDC: inserting synthetic rows must not replicate
// unexpected relationships or send table envelopes to the logical-message SMT.
func connectorSlot(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var c map[string]string
	if err = json.Unmarshal(b, &c); err != nil {
		return "", err
	}
	if c["table.exclude.list"] != ".*" || c["table.include.list"] != "" || c["snapshot.mode"] != "no_data" || c["plugin.name"] != "pgoutput" || c["slot.name"] == "" {
		return "", fmt.Errorf("bulk fixtures require a no-data pgoutput connector excluding all table changes")
	}
	return c["slot.name"], nil
}
func (f *fixtures) drain(ctx context.Context, slot string, timeout, interval time.Duration, advance func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lsn string
	if err := f.pool.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text`).Scan(&lsn); err != nil {
		return err
	}
	// Excluded table writes alone may leave an idle connector without a new
	// offset to acknowledge. A real API event carries it beyond the setup LSN.
	if err := advance(ctx); err != nil {
		return fmt.Errorf("CDC barrier: %w", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		var ready bool
		err := f.pool.QueryRow(ctx, `SELECT active AND confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name=$1 AND database=current_database() AND plugin='pgoutput'`, slot, lsn).Scan(&ready)
		if err != nil {
			return fmt.Errorf("connector slot unavailable: %w", err)
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("CDC setup drain: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// fixtureWarmup verifies correlated consumer completion and relationship cleanup
// outside measurement, and produces a real CDC event after excluded SQL writes.
func (t *transport) fixtureWarmup(ctx context.Context, f *fixtures, warm string) error {
	// Every barrier is a new host; setup must not rely on restoration either.
	warm += "-" + uuid.NewString()
	warmWS := uuid.NewString()
	preflight, err := f.listen(ctx)
	if err != nil {
		return err
	}
	if err = t.report(ctx, warm, warmWS, 0); err == nil {
		err = t.wait(ctx, warm, warmWS, true)
	}
	if err == nil {
		txid, e := f.transactionID(ctx, t.c.Instance, warm)
		err = e
		if err == nil {
			completionCtx, cancel := context.WithTimeout(ctx, t.c.PropagationTimeout)
			err = preflight.wait(completionCtx, txid)
			cancel()
		}
	}
	preflight.close()
	if err == nil {
		err = t.remove(ctx, warm)
	}
	if err == nil {
		err = t.wait(ctx, warm, warmWS, false)
	}
	return err
}

func runFixtures(ctx context.Context, t *transport, runID string) error {
	c := t.c
	slot, err := connectorSlot(c.ConnectorPolicyFile)
	if err != nil {
		return err
	}
	f, err := openFixtures(ctx, c.DatabaseURLFile)
	if err != nil {
		return err
	}
	defer f.pool.Close()
	var baseline *Baseline
	if c.BaselineFile != "" {
		b, err := loadBaseline(c.BaselineFile)
		if err != nil {
			return err
		}
		if err = validateBaselineEntries(b, c); err != nil {
			return err
		}
		baseline = &b
	}
	var scenarioFailures []error
	for _, scenario := range c.selectedScenarios() {
		if scenario == "history_live" {
			err := runConcurrencyCases(ctx, scenario, func(workers int) error {
				return runHistoryLive(ctx, t, f, slot, runID, workers, baseline)
			})
			if err != nil {
				scenarioFailures = append(scenarioFailures, err)
			}
			continue
		}
		for _, workers := range []int{1, 4} {
			freshLifecycle := c.freshLifecycle(scenario)
			// Each scenario has its own natural-key scope and equivalent live row counts.
			// Scoped deletion and VACUUM reuse pages, but physical size may retain growth;
			// report that footprint explicitly rather than claiming a new database.
			instance := fmt.Sprintf("perf-%s-%s-%d", runID, scenario, workers)
			local := *t
			local.c.Instance = instance
			scope := Fixture{Instance: instance, Background: c.Background, Targets: []string{}}
			workspaces := make([]string, workers)
			if scenario == "history" {
				scope.History = c.History
				for w := 0; w < workers; w++ {
					id := fmt.Sprintf("%s-target-%d", runID, w)
					ws := uuid.NewString()
					scope.Targets = append(scope.Targets, id)
					workspaces[w] = ws
					if err = local.report(ctx, id, ws, 0); err == nil {
						err = local.wait(ctx, id, ws, true)
					}
					if err == nil {
						err = local.remove(ctx, id)
					}
					if err == nil {
						err = local.wait(ctx, id, ws, false)
					}
					if err != nil {
						return fmt.Errorf("API history setup: %w", err)
					}
				}
			}
			if err = f.seed(ctx, scope, c.PayloadBytes); err != nil {
				return err
			}
			if err = f.verifySeed(ctx, scope); err != nil {
				return err
			}
			if err = f.analyze(ctx); err != nil {
				return err
			}
			advance := func(barrierCtx context.Context) error {
				return local.fixtureWarmup(barrierCtx, f, runID+"-barrier-"+scenario)
			}
			if err = f.drain(ctx, slot, max(c.PropagationTimeout, 2*time.Minute), c.PollInterval, advance); err != nil {
				return err
			}
			m, err := f.metadata(ctx)
			if err != nil {
				return err
			}
			r := Result{RunID: runID, Instance: instance, Scenario: scenario, Fixture: &scope, Database: &m, Protocol: c.Protocol, Workers: workers, Profile: c.Profile, Rounds: c.Rounds, PayloadBytes: c.PayloadBytes, PollMS: c.PollInterval.Milliseconds(), RequestTimeoutMS: c.RequestTimeout.Milliseconds(), PropagationTimeoutMS: c.PropagationTimeout.Milliseconds(), WriteVisibility: "MINIMIZE_LATENCY", CheckConsistency: "CheckForUpdate", LatencyLimits: "informational; no thresholds", Operations: map[string]Operation{}}
			if freshLifecycle {
				r.ExcludedCoverage = []string{"history", "recreate"}
			}
			samples, propagation := map[string][]time.Duration{}, map[string][]time.Duration{}
			var mu sync.Mutex
			var wg sync.WaitGroup
			begin := time.Now()
			r.MeasurementStart = begin.UTC()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					id := fmt.Sprintf("%s-target-%d", runID, w)
					ws := workspaces[w]
					if ws == "" {
						ws = uuid.NewString()
					}
					generation := 0
					if scenario == "history" {
						generation = 1
					}
					fail := func(err error) { mu.Lock(); r.Failures = append(r.Failures, err.Error()); mu.Unlock() }
					listener, err := f.listen(ctx)
					if err != nil {
						fail(err)
						return
					}
					defer listener.close()
					for round := 0; round < c.Rounds; round++ {
						if freshLifecycle {
							id = fmt.Sprintf("%s-target-%d-round-%d", runID, w, round)
							ws = uuid.NewString()
						} else if round > 0 {
							if err := local.remove(ctx, id); err != nil {
								fail(fmt.Errorf("round preparation request failed"))
								return
							}
							if err := local.wait(ctx, id, ws, false); err != nil {
								fail(fmt.Errorf("round preparation propagation failed"))
								return
							}
							generation++
						}
						ops := []string{"create"}
						for u := 0; u < c.Updates; u++ {
							ops = append(ops, "workspace_update")
						}
						ops = append(ops, "unchanged_update", "delete")
						if !freshLifecycle {
							ops = append(ops, "recreate")
						}
						version := 0
						for step, op := range ops {
							oldWS := ws
							if op == "workspace_update" {
								ws = uuid.NewString()
								version++
							}
							if op == "unchanged_update" || op == "delete" {
								version++
							}
							if op == "recreate" {
								// Restoration behavior observed on image 26c644f: restoring a
								// tombstoned host with the same workspace emits an updated
								// event whose common-representation diff is empty. The DB
								// record returns, but its deleted SpiceDB relationship does
								// not. Recreate support is undecided; this legacy mode keeps
								// its propagation check for intentional investigation.
								// History setup and later rounds also restore tombstoned hosts.
								generation++
								version = 0
							}
							before := time.Now()
							var err error
							if op == "delete" {
								err = local.remove(ctx, id)
							} else {
								err = local.report(ctx, id, ws, round*len(ops)+step)
							}
							elapsed := time.Since(before)
							mu.Lock()
							s := r.Operations[op]
							if err == nil {
								samples[op] = append(samples[op], elapsed)
							} else {
								s.Errors++
								if isTimeout(err) {
									s.Timeouts++
								}
							}
							r.Operations[op] = s
							mu.Unlock()
							if err != nil {
								fail(fmt.Errorf("worker %d round %d %s request failed: %w", w, round, op, err))
								return
							}
							pipelineCtx, cancel := context.WithDeadline(ctx, before.Add(c.PropagationTimeout))
							if op != "delete" {
								txid, e := f.transactionID(pipelineCtx, instance, id)
								err = e
								if err == nil {
									err = listener.wait(pipelineCtx, txid)
								}
							}
							if err == nil {
								err = local.wait(pipelineCtx, id, ws, op != "delete")
							}
							if err == nil && op == "workspace_update" {
								err = local.wait(pipelineCtx, id, oldWS, false)
							}
							cancel()
							mu.Lock()
							if err == nil {
								propagation[op] = append(propagation[op], time.Since(before))
							} else {
								s := r.Operations[op]
								s.PropagationErrors++
								if isTimeout(err) {
									s.PropagationTimeouts++
								}
								r.Operations[op] = s
							}
							mu.Unlock()
							if err != nil {
								fail(fmt.Errorf("worker %d round %d %s propagation failed", w, round, op))
								return
							}
							if err = f.verifyState(ctx, instance, id, version, generation, op == "delete"); err != nil {
								fail(err)
								return
							}
						}

					}
					if freshLifecycle {
						return
					}
					// Verify final state before cleanup, then drain the deletion before SQL.
					if err := local.remove(ctx, id); err != nil {
						fail(fmt.Errorf("target cleanup request failed"))
						return
					}
					if err := local.wait(ctx, id, ws, false); err != nil {
						fail(fmt.Errorf("target cleanup propagation failed"))
						return
					}
				}(w)
			}
			wg.Wait()
			end := time.Now()
			r.MeasurementEnd = end.UTC()
			r.MeasurementSeconds = end.Sub(begin).Seconds()
			n := 0
			for op, s := range r.Operations {
				s.API = summarize(samples[op])
				s.Propagation = summarize(propagation[op])
				r.Operations[op] = s
				n += s.API.Count
			}
			r.Throughput = float64(n) / r.MeasurementSeconds
			r.CompletionSignal = "PostgreSQL consumer_notifications transaction ID for reports; relationship observation for deletes"
			r.RunnerRevision = c.RunnerRevision
			if len(r.Failures) == 0 {
				if err = validateFixtureResult(r, c); err != nil {
					r.Failures = append(r.Failures, err.Error())
				}
			}
			if len(r.Failures) > 0 {
				r.FailureKind = "correctness"
			}
			if baseline != nil && len(r.Failures) == 0 {
				r.LatencyLimits = "calibrated baseline; small profile skips latency gates"
				if err = checkBaseline(r, *baseline); err != nil {
					r.FailureKind = "latency"
					r.Failures = append(r.Failures, err.Error())
				}
			}
			if err = writeResult(c.ResultsDir, r); err != nil {
				return err
			}
			if len(r.Failures) > 0 {
				return fmt.Errorf("fixture workload failed: %s; see partial results", r.Failures[0])
			}
			if err = f.drain(ctx, slot, max(c.PropagationTimeout, 2*time.Minute), c.PollInterval, advance); err != nil {
				return err
			}
			if err = f.cleanup(ctx, instance); err != nil {
				return err
			}
		}
	}
	if err := runRateRounds(ctx, t, f, slot, runID); err != nil {
		scenarioFailures = append(scenarioFailures, err)
	}
	return errors.Join(scenarioFailures...)
}

// runConcurrencyCases preserves failures while allowing the next independent
// concurrency case to run. A canceled run cannot safely execute more requests.
func runConcurrencyCases(ctx context.Context, scenario string, run func(int) error) error {
	var failures []error
	completed, failedCases := 0, 0
	for _, workers := range []int{1, 4} {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			log.Printf("scenario=%s workers=%d not run: %v", scenario, workers, err)
			break
		}
		if err := run(workers); err != nil {
			failedCases++
			failures = append(failures, fmt.Errorf("%s/%d: %w", scenario, workers, err))
			log.Printf("scenario=%s workers=%d case_failed: %v; continuing if context permits", scenario, workers, err)
		}
		completed++
	}
	log.Printf("scenario=%s cases_completed=%d/2 failed_cases=%d", scenario, completed, failedCases)
	return errors.Join(failures...)
}
