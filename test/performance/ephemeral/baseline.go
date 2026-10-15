package ephemeral

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
)

// Baseline contains the maximum allowed p95 latency for each measured case.
type Baseline struct {
	Limits map[string]float64 `json:"p95_limits_ms"`
}

// Minimum sample coverage is a harness rule, independent of the chosen limits.
const minimumBaselineSamples = 100

func loadBaseline(path string) (Baseline, error) {
	var b Baseline
	data, err := os.ReadFile(path)
	if err != nil {
		return b, err
	}
	if err = json.Unmarshal(data, &b); err != nil {
		return b, err
	}
	return b, nil
}
func checkBaseline(r Result, b Baseline) error {
	prefix := fmt.Sprintf("%s/%s/%d/", r.Protocol, r.Scenario, r.Workers)
	measured := map[string]Stats{}
	for op, s := range r.Operations {
		for _, kind := range baselineKinds(r.Scenario, op) {
			stats := map[string]Stats{"api": s.API, "propagation": s.Propagation, "consumer_completion": s.Completion}
			measured[prefix+op+"/"+kind] = stats[kind]
		}
	}
	for key, s := range measured {
		limit, ok := b.Limits[key]
		if !ok || limit <= 0 || math.IsNaN(limit) || math.IsInf(limit, 0) {
			return fmt.Errorf("missing or invalid baseline entry: %s", key)
		}
		if r.Profile == "small" {
			continue
		}
		if s.Count < minimumBaselineSamples {
			return fmt.Errorf("%s has %d samples; baseline requires %d", key, s.Count, minimumBaselineSamples)
		}
		if s.P95MS > limit {
			return fmt.Errorf("%s p95 %.2f ms exceeds %.2f ms", key, s.P95MS, limit)
		}
	}
	for key := range b.Limits {
		if strings.HasPrefix(key, prefix) {
			if _, ok := measured[key]; !ok {
				return fmt.Errorf("stale baseline entry: %s", key)
			}
		}
	}
	return nil
}

func validateFixtureResult(r Result, c Config) error {
	operations := scenarioOperations(r.Scenario, c.freshLifecycle(r.Scenario))
	for _, op := range operations {
		s, ok := r.Operations[op]
		expected := r.Workers * c.Rounds
		if op == "workspace_update" || op == "reporter_update" {
			expected *= c.Updates
		}
		if r.Scenario == "history_live" && op != "delete" && s.Completion.Count != expected {
			return fmt.Errorf("missing correlated completion samples for %s", op)
		}
		if !ok || s.API.Count != expected || s.Propagation.Count != expected || s.Errors != 0 || s.Timeouts != 0 || s.PropagationErrors != 0 || s.PropagationTimeouts != 0 {
			return fmt.Errorf("invalid or missing samples for %s: expected %d API and pipeline samples", op, expected)
		}
	}
	return nil
}

func validateBaselineEntries(b Baseline, c Config) error {
	expected := map[string]bool{}
	for _, scenario := range c.selectedScenarios() {
		operations := scenarioOperations(scenario, c.freshLifecycle(scenario))
		for _, workers := range []int{1, 4} {
			for _, op := range operations {
				kinds := baselineKinds(scenario, op)
				for _, kind := range kinds {
					expected[fmt.Sprintf("%s/%s/%d/%s/%s", c.Protocol, scenario, workers, op, kind)] = true
				}
			}
		}
	}
	for key := range expected {
		limit, ok := b.Limits[key]
		if !ok || limit <= 0 || math.IsNaN(limit) || math.IsInf(limit, 0) {
			return fmt.Errorf("missing or invalid baseline entry: %s", key)
		}
	}
	for key := range b.Limits {
		if !expected[key] {
			return fmt.Errorf("stale baseline entry: %s", key)
		}
	}
	return nil
}

// History gates target consumer completion. Deletes have no correlated completion
// record, so their pipeline observation is the available completion signal.
func baselineKinds(scenario, operation string) []string {
	if scenario == "history_live" {
		if operation == "delete" {
			return []string{"propagation"}
		}
		return []string{"consumer_completion"}
	}
	return []string{"api", "propagation"}
}
