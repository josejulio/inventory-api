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
		measured[prefix+op+"/api"] = s.API
		measured[prefix+op+"/propagation"] = s.Propagation
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
	operations := []string{"create", "workspace_update", "unchanged_update", "delete", "recreate"}
	if c.PartialCalibration {
		operations = operations[:4]
	}
	for _, op := range operations {
		s, ok := r.Operations[op]
		expected := r.Workers * c.Rounds
		if op == "workspace_update" {
			expected *= c.Updates
		}
		if !ok || s.API.Count != expected || s.Propagation.Count != expected || s.Errors != 0 || s.Timeouts != 0 || s.PropagationErrors != 0 || s.PropagationTimeouts != 0 {
			return fmt.Errorf("invalid or missing samples for %s: expected %d API and pipeline samples", op, expected)
		}
	}
	return nil
}

func validateBaselineEntries(b Baseline, c Config) error {
	expected := map[string]bool{}
	scenarios := []string{"fresh", "history"}
	operations := []string{"create", "workspace_update", "unchanged_update", "delete", "recreate"}
	if c.PartialCalibration {
		scenarios = scenarios[:1]
		operations = operations[:4]
	}
	for _, scenario := range scenarios {
		for _, workers := range []int{1, 4} {
			for _, op := range operations {
				for _, kind := range []string{"api", "propagation"} {
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
