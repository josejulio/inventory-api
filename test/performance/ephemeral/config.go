// Package ephemeral provides the in-cluster performance harness. It does not
// start an API, replace authorization, or execute the local performance fixture.
package ephemeral

import (
	"fmt"
	"time"
)

// Config describes lifecycle workloads and optional scoped database fixtures.
type Config struct {
	HistoryDeleteRounds     int           `json:"history_delete_rounds,omitempty"`
	HistoryDepthFourWorkers int           `json:"history_depth_four_workers,omitempty"`
	HistoryBatchRounds      int           `json:"history_batch_rounds,omitempty"`
	Scenarios               []string      `json:"scenarios,omitempty"`
	HistoryDepth            int           `json:"history_depth,omitempty"`
	MessageRates            []int         `json:"message_rates,omitempty"`
	RateDuration            time.Duration `json:"rate_duration,omitempty"`
	Profile                 string        `json:"profile"`
	Protocol                string        `json:"protocol"`
	Endpoint                string        `json:"endpoint"`
	HealthURL               string        `json:"health_url"`
	Plaintext               bool          `json:"plaintext"`
	CAFile                  string        `json:"ca_file,omitempty"`
	HeadersFile             string        `json:"headers_file,omitempty"`
	Rounds                  int           `json:"rounds"`
	PayloadBytes            int           `json:"payload_bytes"`
	RequestTimeout          time.Duration `json:"request_timeout"`
	PropagationTimeout      time.Duration `json:"propagation_timeout"`
	PollInterval            time.Duration `json:"poll_interval"`
	RunTimeout              time.Duration `json:"run_timeout"`
	ResultsDir              string        `json:"results_dir"`
	Instance                string        `json:"instance"`
	Background              int           `json:"background"`
	History                 int           `json:"history"`
	Updates                 int           `json:"updates"`
	DatabaseURLFile         string        `json:"database_url_file,omitempty"`
	RunnerRevision          string        `json:"runner_revision,omitempty"`
	BaselineFile            string        `json:"baseline_file,omitempty"`
	ConnectorPolicyFile     string        `json:"connector_policy_file,omitempty"`
}

// DefaultConfig returns an informational, small-profile workload.
func DefaultConfig() Config {
	return Config{HistoryBatchRounds: 1, Background: 100, History: 40, Updates: 3, Profile: "small", Protocol: "grpc", Endpoint: "kessel-inventory-api:9000", HealthURL: "http://kessel-inventory-api:8000/api/kessel/v1/livez", Rounds: 3, PayloadBytes: 128, RequestTimeout: 10 * time.Second, PropagationTimeout: 60 * time.Second, PollInterval: 100 * time.Millisecond, RunTimeout: 15 * time.Minute, ResultsDir: "/results"}
}

// Validate rejects invalid settings before any remote calls.
func (c Config) Validate() error {
	if len(c.selectedScenarios()) == 0 {
		return fmt.Errorf("at least one scenario is required")
	}
	seen := map[string]bool{}
	for _, scenario := range c.selectedScenarios() {
		if seen[scenario] || (scenario != "fresh" && scenario != "history" && scenario != "history_live") {
			return fmt.Errorf("invalid or duplicate scenario: %s", scenario)
		}
		seen[scenario] = true
	}
	if c.Scenarios != nil && c.DatabaseURLFile == "" {
		return fmt.Errorf("explicit scenario selection requires database fixtures")
	}
	if seen["history_live"] && (c.DatabaseURLFile == "" || c.HistoryDepth < 2 || (c.HistoryDepthFourWorkers != 0 && c.HistoryDepthFourWorkers < 2) || c.HistoryBatchRounds < 1 || c.HistoryBatchRounds > 100) {
		return fmt.Errorf("history_live requires database fixtures, depth >= 2, batch rounds 1..100")
	}

	if seen["history_live"] && (c.HistoryDeleteRounds < 0 || c.historyDeleteRounds() > c.Rounds) {
		return fmt.Errorf("history delete rounds must be between 1 and update rounds")
	}
	if seen["history_live"] && c.Profile == "full" && c.BaselineFile != "" && c.historyDeleteRounds() < minimumDeleteSamples {
		return fmt.Errorf("full history latency gates require at least %d delete rounds", minimumDeleteSamples)
	}

	if len(c.MessageRates) > 0 {
		if c.DatabaseURLFile == "" || !(len(seen) == 1 && seen["fresh"] && c.Scenarios != nil) || c.RateDuration < time.Second || c.RateDuration > 10*time.Minute {
			return fmt.Errorf("rate rounds require fresh database fixtures and duration between 1s and 10m")
		}
		for _, rate := range c.MessageRates {
			if rate < 1 || rate > 60000 || float64(rate)*c.RateDuration.Minutes() > 100000 {
				return fmt.Errorf("rate rounds require 1..60000 writes/minute and at most 100000 scheduled writes per round")
			}
		}
	}
	if c.Profile != "small" && c.Profile != "full" {
		return fmt.Errorf("profile must be small or full")
	}
	if c.Profile == "full" && c.DatabaseURLFile == "" {
		return fmt.Errorf("full profile requires database credentials")
	}
	if c.DatabaseURLFile != "" && (c.ConnectorPolicyFile == "" || c.Background < 0 || (seen["history"] && c.History < 8) || c.Updates < 1) {
		return fmt.Errorf("database fixtures require connector policy, nonnegative background, history >= 8 and positive updates")
	}
	if c.Profile == "full" && c.BaselineFile != "" && c.Rounds < minimumBaselineSamples {
		return fmt.Errorf("full latency gates require at least %d rounds", minimumBaselineSamples)
	}
	if c.BaselineFile != "" && c.DatabaseURLFile == "" {
		return fmt.Errorf("baseline enforcement requires database fixtures")
	}
	if c.Protocol != "grpc" && c.Protocol != "http" {
		return fmt.Errorf("protocol must be grpc or http")
	}
	if c.Endpoint == "" || c.HealthURL == "" || c.Rounds < 1 || c.PayloadBytes < 1 || c.RequestTimeout <= 0 || c.PropagationTimeout <= 0 || c.PollInterval <= 0 || c.RunTimeout <= 0 || c.ResultsDir == "" {
		return fmt.Errorf("endpoints, sizes, deadlines and results directory must be set and positive")
	}
	if c.PollInterval >= c.PropagationTimeout {
		return fmt.Errorf("poll interval must be below propagation timeout")
	}
	return nil
}

func (c Config) selectedScenarios() []string {
	if c.Scenarios != nil {
		return c.Scenarios
	}
	return []string{"fresh", "history"}
}

func scenarioOperations(scenario string, freshLifecycle bool) []string {
	if scenario == "history_live" {
		return []string{"workspace_update", "reporter_update", "delete"}
	}
	ops := []string{"create", "workspace_update", "unchanged_update", "delete"}
	if !freshLifecycle {
		ops = append(ops, "recreate")
	}
	return ops
}

// Explicit fresh selection uses new identities and excludes restoration. An
// omitted selection preserves the original runner's legacy lifecycle.
func (c Config) freshLifecycle(scenario string) bool {
	return scenario == "fresh" && c.Scenarios != nil
}

// The default is 30 deletes per worker, bounded by the number of update rounds
// so small profiles retain their existing short lifecycle.
func (c Config) historyDeleteRounds() int {
	if c.HistoryDeleteRounds != 0 {
		return c.HistoryDeleteRounds
	}
	return min(30, c.Rounds)
}
