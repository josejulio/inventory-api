// Package ephemeral provides the in-cluster performance harness. It does not
// start an API, replace authorization, or execute the local performance fixture.
package ephemeral

import (
	"fmt"
	"time"
)

// Config describes lifecycle workloads and optional scoped database fixtures.
type Config struct {
	MessageRates        []int         `json:"message_rates,omitempty"`
	RateDuration        time.Duration `json:"rate_duration,omitempty"`
	PartialCalibration  bool          `json:"partial_calibration,omitempty"`
	Profile             string        `json:"profile"`
	Protocol            string        `json:"protocol"`
	Endpoint            string        `json:"endpoint"`
	HealthURL           string        `json:"health_url"`
	Plaintext           bool          `json:"plaintext"`
	CAFile              string        `json:"ca_file,omitempty"`
	HeadersFile         string        `json:"headers_file,omitempty"`
	Rounds              int           `json:"rounds"`
	PayloadBytes        int           `json:"payload_bytes"`
	RequestTimeout      time.Duration `json:"request_timeout"`
	PropagationTimeout  time.Duration `json:"propagation_timeout"`
	PollInterval        time.Duration `json:"poll_interval"`
	RunTimeout          time.Duration `json:"run_timeout"`
	ResultsDir          string        `json:"results_dir"`
	Instance            string        `json:"instance"`
	Background          int           `json:"background"`
	History             int           `json:"history"`
	Updates             int           `json:"updates"`
	DatabaseURLFile     string        `json:"database_url_file,omitempty"`
	RunnerRevision      string        `json:"runner_revision,omitempty"`
	BaselineFile        string        `json:"baseline_file,omitempty"`
	ConnectorPolicyFile string        `json:"connector_policy_file,omitempty"`
}

// DefaultConfig returns an informational, small-profile workload.
func DefaultConfig() Config {
	return Config{Background: 100, History: 40, Updates: 3, Profile: "small", Protocol: "grpc", Endpoint: "kessel-inventory-api:9000", HealthURL: "http://kessel-inventory-api:8000/api/kessel/v1/livez", Rounds: 3, PayloadBytes: 128, RequestTimeout: 10 * time.Second, PropagationTimeout: 60 * time.Second, PollInterval: 100 * time.Millisecond, RunTimeout: 15 * time.Minute, ResultsDir: "/results"}
}

// Validate rejects invalid settings before any remote calls.
func (c Config) Validate() error {
	if len(c.MessageRates) > 0 {
		if c.DatabaseURLFile == "" || !c.PartialCalibration || c.RateDuration < time.Second || c.RateDuration > 10*time.Minute {
			return fmt.Errorf("rate rounds require fresh database fixtures and duration between 1s and 10m")
		}
		for _, rate := range c.MessageRates {
			if rate < 1 || rate > 60000 || float64(rate)*c.RateDuration.Minutes() > 100000 {
				return fmt.Errorf("rate rounds require 1..60000 writes/minute and at most 100000 scheduled writes per round")
			}
		}
	}
	if c.PartialCalibration && (c.DatabaseURLFile == "" || c.History != 0) {
		return fmt.Errorf("fresh calibration requires database fixtures and zero history")
	}
	if c.Profile != "small" && c.Profile != "full" {
		return fmt.Errorf("profile must be small or full")
	}
	if c.Profile == "full" && c.DatabaseURLFile == "" {
		return fmt.Errorf("full profile requires database credentials")
	}
	if c.DatabaseURLFile != "" && (c.ConnectorPolicyFile == "" || c.Background < 0 || (!c.PartialCalibration && c.History < 8) || c.Updates < 1) {
		return fmt.Errorf("database fixtures require connector policy, nonnegative background, history >= 8 and positive updates")
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
