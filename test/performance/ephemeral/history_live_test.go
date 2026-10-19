package ephemeral

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestHistoryLiveSelection(t *testing.T) {
	c := DefaultConfig()
	c.DatabaseURLFile = "env"
	c.ConnectorPolicyFile = "policy"
	c.Scenarios = []string{"history_live"}
	c.HistoryDepth = 12
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, scenarios := range [][]string{{"unknown"}, {"fresh", "fresh"}, {}} {
		c.Scenarios = scenarios
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted %v", scenarios)
		}
	}
	c.Scenarios = []string{"history_live"}
	c.HistoryDepth = 1
	if err := c.Validate(); err == nil {
		t.Fatal("accepted shallow history")
	}
}

func TestHistoryLiveCoverage(t *testing.T) {
	c := DefaultConfig()
	r := Result{Scenario: "history_live", Workers: 1, Operations: map[string]Operation{}}
	for _, op := range []string{"workspace_update", "reporter_update", "delete"} {
		count := c.Rounds
		if op != "delete" {
			count *= c.Updates
		}
		r.Operations[op] = Operation{API: Stats{Count: count}, Propagation: Stats{Count: count}, Completion: Stats{Count: count}}
	}
	if err := validateFixtureResult(r, c); err != nil {
		t.Fatal(err)
	}
	update := r.Operations["workspace_update"]
	update.Completion.Count--
	r.Operations["workspace_update"] = update
	if err := validateFixtureResult(r, c); err == nil {
		t.Fatal("accepted missing correlated completion")
	}
	update.Completion.Count++
	r.Operations["workspace_update"] = update
	delete(r.Operations, "delete")
	if err := validateFixtureResult(r, c); err == nil {
		t.Fatal("accepted missing deletes")
	}
}

func TestHistoryLiveBaselineEntries(t *testing.T) {
	c := DefaultConfig()
	c.Scenarios = []string{"history_live"}
	b := Baseline{Limits: map[string]float64{}}
	for _, workers := range []int{1, 4} {
		for _, op := range scenarioOperations("history_live", false) {
			kinds := []string{"consumer_completion"}
			if op == "delete" {
				kinds = []string{"propagation"}
			}
			for _, kind := range kinds {
				b.Limits[fmt.Sprintf("grpc/history_live/%d/%s/%s", workers, op, kind)] = 1
			}
		}
	}
	if err := validateBaselineEntries(b, c); err != nil {
		t.Fatal(err)
	}
	b.Limits["grpc/history_live/1/create/api"] = 1
	if err := validateBaselineEntries(b, c); err == nil {
		t.Fatal("accepted create limit for history_live")
	}
}

func TestExplicitFreshExcludesRestoration(t *testing.T) {
	c := DefaultConfig()
	c.Scenarios = []string{"fresh", "history_live"}
	if !c.freshLifecycle("fresh") {
		t.Fatal("explicit fresh restored identities")
	}
	if c.freshLifecycle("history") {
		t.Fatal("legacy history silently changed")
	}
	c.Scenarios = nil
	if c.freshLifecycle("fresh") {
		t.Fatal("implicit legacy lifecycle changed")
	}
}

func TestHistoryLiveBatchBounds(t *testing.T) {
	c := DefaultConfig()
	c.DatabaseURLFile = "env"
	c.ConnectorPolicyFile = "policy"
	c.Scenarios = []string{"history_live"}
	c.HistoryDepth = 12
	for _, batch := range []int{0, 101} {
		c.HistoryBatchRounds = batch
		if err := c.Validate(); err == nil {
			t.Fatalf("accepted batch %d", batch)
		}
	}
	c.HistoryBatchRounds = 10
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryLiveCompletionGate(t *testing.T) {
	r := Result{Profile: "full", Protocol: "grpc", Scenario: "history_live", Workers: 1, Operations: map[string]Operation{"reporter_update": {API: Stats{Count: 100, P95MS: 5}, Propagation: Stats{Count: 100, P95MS: 15}, Completion: Stats{Count: 100, P95MS: 10}}}}
	b := Baseline{Limits: map[string]float64{"grpc/history_live/1/reporter_update/consumer_completion": 8}}
	if err := checkBaseline(r, b); err == nil {
		t.Fatal("ignored correlated completion regression")
	}
	b.Limits["grpc/history_live/1/reporter_update/consumer_completion"] = 12
	operation := r.Operations["reporter_update"]
	operation.API.P95MS = 1000
	operation.Propagation.P95MS = 2000
	r.Operations["reporter_update"] = operation
	if err := checkBaseline(r, b); err != nil {
		t.Fatal("consumer baseline must not gate API or graph observation latency", err)
	}
	r.Profile = "small"
	if err := checkBaseline(r, b); err != nil {
		t.Fatal("small profile applied completion latency gate", err)
	}
}

func TestExplicitScenariosRequireDatabaseFixtures(t *testing.T) {
	for _, scenario := range []string{"fresh", "history"} {
		c := DefaultConfig()
		c.Scenarios = []string{scenario}
		if err := c.Validate(); err == nil {
			t.Fatalf("%s silently fell back to the legacy non-database workload", scenario)
		}
	}
}

func TestConcurrencyCasesContinueAfterFailure(t *testing.T) {
	var visited []int
	err := runConcurrencyCases(context.Background(), "history_live", func(workers int) error {
		visited = append(visited, workers)
		return fmt.Errorf("failure for %d", workers)
	})
	if len(visited) != 2 || visited[0] != 1 || visited[1] != 4 {
		t.Fatalf("did not run both concurrency cases: %v", visited)
	}
	if err == nil || !strings.Contains(err.Error(), "failure for 1") || !strings.Contains(err.Error(), "failure for 4") {
		t.Fatalf("did not retain both failures: %v", err)
	}
}

func TestConcurrencyCasesRespectCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	visited := 0
	err := runConcurrencyCases(ctx, "history_live", func(workers int) error {
		visited++
		cancel()
		return ctx.Err()
	})
	if visited != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: visited=%d error=%v", visited, err)
	}
}

func TestBaselineReportsAllViolations(t *testing.T) {
	r := Result{Profile: "full", Protocol: "grpc", Scenario: "history_live", Workers: 1, Operations: map[string]Operation{
		"workspace_update": {Completion: Stats{Count: 100, P95MS: 30}},
		"reporter_update":  {Completion: Stats{Count: 100, P95MS: 40}},
	}}
	b := Baseline{Limits: map[string]float64{
		"grpc/history_live/1/workspace_update/consumer_completion": 10,
		"grpc/history_live/1/reporter_update/consumer_completion":  20,
	}}
	violations := baselineViolations(r, b)
	if len(violations) != 2 {
		t.Fatalf("wanted both violations, got %v", violations)
	}
}

func TestHistoryLiveReducedDeletesPreserveUpdates(t *testing.T) {
	c := DefaultConfig()
	c.Rounds = 100
	c.HistoryDeleteRounds = 30
	counts := map[string]int{}
	for target := 0; target < c.historyDeleteRounds(); target++ {
		operations := historyOperations(c, target)
		if operations[len(operations)-1] != "delete" {
			t.Fatal("target must be deleted only after its updates")
		}
		for _, operation := range operations {
			counts[operation]++
		}
	}
	if counts["delete"] != 30 || counts["workspace_update"] != 300 || counts["reporter_update"] != 300 {
		t.Fatalf("incorrect coverage after reducing deletes: %v", counts)
	}
	for _, workers := range []int{1, 4} {
		r := Result{Scenario: "history_live", Workers: workers, Operations: map[string]Operation{}}
		for operation, count := range counts {
			r.Operations[operation] = Operation{API: Stats{Count: count * workers}, Propagation: Stats{Count: count * workers}, Completion: Stats{Count: count * workers}}
		}
		if err := validateFixtureResult(r, c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHistoryLiveDeleteMinimumSamples(t *testing.T) {
	r := Result{Profile: "full", Protocol: "grpc", Scenario: "history_live", Workers: 1, Operations: map[string]Operation{"delete": {Propagation: Stats{Count: 30, P95MS: 10}}}}
	b := Baseline{Limits: map[string]float64{"grpc/history_live/1/delete/propagation": 20}}
	if err := checkBaseline(r, b); err != nil {
		t.Fatal("30 delete samples must be sufficient", err)
	}
	operation := r.Operations["delete"]
	operation.Propagation.Count = 29
	r.Operations["delete"] = operation
	if checkBaseline(r, b) == nil {
		t.Fatal("accepted fewer than 30 delete samples")
	}
}
