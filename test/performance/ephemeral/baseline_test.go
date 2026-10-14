package ephemeral

import (
	"fmt"
	"testing"
)

func TestBaselineGates(t *testing.T) {
	r := Result{Profile: "full", Protocol: "grpc", Scenario: "fresh", Workers: 1, Rounds: 100, PayloadBytes: 1024, Operations: map[string]Operation{"create": {API: Stats{Count: 100, P95MS: 5}, Propagation: Stats{Count: 100, P95MS: 10}}}}
	b := Baseline{Limits: map[string]float64{"grpc/fresh/1/create/api": 6, "grpc/fresh/1/create/propagation": 11}}
	if err := checkBaseline(r, b); err != nil {
		t.Fatal(err)
	}
	b.Limits["grpc/fresh/1/create/api"] = 1
	if err := checkBaseline(r, b); err == nil {
		t.Fatal("accepted deliberately tight threshold")
	}
	b.Limits["grpc/fresh/1/create/api"] = 6
	delete(b.Limits, "grpc/fresh/1/create/propagation")
	if err := checkBaseline(r, b); err == nil {
		t.Fatal("accepted missing entry")
	}
	b.Limits["grpc/fresh/1/create/propagation"] = 11
	b.Limits["grpc/fresh/1/obsolete/api"] = 1
	if err := checkBaseline(r, b); err == nil {
		t.Fatal("accepted stale entry")
	}
	delete(b.Limits, "grpc/fresh/1/obsolete/api")
	s := r.Operations["create"]
	s.API.Count = 20
	r.Operations["create"] = s
	if err := checkBaseline(r, b); err == nil {
		t.Fatal("accepted insufficient samples")
	}
	r.Profile = "small"
	s.API.P95MS = 100
	r.Operations["create"] = s
	if err := checkBaseline(r, b); err != nil {
		t.Fatal("small must skip latency gates", err)
	}
}

func TestResultValidation(t *testing.T) {
	c := DefaultConfig()
	r := Result{Workers: 4, Operations: map[string]Operation{}}
	for _, op := range []string{"create", "workspace_update", "unchanged_update", "delete", "recreate"} {
		count := c.Rounds * r.Workers
		if op == "workspace_update" {
			count *= c.Updates
		}
		r.Operations[op] = Operation{API: Stats{Count: count}, Propagation: Stats{Count: count}}
	}
	if err := validateFixtureResult(r, c); err != nil {
		t.Fatal(err)
	}
	s := r.Operations["unchanged_update"]
	s.Propagation.Count = 0
	r.Operations["unchanged_update"] = s
	if err := validateFixtureResult(r, c); err == nil {
		t.Fatal("accepted missing unchanged completion evidence")
	}
}

func TestBaselineEntrySet(t *testing.T) {
	c := DefaultConfig()
	b := Baseline{Limits: map[string]float64{}}
	for _, scenario := range []string{"fresh", "history"} {
		for _, workers := range []int{1, 4} {
			for _, op := range []string{"create", "workspace_update", "unchanged_update", "delete", "recreate"} {
				for _, kind := range []string{"api", "propagation"} {
					b.Limits[fmt.Sprintf("grpc/%s/%d/%s/%s", scenario, workers, op, kind)] = 1
				}
			}
		}
	}
	if err := validateBaselineEntries(b, c); err != nil {
		t.Fatal(err)
	}
	b.Limits["grpc/old/1/create/api"] = 1
	if err := validateBaselineEntries(b, c); err == nil {
		t.Fatal("accepted unknown scenario")
	}
	delete(b.Limits, "grpc/old/1/create/api")
	delete(b.Limits, "grpc/history/4/delete/api")
	if err := validateBaselineEntries(b, c); err == nil {
		t.Fatal("accepted missing scenario entry")
	}
}

func TestPartialResultValidation(t *testing.T) {
	c := DefaultConfig()
	c.PartialCalibration = true
	r := Result{Workers: 1, Operations: map[string]Operation{}}
	for _, op := range []string{"create", "workspace_update", "unchanged_update", "delete"} {
		count := c.Rounds
		if op == "workspace_update" {
			count *= c.Updates
		}
		r.Operations[op] = Operation{API: Stats{Count: count}, Propagation: Stats{Count: count}}
	}
	if err := validateFixtureResult(r, c); err != nil {
		t.Fatal(err)
	}
	deletion := r.Operations["delete"]
	delete(r.Operations, "delete")
	if validateFixtureResult(r, c) == nil {
		t.Fatal("partial calibration omitted deletion samples")
	}
	r.Operations["delete"] = deletion
	c.PartialCalibration = false
	if validateFixtureResult(r, c) == nil {
		t.Fatal("partial results accepted as full lifecycle coverage")
	}
	c.PartialCalibration = true
	delete(r.Operations, "unchanged_update")
	if validateFixtureResult(r, c) == nil {
		t.Fatal("partial calibration omitted unchanged completion")
	}
}

func TestFreshDeleteBaselineScope(t *testing.T) {
	c := DefaultConfig()
	c.PartialCalibration = true
	c.Profile = "full"
	c.History = 0
	c.Rounds = 100
	b := Baseline{Limits: map[string]float64{}}
	r := Result{Profile: "full", Protocol: "grpc", Scenario: "fresh", Workers: 1, Operations: map[string]Operation{}}
	for _, workers := range []int{1, 4} {
		for _, op := range []string{"create", "workspace_update", "unchanged_update", "delete"} {
			for _, kind := range []string{"api", "propagation"} {
				b.Limits[fmt.Sprintf("grpc/fresh/%d/%s/%s", workers, op, kind)] = 2
			}
			r.Operations[op] = Operation{API: Stats{Count: 100, P95MS: 1}, Propagation: Stats{Count: 100, P95MS: 1}}
		}
	}
	if err := validateBaselineEntries(b, c); err != nil {
		t.Fatal(err)
	}
	if err := checkBaseline(r, b); err != nil {
		t.Fatal(err)
	}
	b.Limits["grpc/fresh/1/delete/propagation"] = 0.5
	if checkBaseline(r, b) == nil {
		t.Fatal("accepted tight delete threshold")
	}
	delete(b.Limits, "grpc/fresh/1/delete/api")
	if validateBaselineEntries(b, c) == nil {
		t.Fatal("accepted missing delete entry")
	}
	b.Limits["grpc/fresh/1/delete/api"] = 2
	b.Limits["grpc/fresh/1/recreate/api"] = 2
	if validateBaselineEntries(b, c) == nil {
		t.Fatal("accepted recreate entry")
	}
}
