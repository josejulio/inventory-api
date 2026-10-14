package ephemeral

import (
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	valid := DefaultConfig()
	for _, change := range []func(*Config){func(c *Config) { c.Profile = "full" }, func(c *Config) { c.Rounds = 0 }, func(c *Config) { c.RequestTimeout = 0 }, func(c *Config) { c.Protocol = "invalid" }, func(c *Config) { c.Endpoint = "" }} {
		c := valid
		change(&c)
		if c.Validate() == nil {
			t.Fatalf("accepted invalid configuration: %+v", c)
		}
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestStatistics(t *testing.T) {
	s := summarize([]time.Duration{time.Millisecond, 3 * time.Millisecond, 2 * time.Millisecond})
	if s.Count != 3 || s.MeanMS != 2 || s.P95MS != 3 || s.MinMS != 1 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestPartialCalibrationScope(t *testing.T) {
	c := DefaultConfig()
	c.PartialCalibration = true
	c.DatabaseURLFile = "env"
	c.ConnectorPolicyFile = "policy.json"
	c.History = 0
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.History = 40
	if c.Validate() == nil {
		t.Fatal("partial calibration must not claim history coverage")
	}
	c.History = 0
	c.BaselineFile = "baseline.json"
	if err := c.Validate(); err != nil {
		t.Fatal("scoped baseline configuration should be accepted", err)
	}
}

func TestRateConfiguration(t *testing.T) {
	c := DefaultConfig()
	c.PartialCalibration = true
	c.DatabaseURLFile = "env"
	c.ConnectorPolicyFile = "policy.json"
	c.History = 0
	c.MessageRates = []int{500, 1000, 2000}
	c.RateDuration = time.Minute
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.MessageRates = []int{0}
	if c.Validate() == nil {
		t.Fatal("accepted zero rate")
	}
}
