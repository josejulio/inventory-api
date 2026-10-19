package ephemeral

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/xeipuuv/gojsonschema"
)

func TestHBIHostRequest(t *testing.T) {
	c := DefaultConfig()
	c.Instance = "performance-instance"
	first, err := hostReportRequest(c, "host-one", "workspace", 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := hostReportRequest(c, "host-one", "workspace", 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Type != "host" || first.ReporterType != "hbi" || first.ReporterInstanceId != c.Instance {
		t.Fatal("incorrect reporter identity")
	}
	if first.Representations.Metadata.GetConsoleHref() == "" || first.Representations.Metadata.GetReporterVersion() == "" {
		t.Fatal("missing HBI metadata")
	}
	for _, field := range []string{"insights_id", "subscription_manager_id"} {
		value := first.Representations.Reporter.Fields[field].GetStringValue()
		if _, err := uuid.Parse(value); err != nil {
			t.Fatal(err)
		}
		if value != second.Representations.Reporter.Fields[field].GetStringValue() {
			t.Fatal("host identity changed on update")
		}
	}
	for _, test := range []struct {
		path  string
		value map[string]any
	}{
		{"../../../data/schema/resources/host/common_representation.json", first.Representations.Common.AsMap()},
		{"../../../data/schema/resources/host/reporters/hbi/host.json", first.Representations.Reporter.AsMap()},
	} {
		schema, err := os.ReadFile(test.path)
		if err != nil {
			t.Fatal(err)
		}
		result, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schema), gojsonschema.NewGoLoader(test.value))
		if err != nil {
			t.Fatal(err)
		}
		if !result.Valid() {
			t.Fatalf("request violates HBI schema: %v", result.Errors())
		}
	}
}
