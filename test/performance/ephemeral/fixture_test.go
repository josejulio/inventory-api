package ephemeral

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestHistoryDistribution(t *testing.T) {
	counts, err := historyDistribution(11, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := []int{3, 3, 3, 2}
	for i, n := range counts {
		if n != want[i] {
			t.Fatalf("distribution %v", counts)
		}
	}
	if _, err = historyDistribution(7, 4); err == nil {
		t.Fatal("accepted history too small for API-established targets")
	}
}

func TestConnectorPolicy(t *testing.T) {
	valid := map[string]string{"slot.name": "inventory", "table.exclude.list": ".*", "snapshot.mode": "no_data", "plugin.name": "pgoutput"}
	file := filepath.Join(t.TempDir(), "connector.json")
	write := func(c map[string]string) {
		b, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(file, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(valid)
	if slot, err := connectorSlot(file); err != nil || slot != "inventory" {
		t.Fatalf("%s %v", slot, err)
	}
	for _, key := range []string{"slot.name", "table.exclude.list", "snapshot.mode", "plugin.name"} {
		t.Run(key, func(t *testing.T) {
			c := map[string]string{}
			for k, v := range valid {
				c[k] = v
			}
			delete(c, key)
			write(c)
			if _, err := connectorSlot(file); err == nil {
				t.Fatal("accepted incomplete connector policy")
			}
		})
	}
	valid["table.include.list"] = "public.resource"
	write(valid)
	if _, err := connectorSlot(file); err == nil {
		t.Fatal("accepted table CDC")
	}
}

func TestDatabaseEnvironmentRequiresCredentials(t *testing.T) {
	for _, name := range []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"} {
		t.Setenv(name, "")
	}
	if _, err := openFixtures(context.Background(), "env"); err == nil {
		t.Fatal("accepted incomplete database environment")
	}
}
