//go:build integration

package ephemeral

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/project-kessel/inventory-api/internal/data"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// TestFixturePostgres uses an isolated database with the real migrations. The
// explicit DSN is for correctness only; no latency workload executes here.
func TestFixturePostgres(t *testing.T) {
	dsn := os.Getenv("PERF_FIXTURE_TEST_DSN")
	if dsn == "" {
		t.Skip("set PERF_FIXTURE_TEST_DSN to an isolated disposable PostgreSQL database")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err = data.Migrate(db, nil); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "dsn")
	if err = os.WriteFile(file, []byte(dsn), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f, err := openFixtures(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.pool.Close()
	scope := Fixture{Instance: "perf-" + uuid.NewString(), Background: 3, History: 11, Targets: []string{"one", "two", "three", "four"}}
	// Create/delete rows match the API's generation-zero transitions. Live API
	// wiring is covered separately; these establish the trusted storage precondition.
	for _, id := range scope.Targets {
		rid, rrid := uuid.NewString(), uuid.NewString()
		for _, stmt := range []struct {
			q    string
			args []any
		}{
			{`INSERT INTO resource (id,type,common_version,ktn,created_at,updated_at) VALUES ($1,'host',1,'',now(),now())`, []any{rid}},
			{`INSERT INTO common_representations (resource_id,version,data,reported_by_reporter_type,reported_by_reporter_instance,transaction_id,created_at) VALUES ($1,1,'{"workspace_id":"fixture"}','hbi',$2,$3,now())`, []any{rid, scope.Instance, uuid.NewString()}},
			{`INSERT INTO reporter_resources (id,local_resource_id,reporter_type,resource_type,reporter_instance_id,resource_id,api_href,representation_version,generation,tombstone,created_at,updated_at) VALUES ($1,$2,'hbi','host',$3,$4,'https://example.com',1,0,true,now(),now())`, []any{rrid, id, scope.Instance, rid}},
			{`INSERT INTO reporter_representations (reporter_resource_id,version,generation,data,common_version,transaction_id,tombstone,created_at) VALUES ($1,0,0,'{}',1,$2,false,now()),($1,1,0,NULL,NULL,$3,true,now())`, []any{rrid, uuid.NewString(), uuid.NewString()}},
		} {
			if _, err = f.pool.Exec(ctx, stmt.q, stmt.args...); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = f.seed(ctx, scope, 128); err != nil {
		t.Fatal(err)
	}
	if err = f.verifySeed(ctx, scope); err != nil {
		t.Fatal(err)
	}
	unrelated := Fixture{Instance: "perf-" + uuid.NewString(), Background: 2, Targets: []string{}}
	if err = f.seed(ctx, unrelated, 128); err != nil {
		t.Fatal(err)
	}
	defer f.cleanup(ctx, unrelated.Instance)
	// Reject a malformed API precondition and roll back background writes too.
	invalid := Fixture{Instance: "perf-" + uuid.NewString(), Background: 1, History: 8, Targets: []string{"missing"}}
	if err = f.seed(ctx, invalid, 128); err == nil {
		t.Fatal("accepted missing API target")
	}
	var rolledBack int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM reporter_resources WHERE reporter_instance_id=$1`, invalid.Instance).Scan(&rolledBack); err != nil {
		t.Fatal(err)
	}
	if rolledBack != 0 {
		t.Fatal("failed fixture transaction left background rows")
	}
	for i, id := range scope.Targets {
		version := 2
		if i == 3 {
			version = 1
		}
		if err = f.verifyState(ctx, scope.Instance, id, version, 0, true); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.seed(ctx, scope, 128); err == nil {
		t.Fatal("accepted existing scope")
	}
	if err = f.analyze(ctx); err != nil {
		t.Fatal(err)
	}
	m, err := f.metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m.TableRows["reporter_representations"] < 14 {
		t.Fatalf("missing fixture rows: %+v", m)
	}
	if err = f.cleanup(ctx, scope.Instance); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM reporter_resources WHERE reporter_instance_id=$1`, scope.Instance).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("fixture scope was not cleaned")
	}
	if err = f.verifySeed(ctx, unrelated); err != nil {
		t.Fatalf("cleanup affected another scope: %v", err)
	}
}

func TestCompletionNotifications(t *testing.T) {
	dsn := os.Getenv("PERF_FIXTURE_TEST_DSN")
	if dsn == "" {
		t.Skip("set PERF_FIXTURE_TEST_DSN to disposable PostgreSQL")
	}
	file := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(file, []byte(dsn), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f, err := openFixtures(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.pool.Close()
	l, err := f.listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	txid := uuid.NewString()
	// Notify before waiting: correlation must survive the database lookup window.
	for _, payload := range []string{"unrelated", txid} {
		if _, err = f.pool.Exec(ctx, `SELECT pg_notify('consumer_notifications',$1)`, payload); err != nil {
			t.Fatal(err)
		}
	}
	if err = l.wait(ctx, txid); err != nil {
		t.Fatal(err)
	}
	timeout, done := context.WithTimeout(ctx, 20*time.Millisecond)
	defer done()
	if err = l.wait(timeout, uuid.NewString()); !isTimeout(err) {
		t.Fatalf("missing event did not time out: %v", err)
	}
}
