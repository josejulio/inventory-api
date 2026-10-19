//go:build integration

package ephemeral

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgconn"
	"os"
	"path/filepath"
	"strconv"
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

func TestLiveHistoryPostgres(t *testing.T) {
	dsn := os.Getenv("PERF_FIXTURE_TEST_DSN")
	if dsn == "" {
		t.Skip("set PERF_FIXTURE_TEST_DSN to disposable PostgreSQL")
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	f, err := openFixtures(ctx, file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.pool.Close()
	depth := 12
	if raw := os.Getenv("PERF_FIXTURE_DEPTH"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 12 {
			t.Fatal("PERF_FIXTURE_DEPTH must be >= 12")
		}
		depth = parsed
	}
	scope := Fixture{Instance: "perf-" + uuid.NewString() + "-history_live-1", Targets: []string{"live"}}
	defer f.cleanup(ctx, scope.Instance)
	targetCount := 1
	if raw := os.Getenv("PERF_FIXTURE_TARGETS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 40 {
			t.Fatal("PERF_FIXTURE_TARGETS must be 1..40")
		}
		targetCount = parsed
	}
	for i := 1; i < targetCount; i++ {
		scope.Targets = append(scope.Targets, fmt.Sprintf("live-%d", i))
	}
	backgroundCount := 0
	if raw := os.Getenv("PERF_FIXTURE_BACKGROUND"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			t.Fatal("invalid PERF_FIXTURE_BACKGROUND")
		}
		backgroundCount = parsed
	}
	if backgroundCount > 0 {
		background := Fixture{Instance: scope.Instance, Background: backgroundCount, Targets: []string{}}
		if err = f.seed(ctx, background, 1024); err != nil {
			t.Fatal(err)
		}
		if err = f.analyze(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var firstReporterID string
	for _, targetID := range scope.Targets {

		rid, rrid := uuid.NewString(), uuid.NewString()
		if firstReporterID == "" {
			firstReporterID = rrid
		}
		for _, stmt := range []struct {
			q    string
			args []any
		}{
			{`INSERT INTO resource (id,type,common_version,ktn,created_at,updated_at) VALUES ($1,'host',1,'',now(),now())`, []any{rid}},
			{`INSERT INTO common_representations (resource_id,version,data,reported_by_reporter_type,reported_by_reporter_instance,transaction_id,created_at) VALUES ($1,1,'{"workspace_id":"fixture"}','hbi',$2,$3,now())`, []any{rid, scope.Instance, uuid.NewString()}},
			{`INSERT INTO reporter_resources (id,local_resource_id,reporter_type,resource_type,reporter_instance_id,resource_id,api_href,representation_version,generation,tombstone,created_at,updated_at) VALUES ($1,$4,'hbi','host',$2,$3,'https://example.com',0,0,false,now(),now())`, []any{rrid, scope.Instance, rid, targetID}},
			{`INSERT INTO reporter_representations (reporter_resource_id,version,generation,data,common_version,transaction_id,tombstone,created_at) VALUES ($1::uuid,0,0,jsonb_build_object('sequence',0,'payload',repeat('x',1024),'satellite_id',NULL,'insights_id',$1::text,'subscription_manager_id',$1::text,'ansible_host','perf-host'),1,$2,false,now())`, []any{rrid, uuid.NewString()}},
		} {
			if _, err = f.pool.Exec(ctx, stmt.q, stmt.args...); err != nil {
				t.Fatal(err)
			}
		}
	}
	rrid := firstReporterID
	if targetCount == 1 && backgroundCount == 0 {
		// A missing second target must roll back writes to the valid first target.
		invalid := scope
		invalid.Targets = []string{"live", "missing"}
		if err = f.seedLive(ctx, invalid, depth); err == nil {
			t.Fatal("accepted missing target")
		}
		if err = f.verifyState(ctx, scope.Instance, "live", 0, 0, false); err != nil {
			t.Fatal(err)
		}
	}
	seedStart := time.Now()
	if err = f.seedLive(ctx, scope, depth); err != nil {
		var pgError *pgconn.PgError
		if errors.As(err, &pgError) {
			t.Fatalf("live seed: %v; hint: %s", err, pgError.Hint)
		}
		t.Fatal(err)
	}
	if err = f.verifyLive(ctx, scope, depth); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d live versions across %d targets (depth %d) in %s", depth*targetCount, targetCount, depth, time.Since(seedStart))
	if err = f.analyze(ctx); err != nil {
		t.Fatalf("post-seed analysis: %v", err)
	}
	metadata, err := f.metadata(ctx)
	if err != nil {
		t.Fatalf("post-seed metadata: %v", err)
	}
	if metadata.Bytes <= 0 || metadata.TableRows["reporter_representations"] < int64(depth*targetCount+backgroundCount) {
		t.Fatal("incomplete fixture footprint metadata")
	}
	var distinctPayloads int
	if err = f.pool.QueryRow(ctx, `SELECT count(DISTINCT data) FROM reporter_representations WHERE reporter_resource_id=$1`, rrid).Scan(&distinctPayloads); err != nil {
		t.Fatal(err)
	}
	if distinctPayloads != 1 {
		t.Fatal("fixture changed API-established payload")
	}
	if err = f.seedLive(ctx, scope, depth); err == nil {
		t.Fatal("accepted already seeded target")
	}
	// A historical payload must still match the API-established version zero.
	if _, err = f.pool.Exec(ctx, `UPDATE reporter_representations SET data='{"corrupt":true}' WHERE reporter_resource_id=$1 AND version=2`, rrid); err != nil {
		t.Fatal(err)
	}
	if err = f.verifyLive(ctx, scope, depth); err == nil {
		t.Fatal("accepted inconsistent historical payload")
	}
	if _, err = f.pool.Exec(ctx, `UPDATE reporter_representations history SET data=original.data FROM reporter_representations original WHERE history.reporter_resource_id=$1 AND history.version=2 AND original.reporter_resource_id=history.reporter_resource_id AND original.version=0 AND original.generation=0`, rrid); err != nil {
		t.Fatal(err)
	}
	// Parent and current representation tombstones must agree.
	if _, err = f.pool.Exec(ctx, `UPDATE reporter_representations SET tombstone=true WHERE reporter_resource_id=$1 AND version=$2`, rrid, depth-1); err != nil {
		t.Fatal(err)
	}
	if err = f.verifyState(ctx, scope.Instance, "live", depth-1, 0, false); err == nil {
		t.Fatal("accepted inconsistent current representation tombstone")
	}
	if _, err = f.pool.Exec(ctx, `UPDATE reporter_representations SET tombstone=false WHERE reporter_resource_id=$1 AND version=$2`, rrid, depth-1); err != nil {
		t.Fatal(err)
	}
	// Version gaps and broken current-version pointers must be rejected.
	if _, err = f.pool.Exec(ctx, `UPDATE reporter_representations SET version=$2 WHERE reporter_resource_id=$1 AND version=5`, rrid, depth+8); err != nil {
		t.Fatal(err)
	}
	if err = f.verifyLive(ctx, scope, depth); err == nil {
		t.Fatal("accepted version gap")
	}
	if _, err = f.pool.Exec(ctx, `UPDATE reporter_representations SET version=5 WHERE reporter_resource_id=$1 AND version=$2`, rrid, depth+8); err != nil {
		t.Fatal(err)
	}
	background := Fixture{Instance: scope.Instance, Background: backgroundCount, Targets: []string{}}
	if backgroundCount == 0 {
		background.Background = 2
		if err = f.seed(ctx, background, 128); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.cleanupTargets(ctx, scope.Instance, scope.Targets); err != nil {
		t.Fatal(err)
	}
	if err = f.verifySeed(ctx, background); err != nil {
		t.Fatalf("target cleanup removed background: %v", err)
	}
}
