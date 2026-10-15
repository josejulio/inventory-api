package ephemeral

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Fixture describes the isolated SQL scope and API-established history targets.
// Background rows deliberately have no Relations state and emit no WAL messages.
type Fixture struct {
	HistoryDepth int      `json:"history_depth,omitempty"`
	Instance     string   `json:"instance"`
	Background   int      `json:"background"`
	History      int      `json:"history"`
	Targets      []string `json:"targets"`
}

// DatabaseMetadata records the physical footprint, including reused pages and
// rows belonging to the deployment rather than this fixture.
type DatabaseMetadata struct {
	Version    string           `json:"postgres_version"`
	Bytes      int64            `json:"database_bytes"`
	TableBytes map[string]int64 `json:"table_bytes"`
	TableRows  map[string]int64 `json:"table_rows"`
}

type fixtures struct{ pool *pgxpool.Pool }

func openFixtures(ctx context.Context, path string) (*fixtures, error) {
	connectionString := ""
	if path == "env" {
		// Kubernetes injects these directly from the owned deployment's Secret.
		// Check variable names only; never include credentials in an error.
		for _, name := range []string{"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"} {
			if os.Getenv(name) == "" {
				return nil, fmt.Errorf("database environment variable %s is required", name)
			}
		}
	} else {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read database credentials: %w", err)
		}
		connectionString = strings.TrimSpace(string(b))
	}
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid database connection configuration")
	}
	config.MaxConns = 6
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("database pool initialization failed")
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database connection failed")
	}
	return &fixtures{pool: pool}, nil
}

func historyDistribution(total, workers int) ([]int, error) {
	if workers < 1 || total < 2*workers {
		return nil, fmt.Errorf("history must provide at least create and delete versions per worker")
	}
	counts := make([]int, workers)
	for i := range counts {
		counts[i] = total / workers
		if i < total%workers {
			counts[i]++
		}
	}
	return counts, nil
}

func (f *fixtures) seed(ctx context.Context, scope Fixture, payloadBytes int) error {
	if scope.Instance == "" || scope.Background < 0 || payloadBytes < 1 {
		return fmt.Errorf("invalid fixture scope")
	}
	var counts []int
	var err error
	if scope.History > 0 {
		counts, err = historyDistribution(scope.History, len(scope.Targets))
		if err != nil {
			return err
		}
	}
	tx, err := f.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var existing int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM reporter_resources WHERE reporter_instance_id=$1 AND local_resource_id LIKE 'background-%'`, scope.Instance).Scan(&existing); err != nil {
		return err
	}
	if existing != 0 {
		return fmt.Errorf("fixture scope is already populated")
	}
	payload := strings.Repeat("x", payloadBytes)
	// UUIDs are deterministically derived from the unique scenario scope, avoiding
	// extensions and avoiding collisions with other reservations or fixture runs.
	statements := []string{
		`INSERT INTO resource (id,type,common_version,ktn,created_at,updated_at) SELECT md5($1||':resource:'||n)::uuid,'host',1,'',now(),now() FROM generate_series(1,$2::int) n`,
		`INSERT INTO common_representations (resource_id,version,data,reported_by_reporter_type,reported_by_reporter_instance,transaction_id,created_at) SELECT md5($1||':resource:'||n)::uuid,1,jsonb_build_object('workspace_id',$1),'hbi',$1,$1||':common:'||n,now() FROM generate_series(1,$2::int) n`,
		`INSERT INTO reporter_resources (id,local_resource_id,reporter_type,resource_type,reporter_instance_id,resource_id,api_href,representation_version,generation,tombstone,created_at,updated_at) SELECT md5($1||':reporter:'||n)::uuid,'background-'||n,'hbi','host',$1,md5($1||':resource:'||n)::uuid,'https://example.com/perf',0,0,false,now(),now() FROM generate_series(1,$2::int) n`,
	}
	for _, sql := range statements {
		if _, err = tx.Exec(ctx, sql, scope.Instance, scope.Background); err != nil {
			return fmt.Errorf("background seed: %w", err)
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO reporter_representations (reporter_resource_id,version,generation,data,common_version,transaction_id,tombstone,created_at) SELECT md5($1||':reporter:'||n)::uuid,0,0,jsonb_build_object('payload',$3::text),1,$1||':reporter:'||n,false,now() FROM generate_series(1,$2::int) n`, scope.Instance, scope.Background, payload)
	if err != nil {
		return err
	}
	for i, id := range scope.Targets {
		if scope.History == 0 {
			break
		}
		var reporterID string
		var commonVersion int64
		var version, generation int
		var tombstone bool
		err = tx.QueryRow(ctx, `SELECT rr.id::text,r.common_version,rr.representation_version,rr.generation,rr.tombstone FROM reporter_resources rr JOIN resource r ON r.id=rr.resource_id WHERE rr.local_resource_id=$1 AND rr.reporter_instance_id=$2 AND rr.reporter_type='hbi' AND rr.resource_type='host'`, id, scope.Instance).Scan(&reporterID, &commonVersion, &version, &generation, &tombstone)
		if err != nil {
			return fmt.Errorf("history target missing: %w", err)
		}
		if version != 1 || generation != 0 || !tombstone {
			return fmt.Errorf("history target must be API-created then deleted in generation zero")
		}
		var rows int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM reporter_representations WHERE reporter_resource_id=$1::uuid`, reporterID).Scan(&rows); err != nil {
			return err
		}
		if rows != 2 {
			return fmt.Errorf("history target must have exactly two API versions")
		}
		// Move the existing API tombstone to the terminal version; retain its data,
		// transaction ID, generation and NULL common_version. Fill only live history.
		n := counts[i]
		tag, err := tx.Exec(ctx, `UPDATE reporter_representations SET version=$2 WHERE reporter_resource_id=$1::uuid AND generation=0 AND version=1 AND tombstone AND common_version IS NULL`, reporterID, n-1)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("invalid history terminal tombstone")
		}
		_, err = tx.Exec(ctx, `INSERT INTO reporter_representations (reporter_resource_id,version,generation,data,common_version,transaction_id,tombstone,created_at) SELECT $1::uuid,v,0,jsonb_build_object('payload',$4::text,'sequence',v),$3,$5||':history:'||$1||':'||v,false,now() FROM generate_series(1,$2::int) v`, reporterID, n-2, commonVersion, payload, scope.Instance)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `UPDATE reporter_resources SET representation_version=$2 WHERE id=$1::uuid`, reporterID, n-1); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (f *fixtures) verifySeed(ctx context.Context, scope Fixture) error {
	var background, history, bad int
	err := f.pool.QueryRow(ctx, `SELECT count(*) FROM reporter_resources WHERE reporter_instance_id=$1 AND local_resource_id LIKE 'background-%'`, scope.Instance).Scan(&background)
	if err != nil {
		return err
	}
	err = f.pool.QueryRow(ctx, `SELECT count(*) FROM reporter_representations rp JOIN reporter_resources rr ON rr.id=rp.reporter_resource_id WHERE rr.reporter_instance_id=$1 AND rr.local_resource_id=ANY($2::text[])`, scope.Instance, scope.Targets).Scan(&history)
	if err != nil {
		return err
	}
	if background != scope.Background || history != scope.History {
		return fmt.Errorf("fixture counts: background %d/%d history %d/%d", background, scope.Background, history, scope.History)
	}
	if scope.History > 0 {
		counts, err := historyDistribution(scope.History, len(scope.Targets))
		if err != nil {
			return err
		}
		for i, id := range scope.Targets {
			var count, minimum, maximum int
			err = f.pool.QueryRow(ctx, `SELECT count(*),min(rp.version),max(rp.version) FROM reporter_representations rp JOIN reporter_resources rr ON rr.id=rp.reporter_resource_id WHERE rr.reporter_instance_id=$1 AND rr.local_resource_id=$2 AND rp.generation=0`, scope.Instance, id).Scan(&count, &minimum, &maximum)
			if err != nil {
				return err
			}
			if count != counts[i] || minimum != 0 || maximum != counts[i]-1 {
				return fmt.Errorf("invalid history sequence for %s", id)
			}
		}
	}
	err = f.pool.QueryRow(ctx, `SELECT count(*) FROM reporter_representations rp JOIN reporter_resources rr ON rr.id=rp.reporter_resource_id WHERE rr.reporter_instance_id=$1 AND ((rp.tombstone AND rp.common_version IS NOT NULL) OR (NOT rp.tombstone AND NOT EXISTS (SELECT 1 FROM common_representations cr WHERE cr.resource_id=rr.resource_id AND cr.version=rp.common_version)))`, scope.Instance).Scan(&bad)
	if err != nil {
		return err
	}
	if bad != 0 {
		return fmt.Errorf("fixture has %d invalid historical common-version references", bad)
	}
	err = f.pool.QueryRow(ctx, `SELECT count(*) FROM reporter_resources rr LEFT JOIN reporter_representations rp ON rp.reporter_resource_id=rr.id AND rp.generation=rr.generation AND rp.version=rr.representation_version WHERE rr.reporter_instance_id=$1 AND (rp.reporter_resource_id IS NULL OR rp.tombstone<>rr.tombstone OR (rp.tombstone AND rp.common_version IS NOT NULL) OR (NOT rp.tombstone AND NOT EXISTS (SELECT 1 FROM common_representations cr WHERE cr.resource_id=rr.resource_id AND cr.version=rp.common_version)))`, scope.Instance).Scan(&bad)
	if err != nil {
		return err
	}
	if bad != 0 {
		return fmt.Errorf("fixture has %d invalid current representations", bad)
	}
	return nil
}
func (f *fixtures) verifyState(ctx context.Context, instance, id string, version, generation int, tombstone bool) error {
	var v, g int
	var deleted, currentDeleted bool
	err := f.pool.QueryRow(ctx, `SELECT rr.representation_version,rr.generation,rr.tombstone,rp.tombstone FROM reporter_resources rr JOIN reporter_representations rp ON rp.reporter_resource_id=rr.id AND rp.version=rr.representation_version AND rp.generation=rr.generation WHERE rr.local_resource_id=$1 AND rr.reporter_instance_id=$2 AND rr.reporter_type='hbi' AND rr.resource_type='host'`, id, instance).Scan(&v, &g, &deleted, &currentDeleted)
	if err != nil {
		return err
	}
	if v != version || g != generation || deleted != tombstone || currentDeleted != tombstone {
		return fmt.Errorf("persisted state for %s: version=%d generation=%d tombstone=%t current_tombstone=%t; expected %d/%d/%t", id, v, g, deleted, currentDeleted, version, generation, tombstone)
	}
	return nil
}
func (f *fixtures) cleanup(ctx context.Context, instance string) error {
	return f.cleanupTargets(ctx, instance, nil)
}

func (f *fixtures) cleanupTargets(ctx context.Context, instance string, targets []string) error {
	tx, err := f.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Remove only resources exclusive to this scenario. Reject correlation to a
	// different reporter rather than deleting a resource shared with that reporter.
	var shared int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM reporter_resources other WHERE other.reporter_instance_id<>$1 AND other.resource_id IN (SELECT resource_id FROM reporter_resources WHERE reporter_instance_id=$1 AND ($2::text[] IS NULL OR local_resource_id=ANY($2)))`, instance, targets).Scan(&shared)
	if err != nil {
		return err
	}
	if shared > 0 {
		return fmt.Errorf("fixture resources are shared; refusing cleanup")
	}
	var resourceIDs []string
	rows, err := tx.Query(ctx, `SELECT resource_id::text FROM reporter_resources WHERE reporter_instance_id=$1 AND ($2::text[] IS NULL OR local_resource_id=ANY($2))`, instance, targets)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		resourceIDs = append(resourceIDs, id)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	// Common representation FKs vary across schema revisions: delete children
	// explicitly, without TRUNCATE, CASCADE, or touching unrelated instances.
	for _, q := range []string{
		`DELETE FROM reporter_representations WHERE reporter_resource_id IN (SELECT id FROM reporter_resources WHERE reporter_instance_id=$1 AND ($2::text[] IS NULL OR local_resource_id=ANY($2)))`,
		`DELETE FROM common_representations WHERE resource_id IN (SELECT resource_id FROM reporter_resources WHERE reporter_instance_id=$1 AND ($2::text[] IS NULL OR local_resource_id=ANY($2)))`,
		`DELETE FROM reporter_resources WHERE reporter_instance_id=$1 AND ($2::text[] IS NULL OR local_resource_id=ANY($2))`,
	} {
		if _, err = tx.Exec(ctx, q, instance, targets); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM resource WHERE id=ANY($1::uuid[])`, resourceIDs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (f *fixtures) metadata(ctx context.Context) (DatabaseMetadata, error) {
	m := DatabaseMetadata{TableBytes: map[string]int64{}, TableRows: map[string]int64{}}
	if err := f.pool.QueryRow(ctx, `SELECT version(),pg_database_size(current_database())`).Scan(&m.Version, &m.Bytes); err != nil {
		return m, err
	}
	for _, table := range []string{"resource", "reporter_resources", "reporter_representations", "common_representations"} {
		var bytes, rows int64
		if err := f.pool.QueryRow(ctx, `SELECT pg_total_relation_size($1::regclass)`, table).Scan(&bytes); err != nil {
			return m, err
		}
		if err := f.pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&rows); err != nil {
			return m, err
		}
		m.TableBytes[table] = bytes
		m.TableRows[table] = rows
	}
	return m, nil
}
func (f *fixtures) analyze(ctx context.Context) error {
	// Fixture indexes can make parallel VACUUM request more dynamic shared
	// memory than the deployment container provides. Keep setup maintenance
	// serial without changing application query parallelism or DB settings.
	for _, table := range []string{"resource", "reporter_resources", "reporter_representations", "common_representations"} {
		if _, err := f.pool.Exec(ctx, "VACUUM (ANALYZE, PARALLEL 0) "+table); err != nil {
			return err
		}
	}
	return nil
}
