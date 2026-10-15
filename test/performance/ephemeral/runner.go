package ephemeral

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// Stats describes successful request samples in milliseconds.
type Stats struct {
	Count  int     `json:"count"`
	MinMS  float64 `json:"min_ms"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  float64 `json:"p50_ms"`
	P95MS  float64 `json:"p95_ms"`
	P99MS  float64 `json:"p99_ms"`
	MaxMS  float64 `json:"max_ms"`
}

// Operation separates API completion latency from relationship observation.
type Operation struct {
	Completion          Stats `json:"consumer_completion,omitempty"`
	API                 Stats `json:"api"`
	Propagation         Stats `json:"propagation"`
	Errors              int   `json:"errors"`
	Timeouts            int   `json:"timeouts"`
	PropagationErrors   int   `json:"propagation_errors"`
	PropagationTimeouts int   `json:"propagation_timeouts"`
}

// MeasurementWindow excludes setup and cleanup gaps between batches.
type MeasurementWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Result includes partial results even when correctness or propagation fails.
type Result struct {
	MeasurementWindows []MeasurementWindow `json:"measurement_windows,omitempty"`
	SetupSeconds       float64             `json:"setup_seconds,omitempty"`
	CleanupSeconds     float64             `json:"cleanup_seconds,omitempty"`
	SeededHistoryRows  int64               `json:"seeded_history_rows,omitempty"`
	FailureKind        string              `json:"failure_kind,omitempty"`

	ExcludedCoverage     []string             `json:"excluded_coverage,omitempty"`
	RunnerRevision       string               `json:"runner_revision,omitempty"`
	CompletionSignal     string               `json:"completion_signal,omitempty"`
	Scenario             string               `json:"scenario"`
	Fixture              *Fixture             `json:"fixture,omitempty"`
	Database             *DatabaseMetadata    `json:"database,omitempty"`
	RunID                string               `json:"run_id"`
	Instance             string               `json:"instance"`
	Protocol             string               `json:"protocol"`
	Workers              int                  `json:"workers"`
	Profile              string               `json:"profile"`
	Rounds               int                  `json:"rounds"`
	PayloadBytes         int                  `json:"payload_bytes"`
	PollMS               int64                `json:"poll_ms"`
	RequestTimeoutMS     int64                `json:"request_timeout_ms"`
	PropagationTimeoutMS int64                `json:"propagation_timeout_ms"`
	WriteVisibility      string               `json:"write_visibility"`
	CheckConsistency     string               `json:"check_consistency"`
	LatencyLimits        string               `json:"latency_limits"`
	Operations           map[string]Operation `json:"operations"`
	MeasurementStart     time.Time            `json:"measurement_start"`
	MeasurementEnd       time.Time            `json:"measurement_end"`
	MeasurementSeconds   float64              `json:"measurement_seconds"`
	Throughput           float64              `json:"throughput_per_second"`
	Failures             []string             `json:"failures,omitempty"`
}

type transport struct {
	c       Config
	headers map[string]string
	http    *http.Client
	grpc    pb.KesselInventoryServiceClient
	conn    *grpc.ClientConn
}

type httpStatusError struct{ code int }

func (e httpStatusError) Error() string { return fmt.Sprintf("HTTP status %d", e.code) }

func connect(c Config) (*transport, error) {
	t := &transport{c: c, headers: map[string]string{}}
	if c.HeadersFile != "" {
		b, e := os.ReadFile(c.HeadersFile)
		if e != nil {
			return nil, e
		}
		if e = json.Unmarshal(b, &t.headers); e != nil {
			return nil, fmt.Errorf("invalid credentials JSON")
		}
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if c.CAFile != "" {
		b, e := os.ReadFile(c.CAFile)
		if e != nil {
			return nil, e
		}
		pool, e := x509.SystemCertPool()
		if e != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("CA file contains no certificates")
		}
		tc.RootCAs = pool
	}
	t.http = &http.Client{Timeout: c.RequestTimeout, Transport: &http.Transport{TLSClientConfig: tc}}
	if c.Protocol == "grpc" {
		creds := credentials.NewTLS(tc)
		var opt grpc.DialOption = grpc.WithTransportCredentials(creds)
		if c.Plaintext {
			opt = grpc.WithTransportCredentials(insecure.NewCredentials())
		}
		conn, e := grpc.NewClient(c.Endpoint, opt)
		if e != nil {
			return nil, e
		}
		t.conn = conn
		t.grpc = pb.NewKesselInventoryServiceClient(conn)
	} else if !c.Plaintext && !strings.HasPrefix(c.Endpoint, "https://") {
		return nil, fmt.Errorf("HTTP requires https unless plaintext is explicitly enabled")
	}
	return t, nil
}

func (t *transport) call(ctx context.Context, method string, in, out proto.Message) error {
	ctx, cancel := context.WithTimeout(ctx, t.c.RequestTimeout)
	defer cancel()
	if t.grpc != nil {
		for k, v := range t.headers {
			ctx = metadata.AppendToOutgoingContext(ctx, strings.ToLower(k), v)
		}
		switch method {
		case "reportresource":
			_, e := t.grpc.ReportResource(ctx, in.(*pb.ReportResourceRequest))
			return e
		case "deleteresource":
			_, e := t.grpc.DeleteResource(ctx, in.(*pb.DeleteResourceRequest))
			return e
		case "checkforupdate":
			v, e := t.grpc.CheckForUpdate(ctx, in.(*pb.CheckForUpdateRequest))
			if e == nil {
				proto.Merge(out, v)
			}
			return e
		}
	}
	b, e := protojson.Marshal(in)
	if e != nil {
		return e
	}
	// Paths follow the protocol annotations, including lifecycle resource paths.
	path := map[string]string{"reportresource": "resources", "deleteresource": "resources", "checkforupdate": "checkforupdate"}[method]
	verb := "POST"
	if method == "deleteresource" {
		verb = "DELETE"
	}
	req, e := http.NewRequestWithContext(ctx, verb, strings.TrimRight(t.c.Endpoint, "/")+"/api/kessel/v1beta2/"+path, strings.NewReader(string(b)))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	resp, e := t.http.Do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpStatusError{code: resp.StatusCode}
	}
	b, e = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if e != nil {
		return e
	}
	return protojson.Unmarshal(b, out)
}

func (t *transport) report(ctx context.Context, id, workspace string, step int) error {
	request, err := hostReportRequest(t.c, id, workspace, step)
	if err != nil {
		return err
	}
	return t.call(ctx, "reportresource", request, &pb.ReportResourceResponse{})
}

// hostReportRequest follows the repository's HBI host fixture, with synthetic
// payload padding and sequence values for controlled benchmark updates.
func hostReportRequest(c Config, id, workspace string, step int) (*pb.ReportResourceRequest, error) {
	common, e := structpb.NewStruct(map[string]any{"workspace_id": workspace})
	if e != nil {
		return nil, e
	}
	reporter, e := structpb.NewStruct(map[string]any{"satellite_id": nil, "subscription_manager_id": uuid.NewSHA1(uuid.NameSpaceDNS, []byte(id+":subscription-manager")).String(), "insights_id": uuid.NewSHA1(uuid.NameSpaceDNS, []byte(id+":insights")).String(), "ansible_host": "perf-host", "payload": strings.Repeat("x", c.PayloadBytes), "sequence": step})
	if e != nil {
		return nil, e
	}
	return &pb.ReportResourceRequest{Type: "host", ReporterType: "hbi", ReporterInstanceId: c.Instance, WriteVisibility: pb.WriteVisibility_MINIMIZE_LATENCY, Representations: &pb.ResourceRepresentations{Metadata: &pb.RepresentationMetadata{LocalResourceId: id, ApiHref: "https://console.redhat.com/api/inventory/v1/hosts/" + id, ConsoleHref: proto.String("https://console.redhat.com/insights/inventory/" + id), ReporterVersion: proto.String("2.7.16")}, Common: common, Reporter: reporter}}, nil
}
func (t *transport) reference(id string) *pb.ResourceReference {
	return &pb.ResourceReference{ResourceType: "host", ResourceId: id, Reporter: &pb.ReporterReference{Type: "hbi", InstanceId: &t.c.Instance}}
}
func (t *transport) remove(ctx context.Context, id string) error {
	return t.call(ctx, "deleteresource", &pb.DeleteResourceRequest{Reference: t.reference(id)}, &pb.DeleteResourceResponse{})
}
func (t *transport) wait(ctx context.Context, id, workspace string, want bool) error {
	ctx, cancel := context.WithTimeout(ctx, t.c.PropagationTimeout)
	defer cancel()
	ticker := time.NewTicker(t.c.PollInterval)
	defer ticker.Stop()
	for {
		out := &pb.CheckForUpdateResponse{}
		err := t.call(ctx, "checkforupdate", &pb.CheckForUpdateRequest{Object: t.reference(id), Relation: "workspace", Subject: &pb.SubjectReference{Resource: &pb.ResourceReference{ResourceType: "workspace", ResourceId: workspace, Reporter: &pb.ReporterReference{Type: "rbac"}}}}, out)
		if err != nil {
			var httpError httpStatusError
			if !want && (status.Code(err) == codes.NotFound || (errors.As(err, &httpError) && httpError.code == http.StatusNotFound)) {
				return nil
			}
			return fmt.Errorf("relationship check: %w", err)
		}
		if (out.Allowed == pb.Allowed_ALLOWED_TRUE && want) || (out.Allowed == pb.Allowed_ALLOWED_FALSE && !want) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("propagation deadline: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// Run executes closed-loop lifecycles with one and four independent workers.
// Database credentials enable scoped fresh/history fixtures and state verification.
// Setup, warmup and correctness checks are outside API latency samples. Pipeline
// timings start before the write and end after a strongly consistent observation.
func Run(ctx context.Context, c Config) error {
	if e := c.Validate(); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, c.RunTimeout)
	defer cancel()
	runID := uuid.NewString()
	if c.Instance == "" {
		c.Instance = "perf-" + runID
	}
	if e := os.MkdirAll(c.ResultsDir, 0755); e != nil {
		return e
	}
	t, e := connect(c)
	if e != nil {
		return e
	}
	if t.conn != nil {
		defer t.conn.Close()
	}
	defer t.http.CloseIdleConnections()
	healthCtx, hcancel := context.WithTimeout(ctx, c.RequestTimeout)
	req, e := http.NewRequestWithContext(healthCtx, "GET", c.HealthURL, nil)
	if e != nil {
		hcancel()
		return e
	}
	for k, v := range t.headers {
		req.Header.Set(k, v)
	}
	resp, e := t.http.Do(req)
	hcancel()
	if e != nil {
		return fmt.Errorf("health check failed")
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("health status %d", resp.StatusCode)
	}
	// Warmup also proves create and delete pass through the real pipeline.
	warm := runID + "-warm"
	workspace := uuid.NewString()
	if e = t.report(ctx, warm, workspace, 0); e == nil {
		e = t.wait(ctx, warm, workspace, true)
	}
	if e == nil {
		e = t.remove(ctx, warm)
	}
	if e == nil {
		e = t.wait(ctx, warm, workspace, false)
	}
	if e != nil {
		return fmt.Errorf("pipeline preflight: %w", e)
	}
	if c.DatabaseURLFile != "" {
		return runFixtures(ctx, t, runID)
	}
	for _, workers := range []int{1, 4} {
		r := Result{RunID: runID, Instance: c.Instance, Protocol: c.Protocol, Workers: workers, Profile: c.Profile, Rounds: c.Rounds, PayloadBytes: c.PayloadBytes, PollMS: c.PollInterval.Milliseconds(), RequestTimeoutMS: c.RequestTimeout.Milliseconds(), PropagationTimeoutMS: c.PropagationTimeout.Milliseconds(), WriteVisibility: "MINIMIZE_LATENCY", CheckConsistency: "CheckForUpdate (strongly consistent)", LatencyLimits: "informational; no thresholds", Operations: map[string]Operation{}}
		api, pipe := map[string][]time.Duration{}, map[string][]time.Duration{}
		var mu sync.Mutex
		var wg sync.WaitGroup
		start := time.Now()
		r.MeasurementStart = start.UTC()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for round := 0; round < c.Rounds; round++ {
					id := fmt.Sprintf("%s-%d-%d-%d", runID, workers, w, round)
					ws := uuid.NewString()
					oldWS := ws
					for step, op := range []string{"create", "workspace_update", "delete", "recreate"} {
						if op == "workspace_update" {
							ws = uuid.NewString()
						}
						before := time.Now()
						var err error
						if op == "delete" {
							err = t.remove(ctx, id)
						} else {
							err = t.report(ctx, id, ws, step)
						}
						elapsed := time.Since(before)
						requestFailed := err != nil
						mu.Lock()
						s := r.Operations[op]
						if err == nil {
							api[op] = append(api[op], elapsed)
						} else {
							s.Errors++
							if isTimeout(err) {
								s.Timeouts++
							}
						}
						r.Operations[op] = s
						mu.Unlock()
						if err == nil {
							err = t.wait(ctx, id, ws, op != "delete")
						}
						if err == nil && op == "workspace_update" {
							err = t.wait(ctx, id, oldWS, false)
						}
						mu.Lock()
						if err == nil {
							pipe[op] = append(pipe[op], time.Since(before))
						} else {
							if !requestFailed {
								s := r.Operations[op]
								s.PropagationErrors++
								if isTimeout(err) {
									s.PropagationTimeouts++
								}
								r.Operations[op] = s
							}
							r.Failures = append(r.Failures, fmt.Sprintf("worker %d round %d %s failed (%s)", w, round, op, status.Code(err)))
						}
						mu.Unlock()
						if err != nil {
							return
						}
					}
					if err := t.remove(ctx, id); err != nil {
						mu.Lock()
						r.Failures = append(r.Failures, "scenario cleanup failed")
						mu.Unlock()
						return
					}
					if err := t.wait(ctx, id, ws, false); err != nil {
						mu.Lock()
						r.Failures = append(r.Failures, "scenario cleanup propagation failed")
						mu.Unlock()
						return
					}
				}
			}(w)
		}
		wg.Wait()
		end := time.Now()
		r.MeasurementEnd = end.UTC()
		r.MeasurementSeconds = end.Sub(start).Seconds()
		total := 0
		for op, s := range r.Operations {
			s.API = summarize(api[op])
			s.Propagation = summarize(pipe[op])
			r.Operations[op] = s
			total += s.API.Count
		}
		r.Throughput = float64(total) / r.MeasurementSeconds
		if e = writeResult(c.ResultsDir, r); e != nil {
			return e
		}
		if len(r.Failures) > 0 {
			return fmt.Errorf("lifecycle failed; see partial results")
		}
	}
	return nil
}
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}
func summarize(v []time.Duration) Stats {
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	if len(v) == 0 {
		return Stats{}
	}
	var sum float64
	for _, d := range v {
		sum += float64(d) / float64(time.Millisecond)
	}
	pct := func(p float64) float64 {
		i := int(float64(len(v))*p+.999999) - 1
		return float64(v[i]) / float64(time.Millisecond)
	}
	return Stats{Count: len(v), MinMS: float64(v[0]) / float64(time.Millisecond), MeanMS: sum / float64(len(v)), P50MS: pct(.5), P95MS: pct(.95), P99MS: pct(.99), MaxMS: float64(v[len(v)-1]) / float64(time.Millisecond)}
}
func writeResult(dir string, r Result) error {
	b, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	scenario := r.Scenario
	if scenario == "" {
		scenario = "fresh"
	}
	name := fmt.Sprintf("%s-%s-%d", r.Protocol, scenario, r.Workers)
	if e = os.WriteFile(filepath.Join(dir, name+".json"), b, 0644); e != nil {
		return e
	}
	var out strings.Builder
	fmt.Fprintf(&out, "%s %s / %d workers: %.2f successful calls/s (closed loop includes verification and cleanup)\nLatency limits: %s\n", r.Protocol, scenario, r.Workers, r.Throughput, r.LatencyLimits)
	for _, op := range []string{"create", "workspace_update", "unchanged_update", "delete", "recreate"} {
		s, ok := r.Operations[op]
		if !ok {
			continue
		}
		fmt.Fprintf(&out, "%s: n=%d min/mean/p50/p95/p99/max=%.2f/%.2f/%.2f/%.2f/%.2f/%.2f ms; propagation p95=%.2f ms; errors=%d timeouts=%d propagation_errors=%d propagation_timeouts=%d\n", op, s.API.Count, s.API.MinMS, s.API.MeanMS, s.API.P50MS, s.API.P95MS, s.API.P99MS, s.API.MaxMS, s.Propagation.P95MS, s.Errors, s.Timeouts, s.PropagationErrors, s.PropagationTimeouts)
	}
	return os.WriteFile(filepath.Join(dir, name+".txt"), []byte(out.String()), 0644)
}
