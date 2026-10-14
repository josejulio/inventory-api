package ephemeral

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/project-kessel/inventory-api/api/kessel/inventory/v1beta2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// inventory is an in-memory lifecycle implementation, not a method-verifying mock.
type inventory struct {
	pb.UnimplementedKesselInventoryServiceServer
	mu             sync.Mutex
	workspaces     map[string]string
	reject         bool
	stall          bool
	rejectMeasured bool
	checkError     codes.Code
}

func (i *inventory) ReportResource(ctx context.Context, r *pb.ReportResourceRequest) (*pb.ReportResourceResponse, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 || values[0] != "Bearer test" {
		return nil, status.Error(codes.Unauthenticated, "credentials missing")
	}
	if i.reject || (i.rejectMeasured && !strings.HasSuffix(r.Representations.Metadata.LocalResourceId, "-warm")) {
		return nil, status.Error(codes.PermissionDenied, "report denied")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.stall {
		i.workspaces[r.Representations.Metadata.LocalResourceId] = r.Representations.Common.Fields["workspace_id"].GetStringValue()
	}
	return &pb.ReportResourceResponse{}, nil
}
func (i *inventory) DeleteResource(_ context.Context, r *pb.DeleteResourceRequest) (*pb.DeleteResourceResponse, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.workspaces, r.Reference.ResourceId)
	return &pb.DeleteResourceResponse{}, nil
}
func (i *inventory) CheckForUpdate(_ context.Context, r *pb.CheckForUpdateRequest) (*pb.CheckForUpdateResponse, error) {
	if i.checkError != codes.OK {
		return nil, status.Error(i.checkError, "check failed")
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	allowed := pb.Allowed_ALLOWED_FALSE
	if i.workspaces[r.Object.ResourceId] == r.Subject.Resource.ResourceId {
		allowed = pb.Allowed_ALLOWED_TRUE
	}
	return &pb.CheckForUpdateResponse{Allowed: allowed}, nil
}
func setup(t *testing.T, i *inventory) Config {
	t.Helper()
	i.workspaces = map[string]string{}
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	pb.RegisterKesselInventoryServiceServer(server, i)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	t.Cleanup(health.Close)
	c := DefaultConfig()
	c.Plaintext = true
	c.Endpoint = listener.Addr().String()
	c.HealthURL = health.URL
	c.ResultsDir = t.TempDir()
	c.Rounds = 1
	c.PollInterval = time.Millisecond
	c.PropagationTimeout = 50 * time.Millisecond
	c.HeadersFile = filepath.Join(t.TempDir(), "headers.json")
	if e = os.WriteFile(c.HeadersFile, []byte(`{"authorization":"Bearer test"}`), 0600); e != nil {
		t.Fatal(e)
	}
	return c
}
func TestLifecycleArtifacts(t *testing.T) {
	i := &inventory{}
	c := setup(t, i)
	if e := Run(context.Background(), c); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"grpc-fresh-1.json", "grpc-fresh-4.json"} {
		b, e := os.ReadFile(filepath.Join(c.ResultsDir, name))
		if e != nil {
			t.Fatal(e)
		}
		var r Result
		if e = json.Unmarshal(b, &r); e != nil {
			t.Fatal(e)
		}
		if r.MeasurementStart.IsZero() || !r.MeasurementEnd.After(r.MeasurementStart) || r.MeasurementSeconds <= 0 {
			t.Fatalf("missing scenario measurement window: %+v", r)
		}
		for op, s := range r.Operations {
			if s.API.Count != r.Workers || s.Propagation.Count != r.Workers || s.Errors != 0 {
				t.Fatalf("%s: %+v", op, s)
			}
		}
	}
	if len(i.workspaces) != 0 {
		t.Fatal("resources left after cleanup")
	}
}
func TestFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		i    inventory
	}{{"request denial", inventory{reject: true}}, {"propagation timeout", inventory{stall: true}}} {
		t.Run(test.name, func(t *testing.T) {
			c := setup(t, &test.i)
			if e := Run(context.Background(), c); e == nil {
				t.Fatal("expected failure")
			}
		})
	}
}

func TestPartialResultsOnRequestFailure(t *testing.T) {
	c := setup(t, &inventory{rejectMeasured: true})
	if err := Run(context.Background(), c); err == nil {
		t.Fatal("expected measured request failure")
	}
	b, err := os.ReadFile(filepath.Join(c.ResultsDir, "grpc-fresh-1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var r Result
	if err = json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	if len(r.Failures) != 1 || r.Operations["create"].Errors != 1 || r.Operations["create"].API.Count != 0 {
		t.Fatalf("incorrect partial results: %+v", r)
	}
}

func TestHTTPLifecycle(t *testing.T) {
	i := &inventory{}
	c := setup(t, i)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := metadata.NewIncomingContext(r.Context(), metadata.Pairs("authorization", r.Header.Get("authorization")))
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body read failed", 400)
			return
		}
		switch {
		case r.URL.Path == "/api/kessel/v1beta2/resources" && r.Method == "POST":
			in := &pb.ReportResourceRequest{}
			if err := protojson.Unmarshal(b, in); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if _, err := i.ReportResource(ctx, in); err != nil {
				http.Error(w, "denied", 403)
				return
			}
			w.Write([]byte(`{}`))
		case r.URL.Path == "/api/kessel/v1beta2/resources" && r.Method == "DELETE":
			in := &pb.DeleteResourceRequest{}
			if err := protojson.Unmarshal(b, in); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			i.DeleteResource(ctx, in)
			w.Write([]byte(`{}`))
		case r.URL.Path == "/api/kessel/v1beta2/checkforupdate" && r.Method == "POST":
			in := &pb.CheckForUpdateRequest{}
			if err := protojson.Unmarshal(b, in); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			out, _ := i.CheckForUpdate(ctx, in)
			b, _ := protojson.Marshal(out)
			w.Write(b)
		default:
			http.Error(w, "bad route", 404)
		}
	}))
	defer server.Close()
	c.Protocol = "http"
	c.Endpoint = server.URL
	if err := Run(context.Background(), c); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteObservationKeepsAuthorizationFailures(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.PermissionDenied, codes.Unauthenticated} {
		t.Run(code.String(), func(t *testing.T) {
			c := setup(t, &inventory{checkError: code})
			transport, err := connect(c)
			if err != nil {
				t.Fatal(err)
			}
			defer transport.conn.Close()
			err = transport.wait(context.Background(), "deleted", "workspace", false)
			if code == codes.NotFound && err != nil {
				t.Fatal(err)
			}
			if code != codes.NotFound && err == nil {
				t.Fatal("authorization failure was accepted as deletion")
			}
		})
	}
}
