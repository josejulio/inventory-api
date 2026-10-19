// Command ephemeral runs the in-cluster lifecycle workload and exports artifacts.
package main

import (
	"archive/tar"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/project-kessel/inventory-api/test/performance/ephemeral"
)

// sourceRevision is supplied by the container build when known.
var sourceRevision = "unknown"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) > 1 && os.Args[1] == "hold" {
		<-ctx.Done()
		return nil
	}
	if len(os.Args) > 1 && os.Args[1] == "export" {
		return export("/results", os.Stdout)
	}
	config := flag.String("config", "/config/config.json", "runner configuration JSON")
	flag.Parse()
	b, e := os.ReadFile(*config)
	if e != nil {
		return e
	}
	var profile struct {
		Profile string `json:"profile"`
	}
	if e = json.Unmarshal(b, &profile); e != nil {
		return e
	}
	c := ephemeral.DefaultConfig()
	if profile.Profile == "full" {
		c.Background = 100000
		c.History = 227872
		c.PayloadBytes = 1024
		c.Rounds = 20
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return e
	}
	c.RunnerRevision = sourceRevision
	e = ephemeral.Run(ctx, c)
	// This status is also present when preflight fails before samples exist.
	result := map[string]any{"success": e == nil}
	if e != nil {
		result["error"] = e.Error()
	}
	b, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		return marshalErr
	}
	if mkdirErr := os.MkdirAll(c.ResultsDir, 0755); mkdirErr != nil {
		return mkdirErr
	}
	if writeErr := os.WriteFile(filepath.Join(c.ResultsDir, "status.json"), b, 0644); writeErr != nil {
		return writeErr
	}
	return e
}
func export(dir string, out io.Writer) error {
	tw := tar.NewWriter(out)
	walkErr := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("artifact is not a regular file")
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		name, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		h, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		h.Name = name
		if err = tw.WriteHeader(h); err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	closeErr := tw.Close()
	if walkErr != nil {
		return walkErr
	}
	return closeErr
}
