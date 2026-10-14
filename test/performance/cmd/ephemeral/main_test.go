package main

import (
	"archive/tar"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestArtifactExport(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`{"success":false}`)
	if err := os.WriteFile(filepath.Join(dir, "status.json"), content, 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := export(dir, &out); err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(&out)
	h, err := reader.Next()
	if err != nil {
		t.Fatal(err)
	}
	if h.Name != "status.json" {
		t.Fatalf("unsafe or incorrect archive name %q", h.Name)
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, content) {
		t.Fatal("export changed artifact")
	}
	if _, err = reader.Next(); err != io.EOF {
		t.Fatalf("archive did not finish: %v", err)
	}
}
