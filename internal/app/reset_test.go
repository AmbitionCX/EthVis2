package app

import (
	"ethvis2/internal/config"
	"os"
	"path/filepath"
	"testing"
)

func TestResetRefusesParentExternalAndSymlinkPaths(t *testing.T) {
	repo, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	t.Chdir(repo)
	cwd, e := os.Getwd()
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"/", cwd, "var", "../outside"} {
		if _, e = ResetPaths(config.Config{ArchiveDir: path, RunsDir: "var/runs"}); e == nil {
			t.Fatalf("unsafe cleanup path accepted %s", path)
		}
	}
	if _, e = ResetPaths(config.Config{ArchiveDir: "var/archive", RunsDir: "var/runs"}); e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	if e = os.WriteFile("go.mod", []byte("module ethvis2\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll("var", 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink(t.TempDir(), filepath.Join("var", "archive")); e != nil {
		t.Fatal(e)
	}
	if _, e = ResetPaths(config.Config{ArchiveDir: "var/archive", RunsDir: "var/runs"}); e == nil {
		t.Fatal("symlink accepted")
	}
}
