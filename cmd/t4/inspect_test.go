package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/t4db/t4"
)

func TestInspectMeta(t *testing.T) {
	dataDir := seedInspectDB(t)

	stdout := &bytes.Buffer{}
	cmd := rootCmd()
	cmd.SetOut(stdout)
	cmd.SetErr(stdout)
	cmd.SetArgs([]string{"inspect", "meta", "--data-dir", dataDir})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("inspect meta: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "Current revision:  4") {
		t.Fatalf("expected current revision in output, got:\n%s", out)
	}
	if !strings.Contains(out, "Total keys:        2") {
		t.Fatalf("expected total keys in output, got:\n%s", out)
	}
}

func TestInspectGetJSON(t *testing.T) {
	dataDir := seedInspectDB(t)

	stdout := &bytes.Buffer{}
	cmd := rootCmd()
	cmd.SetOut(stdout)
	cmd.SetErr(stdout)
	cmd.SetArgs([]string{"inspect", "get", "--data-dir", dataDir, "--json", "/alpha"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("inspect get: %v", err)
	}

	var kv struct {
		Key      string `json:"Key"`
		Revision int64  `json:"Revision"`
		Value    []byte `json:"Value"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &kv); err != nil {
		t.Fatalf("decode inspect get json: %v\noutput=%s", err, stdout.String())
	}
	if kv.Key != "/alpha" || kv.Revision != 1 || string(kv.Value) != "one" {
		t.Fatalf("unexpected kv: %+v", kv)
	}
}

func TestInspectListAndCount(t *testing.T) {
	dataDir := seedInspectDB(t)

	listOut := &bytes.Buffer{}
	listCmd := rootCmd()
	listCmd.SetOut(listOut)
	listCmd.SetErr(listOut)
	listCmd.SetArgs([]string{"inspect", "list", "--data-dir", dataDir, "--prefix", "/a"})
	if err := listCmd.Execute(); err != nil {
		t.Fatalf("inspect list: %v", err)
	}
	if !strings.Contains(listOut.String(), "/alpha") {
		t.Fatalf("expected /alpha in list output, got:\n%s", listOut.String())
	}
	if strings.Contains(listOut.String(), "/beta") {
		t.Fatalf("did not expect /beta in prefix list output, got:\n%s", listOut.String())
	}

	countOut := &bytes.Buffer{}
	countCmd := rootCmd()
	countCmd.SetOut(countOut)
	countCmd.SetErr(countOut)
	countCmd.SetArgs([]string{"inspect", "count", "--data-dir", dataDir, "--prefix", "/"})
	if err := countCmd.Execute(); err != nil {
		t.Fatalf("inspect count: %v", err)
	}
	if strings.TrimSpace(countOut.String()) != "/: 2" {
		t.Fatalf("unexpected count output: %q", countOut.String())
	}
}

func TestInspectListPositionalPrefix(t *testing.T) {
	dataDir := seedPathInspectDB(t)

	stdout := &bytes.Buffer{}
	cmd := rootCmd()
	cmd.SetOut(stdout)
	cmd.SetErr(stdout)
	cmd.SetArgs([]string{"inspect", "list", "--data-dir", dataDir, "/foo"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("inspect list positional prefix: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "/foo/bar") || !strings.Contains(out, "/foo/baz") {
		t.Fatalf("expected /foo children in output, got:\n%s", out)
	}
	if strings.Contains(out, "/asdf") {
		t.Fatalf("did not expect /asdf in output, got:\n%s", out)
	}
}

func TestInspectListRejectsPositionalPrefixAndFlag(t *testing.T) {
	dataDir := seedPathInspectDB(t)

	stdout := &bytes.Buffer{}
	cmd := rootCmd()
	cmd.SetOut(stdout)
	cmd.SetErr(stdout)
	cmd.SetArgs([]string{"inspect", "list", "--data-dir", dataDir, "--prefix", "/foo", "/bar"})
	err := cmd.Execute()
	if err == nil {
		t.Fatalf("expected inspect list to reject both positional prefix and --prefix")
	}
	if !strings.Contains(err.Error(), "either a positional prefix or --prefix") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInspectHistory(t *testing.T) {
	dataDir := seedInspectDB(t)

	stdout := &bytes.Buffer{}
	cmd := rootCmd()
	cmd.SetOut(stdout)
	cmd.SetErr(stdout)
	cmd.SetArgs([]string{"inspect", "history", "--data-dir", dataDir, "/beta"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("inspect history: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "2    put") {
		t.Fatalf("expected initial put in history output, got:\n%s", out)
	}
	if !strings.Contains(out, "3    delete") {
		t.Fatalf("expected delete in history output, got:\n%s", out)
	}
	if !strings.Contains(out, "4    put") {
		t.Fatalf("expected recreate put in history output, got:\n%s", out)
	}
}

func TestInspectDiffJSON(t *testing.T) {
	dataDir := seedInspectDB(t)

	stdout := &bytes.Buffer{}
	cmd := rootCmd()
	cmd.SetOut(stdout)
	cmd.SetErr(stdout)
	cmd.SetArgs([]string{"inspect", "diff", "--data-dir", dataDir, "--from-rev", "2", "--to-rev", "4", "--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("inspect diff: %v", err)
	}

	var out struct {
		Changes []struct {
			Key           string `json:"key"`
			Type          string `json:"type"`
			FirstRevision int64  `json:"first_revision"`
			LastRevision  int64  `json:"last_revision"`
			Operations    int    `json:"operations"`
			BeforeValue   string `json:"before_value"`
			AfterValue    string `json:"after_value"`
		} `json:"changes"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode inspect diff json: %v\noutput=%s", err, stdout.String())
	}
	if len(out.Changes) != 1 {
		t.Fatalf("expected one changed key, got %+v", out.Changes)
	}
	change := out.Changes[0]
	if change.Key != "/beta" || change.Type != "created" {
		t.Fatalf("unexpected change summary: %+v", change)
	}
	if change.FirstRevision != 2 || change.LastRevision != 4 || change.Operations != 3 {
		t.Fatalf("unexpected revision summary: %+v", change)
	}
	if change.BeforeValue != "(absent)" || change.AfterValue != "\"three\"" {
		t.Fatalf("unexpected before/after summary: %+v", change)
	}
}

func seedInspectDB(t *testing.T) string {
	t.Helper()

	dataDir := t.TempDir()
	node, err := t4.Open(t4.Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}

	ctx := context.Background()
	if _, err := node.Put(ctx, "/alpha", []byte("one"), 0); err != nil {
		t.Fatalf("put /alpha: %v", err)
	}
	if _, err := node.Put(ctx, "/beta", []byte("two"), 0); err != nil {
		t.Fatalf("put /beta: %v", err)
	}
	if _, err := node.Delete(ctx, "/beta"); err != nil {
		t.Fatalf("delete /beta: %v", err)
	}
	if _, err := node.Put(ctx, "/beta", []byte("three"), 0); err != nil {
		t.Fatalf("recreate /beta: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}
	return dataDir
}

func seedPathInspectDB(t *testing.T) string {
	t.Helper()

	dataDir := t.TempDir()
	node, err := t4.Open(t4.Config{DataDir: dataDir})
	if err != nil {
		t.Fatalf("open node: %v", err)
	}

	ctx := context.Background()
	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "/foo/bar", value: "bar"},
		{key: "/foo/baz", value: "baz"},
		{key: "/asdf", value: "asdf"},
	} {
		if _, err := node.Put(ctx, tc.key, []byte(tc.value), 0); err != nil {
			t.Fatalf("put %s: %v", tc.key, err)
		}
	}
	if err := node.Close(); err != nil {
		t.Fatalf("close node: %v", err)
	}
	return dataDir
}
