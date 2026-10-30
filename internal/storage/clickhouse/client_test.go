package clickhouse

import (
	"encoding/json"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTypedChunksRouteAndNoRewardWrites(t *testing.T) {
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		if !strings.HasPrefix(q, "INSERT INTO test.") {
			t.Errorf("unexpected SQL %s", q)
		}
		if r.URL.Query().Get("output_format_json_quote_64bit_integers") != "1" {
			t.Error("unsafe JSON integer output")
		}
		d := json.NewDecoder(r.Body)
		for {
			var row domain.Row
			e := d.Decode(&row)
			if e == io.EOF {
				break
			}
			if e != nil {
				t.Error(e)
				return
			}
			if q != "INSERT INTO test."+row.Dataset+" FORMAT JSONEachRow" {
				t.Error("wrong typed table")
			}
			seen[row.Dataset]++
		}
	}))
	defer srv.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "rows.jsonl")
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	enc := json.NewEncoder(f)
	for _, table := range []string{"beacon_blocks", "validator_attestation_inclusions", "beacon_blocks"} {
		if e = enc.Encode(domain.Row{Run: "r", Task: "t", Attempt: "a", ID: "row", Dataset: table, Payload: "{}"}); e != nil {
			t.Fatal(e)
		}
	}
	f.Close()
	ch := NewCH(config.Config{CH: srv.URL, Database: "test", BatchRows: 1, BatchBytes: 1024})
	if e = ch.Insert(t.Context(), []string{path}); e != nil {
		t.Fatal(e)
	}
	if seen["beacon_blocks"] != 2 || seen["validator_attestation_inclusions"] != 1 {
		t.Fatal(seen)
	}
}
