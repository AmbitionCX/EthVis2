package collector

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestConnectionChecksOnlyReadAndDoNotArchive(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.URL.Path)
		switch r.URL.Path {
		case "/eth/v1/node/version":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"data":{"version":"fixture"}}`))
		case "/":
			if r.URL.Query().Get("query") != "SELECT 1 FORMAT TabSeparated" {
				t.Error("non-read-only SQL")
			}
			w.Write([]byte("1\n"))
		default:
			t.Error("unexpected request")
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := testConfig(t)
	c.Teku = server.URL
	c.CH = server.URL
	c.PG = ""
	var out bytes.Buffer
	if err := CheckConnections(context.Background(), c, &out); err == nil {
		t.Fatal("missing PG configuration must fail")
	}
	for _, want := range []string{"[OK]   Teku", "[FAIL] PostgreSQL", "[OK]   ClickHouse"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %s: %s", want, out.String())
		}
	}
	if len(calls) != 2 {
		t.Fatalf("requests: %v", calls)
	}
	if _, err := os.Stat(c.ArchiveDir); !os.IsNotExist(err) {
		t.Fatal("connectivity check created archive")
	}
}

func TestClickHouseGatewayErrorsDoNotBlameCredentials(t *testing.T) {
	for _, status := range []int{502, 503, 504} {
		message := clickHouseStatusError(status, "9000").Error()
		if !strings.Contains(message, "gateway/proxy") || !strings.Contains(message, "credentials have not been verified") || !strings.Contains(message, "native TCP") {
			t.Fatalf("misleading gateway error: %s", message)
		}
		if strings.Contains(message, "check credentials and query permissions") {
			t.Fatalf("gateway error attributed to credentials: %s", message)
		}
	}
	if message := clickHouseStatusError(403, "8123").Error(); !strings.Contains(message, "authentication/access rejected") || strings.Contains(message, "native TCP") {
		t.Fatal(message)
	}
}
