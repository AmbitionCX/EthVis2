package app

import (
	"context"
	"encoding/json"
	"errors"
	"ethvis2/internal/config"
	"ethvis2/internal/storage/clickhouse"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CheckConnections only performs read requests/SELECT 1. It does not use Source
// (which archives responses), migrate schemas, or register collection tasks.
func CheckConnections(ctx context.Context, c config.Config, out io.Writer) error {
	checks := []struct {
		name  string
		check func(context.Context) error
	}{
		{"Teku", func(ctx context.Context) error { return checkTeku(ctx, c) }},
		{"PostgreSQL", func(ctx context.Context) error {
			if c.PG == "" {
				return errors.New("POSTGRESQL_API is empty")
			}
			p, e := postgres.OpenControl(ctx, c)
			if e != nil {
				return errors.New("connection/authentication failed; check DSN, network and credentials")
			}
			defer p.Close()
			var value int
			if e = p.Pool.QueryRow(ctx, "SELECT 1").Scan(&value); e != nil || value != 1 {
				return errors.New("SELECT 1 failed")
			}
			return nil
		}},
		{"ClickHouse", func(ctx context.Context) error {
			u, e := url.Parse(c.CH)
			if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
				return errors.New("CLICKHOUSE_API must be an HTTP(S) URL")
			}
			ch := clickhouse.NewCH(c)
			req, e := ch.NewRequest(ctx, "SELECT 1 FORMAT TabSeparated", nil)
			if e != nil {
				return errors.New("invalid HTTP connection configuration")
			}
			resp, e := ch.HTTP.Do(req)
			if e != nil {
				return errors.New("HTTP connection failed; check network and HTTP port")
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return clickHouseStatusError(resp.StatusCode, u.Port())
			}
			body, e := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if e != nil || strings.TrimSpace(string(body)) != "1" {
				return errors.New("unexpected SELECT 1 response")
			}
			return nil
		}},
	}
	failed := false
	for _, check := range checks {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := check.check(checkCtx)
		cancel()
		if err != nil {
			failed = true
			fmt.Fprintf(out, "[FAIL] %s: %s\n", check.name, err)
		} else {
			fmt.Fprintf(out, "[OK]   %s\n", check.name)
		}
	}
	if failed {
		return errors.New("one or more connection checks failed")
	}
	return nil
}
func checkTeku(ctx context.Context, c config.Config) error {
	if e := c.ValidateSource(); e != nil {
		return e
	}
	u, _ := url.Parse(c.Teku)
	u.Path = strings.TrimRight(u.Path, "/") + "/eth/v1/node/version"
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if e != nil {
		return errors.New("invalid Teku request")
	}
	req.Header.Set("Accept", "application/json")
	resp, e := http.DefaultClient.Do(req)
	if e != nil {
		return errors.New("REST connection failed; check network and REST port")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from node/version", resp.StatusCode)
	}
	var result struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if e = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); e != nil || result.Data.Version == "" {
		return errors.New("unexpected Beacon node/version response")
	}
	return nil
}

// HTTP gateway errors do not establish that ClickHouse rejected credentials.
func clickHouseStatusError(status int, port string) error {
	var hint string
	switch status {
	case 401, 403:
		hint = "authentication/access rejected; check credentials and query permissions"
	case 407:
		hint = "HTTP proxy authentication required; this is not ClickHouse authentication"
	case 502, 503, 504:
		hint = "gateway/proxy or upstream unavailable; check proxy settings, server reachability and HTTP port; credentials have not been verified"
	case 404:
		hint = "HTTP endpoint/path not found; check reverse proxy routing and API URL"
	default:
		hint = "request failed; check HTTP endpoint and ClickHouse/proxy logs"
	}
	if port == "9000" || port == "9440" {
		hint += "; configured port is normally a native TCP port, verify HTTP port/mapping (normally 8123, or HTTPS 8443)"
	}
	return fmt.Errorf("HTTP %d; %s", status, hint)
}
