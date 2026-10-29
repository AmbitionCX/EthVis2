package collector

import (
	"bufio"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Teku, PG, CH, Database, CHUser, CHPassword, ArchiveDir, RunsDir string
	Blocks, Workers, StateWorkers, Attempts, BatchRows, BatchBytes  int
	ValidatorBatchSize                                              int
	Timeout                                                         time.Duration
	SSZ                                                             bool
}

// LoadEnv never evaluates shell code. Existing process variables take precedence.
func LoadEnv(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		s = strings.TrimPrefix(s, "export ")
		k, v, ok := strings.Cut(s, "=")
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if !ok || k == "" || strings.ContainsAny(k, " \t") {
			return fmt.Errorf("invalid .env line %d", line)
		}
		if len(v) >= 2 && ((v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'')) {
			v = v[1 : len(v)-1]
		}
		if _, exists := os.LookupEnv(k); !exists {
			if err := os.Setenv(k, v); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}
func env(k, d string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return d
}
func ReadConfig() (Config, error) {
	c := Config{Teku: env("TEKU_API", ""), PG: env("POSTGRESQL_API", ""), CH: env("CLICKHOUSE_API", ""), Database: env("CLICKHOUSE_DATABASE", "ethvis"), CHUser: env("CLICKHOUSE_USER", "default"), CHPassword: env("CLICKHOUSE_PASSWORD", ""), ArchiveDir: env("ARCHIVE_DIR", "./var/archive"), RunsDir: env("RUNS_DIR", "./var/runs")}
	for k, p := range map[string]*int{"BLOCK_COUNT": &c.Blocks, "WORKERS": &c.Workers, "STATE_WORKERS": &c.StateWorkers, "HTTP_ATTEMPTS": &c.Attempts, "BATCH_ROWS": &c.BatchRows, "BATCH_BYTES": &c.BatchBytes, "VALIDATOR_BATCH_SIZE": &c.ValidatorBatchSize} {
		defaults := map[string]string{"BLOCK_COUNT": "100000", "WORKERS": "4", "STATE_WORKERS": "1", "HTTP_ATTEMPTS": "4", "BATCH_ROWS": "20000", "BATCH_BYTES": "16777216", "VALIDATOR_BATCH_SIZE": "500"}
		n, e := strconv.Atoi(env(k, defaults[k]))
		if e != nil || n < 1 {
			return c, fmt.Errorf("%s must be positive", k)
		}
		*p = n
	}
	if c.ValidatorBatchSize > 10000 {
		return c, errors.New("VALIDATOR_BATCH_SIZE must be <= 10000")
	}
	sec, e := strconv.Atoi(env("HTTP_TIMEOUT_SECONDS", "180"))
	if e != nil || sec < 1 {
		return c, errors.New("invalid HTTP_TIMEOUT_SECONDS")
	}
	c.Timeout = time.Duration(sec) * time.Second
	if u, err := url.Parse(c.CH); err == nil && u.User != nil {
		c.CHUser = u.User.Username()
		if password, ok := u.User.Password(); ok {
			c.CHPassword = password
		}
		u.User = nil
		c.CH = u.String()
	}
	c.SSZ, e = strconv.ParseBool(env("COLLECT_SSZ", "false"))
	if e != nil {
		return c, errors.New("invalid COLLECT_SSZ")
	}
	if !identifier(c.Database) {
		return c, errors.New("invalid CLICKHOUSE_DATABASE identifier")
	}
	return c, nil
}
func identifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
func (c Config) ValidateSource() error {
	u, e := url.Parse(c.Teku)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("fill TEKU_API with an http(s) URL")
	}
	return nil
}
func (c Config) ValidateStores() error {
	if c.PG == "" {
		return errors.New("fill POSTGRESQL_API with a PostgreSQL DSN")
	}
	u, e := url.Parse(c.CH)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("fill CLICKHOUSE_API with an HTTP interface URL (usually port 8123)")
	}
	return nil
}
func (c Config) ManifestPath(id string) (string, error) {
	if !identifier(id) {
		return "", errors.New("invalid run id")
	}
	return filepath.Join(c.RunsDir, id+".json"), nil
}
