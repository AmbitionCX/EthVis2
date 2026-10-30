package clickhouse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (ch *ClickHouse) Export(ctx context.Context, query, path string) error {
	// Reuse request URL/auth but stream to an atomic output instead of ReadAll.
	req, e := ch.NewRequest(ctx, query, nil)
	if e != nil {
		return e
	}
	resp, e := ch.HTTP.Do(req)
	if e != nil {
		return errors.New("ClickHouse export request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("ClickHouse export HTTP %d", resp.StatusCode)
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), "export-*.partial")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if !json.Valid(bytes.TrimSpace(line)) {
				return errors.New("invalid/truncated JSONEachRow export")
			}
			if _, e = f.Write(line); e != nil {
				return e
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
