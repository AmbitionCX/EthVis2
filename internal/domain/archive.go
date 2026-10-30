package domain

import "time"

type RawRef struct {
	RequestSHA256 string            `json:"request_sha256,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Path          string            `json:"path"`
	SHA256        string            `json:"sha256"`
	Bytes         int64             `json:"bytes"`
	Status        int               `json:"status"`
	Endpoint      string            `json:"endpoint"`
	Method        string            `json:"method"`
	ContentType   string            `json:"content_type"`
	FetchedAt     time.Time         `json:"fetched_at"`
}
