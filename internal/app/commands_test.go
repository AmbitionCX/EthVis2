package app

import (
	"bytes"
	"strings"
	"testing"
)

func TestHelpAndInvalidFlagsDoNotReadEnvOrConnect(t *testing.T) {
	var out bytes.Buffer
	if e := Run(t.Context(), []string{"collect", "--help"}, &out); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(out.String(), "execute") || strings.Contains(out.String(), "profile") {
		t.Fatal("irrelevant flags in collect help")
	}
	if e := Run(t.Context(), []string{"collect", "--profile", "blocks"}, &out); e == nil {
		t.Fatal("ignored incompatible option")
	}
	if e := Run(t.Context(), []string{"unknown"}, &out); e == nil {
		t.Fatal("unknown command")
	}
}
