package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_Valid(t *testing.T) {
	cfg, err := Load("../../testdata/config.valid.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Backends) != 3 {
		t.Fatalf("want 3 backends got %d", len(cfg.Backends))
	}
	if cfg.Algorithm != "round-robin" {
		t.Fatalf("algo=%s", cfg.Algorithm)
	}
}

func TestLoad_BadPortFailsFast(t *testing.T) {
	if _, err := Load("../../testdata/config.bad.yaml"); err == nil {
		t.Fatal("want error for bad port")
	}
}

func loadInline(t *testing.T, doc string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return Load(path)
}

func TestLoad_Invalid(t *testing.T) {
	validBackend := "  - {name: \"a\", host: \"127.0.0.1\", port: 9001}\n"
	header := "listen_addr: \":9000\"\nadmin_addr: \":8080\"\n" +
		"algorithm: \"round-robin\"\nbackends:\n"
	foot := "healthcheck: {interval_secs: 5, timeout_secs: 2, fail_threshold: 2}\n" +
		"timeouts: {dial_secs: 3, idle_secs: 60}\n"

	cases := []struct {
		name    string
		doc     string
		wantErr string
	}{
		{"missing host",
			header + "  - {name: \"a\", port: 9001}\n" + foot, "host"},
		{"port zero",
			header + "  - {name: \"a\", host: \"127.0.0.1\", port: 0}\n" + foot, "port"},
		{"port negative",
			header + "  - {name: \"a\", host: \"127.0.0.1\", port: -1}\n" + foot, "port"},
		{"port too large",
			header + "  - {name: \"a\", host: \"127.0.0.1\", port: 70000}\n" + foot, "port"},
		{"empty name",
			header + "  - {name: \"\", host: \"127.0.0.1\", port: 9001}\n" + foot, "name"},
		{"duplicate address",
			header + validBackend + validBackend + foot, "duplicate"},
		{"negative hc interval",
			header + validBackend +
				"healthcheck: {interval_secs: -1, timeout_secs: 2, fail_threshold: 2}\n" +
				"timeouts: {dial_secs: 3, idle_secs: 60}\n", "interval_secs"},
		{"negative hc timeout",
			header + validBackend +
				"healthcheck: {interval_secs: 5, timeout_secs: -2, fail_threshold: 2}\n" +
				"timeouts: {dial_secs: 3, idle_secs: 60}\n", "timeout_secs"},
		{"negative fail threshold",
			header + validBackend +
				"healthcheck: {interval_secs: 5, timeout_secs: 2, fail_threshold: -1}\n" +
				"timeouts: {dial_secs: 3, idle_secs: 60}\n", "fail_threshold"},
		{"negative dial timeout",
			header + validBackend +
				"healthcheck: {interval_secs: 5, timeout_secs: 2, fail_threshold: 2}\n" +
				"timeouts: {dial_secs: -3, idle_secs: 60}\n", "dial_secs"},
		{"negative idle timeout",
			header + validBackend +
				"healthcheck: {interval_secs: 5, timeout_secs: 2, fail_threshold: 2}\n" +
				"timeouts: {dial_secs: 3, idle_secs: -60}\n", "idle_secs"},
		{"malformed listen addr",
			"listen_addr: \":99999\"\nadmin_addr: \":8080\"\n" +
				"algorithm: \"round-robin\"\nbackends:\n" + validBackend + foot, "listen_addr"},
		{"malformed admin addr",
			"listen_addr: \":9000\"\nadmin_addr: \"not-an-address\"\n" +
				"algorithm: \"round-robin\"\nbackends:\n" + validBackend + foot, "admin_addr"},
		{"malformed backend host",
			header + "  - {name: \"a\", host: \"bad host\", port: 9001}\n" + foot, "backend"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadInline(t, tc.doc)
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestLoad_ZeroMeansDefault(t *testing.T) {
	doc := "listen_addr: \":9000\"\nadmin_addr: \":8080\"\n" +
		"algorithm: \"least-conn\"\nbackends:\n" +
		"  - {name: \"a\", host: \"127.0.0.1\", port: 9001}\n" +
		"healthcheck: {interval_secs: 0, timeout_secs: 0, fail_threshold: 0}\n" +
		"timeouts: {dial_secs: 0, idle_secs: 0}\n"
	cfg, err := loadInline(t, doc)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Healthcheck.IntervalSecs != 5 || cfg.Healthcheck.TimeoutSecs != 2 ||
		cfg.Healthcheck.FailThreshold != 2 || cfg.Timeouts.DialSecs != 3 ||
		cfg.Timeouts.IdleSecs != 60 {
		t.Fatalf("defaults not applied: %+v %+v", cfg.Healthcheck, cfg.Timeouts)
	}
}
