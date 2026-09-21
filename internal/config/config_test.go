package config

import "testing"

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
