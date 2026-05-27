package oceanbase

import (
	"testing"
	"time"
)

func TestParseOpaqueDSN(t *testing.T) {
	cfg, err := ParseDSN("oceanbase:sys%40tenant%23cluster:p%40ss@192.0.2.10:2883/DB?CONNECT%20TIMEOUT=5&TIMEOUT=10&trace=true")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "sys@tenant#cluster" {
		t.Fatalf("user = %q", cfg.User)
	}
	if cfg.Password != "p@ss" {
		t.Fatalf("password = %q", cfg.Password)
	}
	if cfg.Addr != "192.0.2.10:2883" {
		t.Fatalf("addr = %q", cfg.Addr)
	}
	if cfg.Database != "DB" {
		t.Fatalf("database = %q", cfg.Database)
	}
	if cfg.Timeout != 10*time.Second {
		t.Fatalf("timeout = %s", cfg.Timeout)
	}
	if !cfg.Trace {
		t.Fatal("trace should be enabled")
	}
}

func TestParseURLDSNAttributes(t *testing.T) {
	cfg, err := ParseDSN("oceanbase://u:p@127.0.0.1:2883/db?attr.foo=bar&preset=oboracle&cap.add=0x80&cap.drop=0x20&collation=46&init=select+1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Attributes["foo"] != "bar" {
		t.Fatalf("attr foo = %q", cfg.Attributes["foo"])
	}
	if cfg.Preset != "oboracle" {
		t.Fatalf("preset = %q", cfg.Preset)
	}
	if cfg.CapabilityAdd != 0x80 || cfg.CapabilityDrop != 0x20 {
		t.Fatalf("cap add/drop = %#x/%#x", cfg.CapabilityAdd, cfg.CapabilityDrop)
	}
	if cfg.Collation != 46 {
		t.Fatalf("collation = %d", cfg.Collation)
	}
	if len(cfg.InitSQL) != 1 || cfg.InitSQL[0] != "select 1" {
		t.Fatalf("init sql = %#v", cfg.InitSQL)
	}
}

func TestParseObOracleSchemeAutoPreset(t *testing.T) {
	// Test URL scheme
	cfg, err := ParseDSN("oboracle://u:p@127.0.0.1:2883/db")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Preset != "oboracle" {
		t.Fatalf("expected preset 'oboracle', got %q", cfg.Preset)
	}

	// Test Opaque scheme
	cfg2, err := ParseDSN("oboracle:u:p@127.0.0.1:2883/db")
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.Preset != "oboracle" {
		t.Fatalf("expected preset 'oboracle', got %q", cfg2.Preset)
	}
}
