package main

import (
	"strings"
	"testing"
	"time"
)

func TestApplyExperimentParamsOpaqueDSN(t *testing.T) {
	dsn, err := applyExperimentParams("oceanbase:u:p@127.0.0.1:2883/db?TIMEOUT=5", true, "", "", "", "oboracle", false, false, false, "", "", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "TIMEOUT=5&") {
		t.Fatalf("original query not preserved: %s", dsn)
	}
	if !strings.Contains(dsn, "trace=true") {
		t.Fatalf("trace not appended: %s", dsn)
	}
	if !strings.Contains(dsn, "preset=oboracle") {
		t.Fatalf("preset not appended: %s", dsn)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		value any
		want  string
	}{
		{nil, "NULL"},
		{[]byte("abc"), "abc"},
		{int64(42), "42"},
		{"hello", "hello"},
	}
	for _, tt := range tests {
		if got := formatValue(tt.value); got != tt.want {
			t.Fatalf("formatValue(%#v) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

func TestSmokeTableName(t *testing.T) {
	generated, err := smokeTableName("")
	if err != nil {
		t.Fatal(err)
	}
	if !validIdentifier.MatchString(generated) {
		t.Fatalf("generated invalid table name %q", generated)
	}

	named, err := smokeTableName("my_table_1")
	if err != nil {
		t.Fatal(err)
	}
	if named != "MY_TABLE_1" {
		t.Fatalf("named table = %q", named)
	}

	if _, err := smokeTableName("1bad"); err == nil {
		t.Fatal("invalid table name should fail")
	}
}

func TestIsMissingTableError(t *testing.T) {
	if !isMissingTableError(errString("oceanbase: error 942 (42S02): ORA-00942: table does not exist")) {
		t.Fatal("ORA-00942 should be recognized")
	}
	if !isMissingTableError(errString("oceanbase: error 1051 (42S02): Unknown table 'foo'")) {
		t.Fatal("MySQL error 1051 should be recognized")
	}
	if !isMissingTableError(errString("Unknown table 'foo'")) {
		t.Fatal("'Unknown table' string should be recognized")
	}
	if isMissingTableError(errString("some other error")) {
		t.Fatal("unrelated error should not be recognized")
	}
}

type errString string

func (e errString) Error() string { return string(e) }

func TestResultValue(t *testing.T) {
	if got := resultValue(3, nil); got != "3" {
		t.Fatalf("resultValue = %q", got)
	}
	if got := resultValue(0, errString("unsupported")); got != "unknown" {
		t.Fatalf("resultValue error = %q", got)
	}
}

func TestOracleModeSetsPreset(t *testing.T) {
	dsn, err := applyExperimentParams("oceanbase:u:p@127.0.0.1:2883/db", false, "", "", "", "", false, true, false, "", "", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "preset=oboracle") {
		t.Fatalf("expected preset=oboracle, got %s", dsn)
	}
	if strings.Contains(dsn, "oracleMode") {
		t.Fatalf("should not contain oracleMode, got %s", dsn)
	}
}

func TestResolveCharset(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{"GBK", "28"},
		{"gbk", "28"},
		{"UTF8", "33"},
		{"utf8mb4", "45"},
		{"ascii", "11"},
		{"binary", "63"},
		{"unknown_charset", ""},
	}
	for _, tt := range tests {
		got := resolveCharset(tt.name)
		if got != tt.id {
			t.Errorf("resolveCharset(%q) = %q, want %q", tt.name, got, tt.id)
		}
	}
}

func TestCompressFlag(t *testing.T) {
	dsn, err := applyExperimentParams("oceanbase:u:p@127.0.0.1:2883/db", false, "", "", "", "", false, false, true, "", "", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "compress=true") {
		t.Fatalf("expected compress=true, got %s", dsn)
	}
}

func TestOB20MagicFlag(t *testing.T) {
	dsn, err := applyExperimentParams("oceanbase:u:p@127.0.0.1:2883/db", false, "", "", "", "", false, false, false, "", "0xCAFE", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "ob20.magic=0xCAFE") {
		t.Fatalf("expected ob20.magic=0xCAFE, got %s", dsn)
	}
}

func TestCharsetFlag(t *testing.T) {
	dsn, err := applyExperimentParams("oceanbase:u:p@127.0.0.1:2883/db", false, "", "", "", "", false, false, false, "GBK", "", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "collation=28") {
		t.Fatalf("expected collation=28 for GBK, got %s", dsn)
	}
}

func TestBuildDSNWithAddrs(t *testing.T) {
	dsn := buildDSN("u", "p", "127.0.0.1", "2883", "db", 5*time.Second, false, "", "", "", "", false, false, false, "", "", "10.0.0.1:2883,10.0.0.2:2883", nil, nil)
	if !strings.Contains(dsn, "10.0.0.1:2883,10.0.0.2:2883") {
		t.Fatalf("expected multi-address in DSN, got %s", dsn)
	}
}
