package oceanbase

import (
	"database/sql/driver"
	"strings"
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

func TestParseSessionTimeZone(t *testing.T) {
	cfg, err := ParseDSN("oboracle://u:p@127.0.0.1:2881/db?sessionTimeZone=%2B08%3A00")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SessionTimeZone != "+08:00" {
		t.Fatalf("session timezone = %q, want +08:00", cfg.SessionTimeZone)
	}

	loc, sqlValue, enabled, err := resolveSessionTimeZone("oracle", cfg.SessionTimeZone)
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || sqlValue != "+08:00" || loc.String() != "UTC+08:00" {
		t.Fatalf("resolved timezone = loc %q, sql %q, enabled %v", loc, sqlValue, enabled)
	}

	if _, _, err = parseSessionTimeZone("Asia/Shanghai"); err == nil {
		t.Fatal("IANA region should be rejected for deterministic Oracle timestamp decoding")
	}
}

func TestOracleSessionTimeZoneDefaultsToUTC(t *testing.T) {
	loc, sqlValue, enabled, err := resolveSessionTimeZone("oracle", "")
	if err != nil {
		t.Fatal(err)
	}
	if !enabled || sqlValue != "+00:00" || loc != time.UTC {
		t.Fatalf("resolved default timezone = loc %v, sql %q, enabled %v", loc, sqlValue, enabled)
	}

	loc, sqlValue, enabled, err = resolveSessionTimeZone("mysql", "")
	if err != nil {
		t.Fatal(err)
	}
	if enabled || sqlValue != "" || loc != time.UTC {
		t.Fatalf("resolved mysql timezone = loc %v, sql %q, enabled %v", loc, sqlValue, enabled)
	}
}

func TestParseSessionTimeZoneRejectsInvalidValue(t *testing.T) {
	for _, value := range []string{"+14:00", "+08:60", "Local", "UTC+08:00", "Asia/Shanghai", "bad'zone"} {
		if _, _, err := parseSessionTimeZone(value); err == nil {
			t.Errorf("parseSessionTimeZone(%q) error = nil, want error", value)
		}
	}
}

func TestParseSessionTimeZoneRejectsSignedOffsetComponents(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{value: "+-1:00", want: "invalid hour"},
		{value: "++5:00", want: "invalid hour"},
		{value: "+01:-1", want: "invalid minute"},
		{value: "+01:+1", want: "invalid minute"},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if _, _, err := parseSessionTimeZone(tt.value); err == nil {
				t.Fatalf("parseSessionTimeZone(%q) error = nil, want %s error", tt.value, tt.want)
			} else if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parseSessionTimeZone(%q) error = %v, want %s error", tt.value, err, tt.want)
			}
		})
	}
}

func TestParseOracleMode(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{"", "auto"},
		{"auto", "auto"},
		{"true", "true"},
		{"false", "false"},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			cfg, err := ParseDSN("oceanbase://u:p@127.0.0.1:2881/db?oracleMode=" + tt.value)
			if tt.value == "" {
				cfg, err = ParseDSN("oceanbase://u:p@127.0.0.1:2881/db")
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.OracleMode != tt.want {
				t.Fatalf("oracle mode = %q, want %q", cfg.OracleMode, tt.want)
			}
		})
	}
	if _, err := ParseDSN("oceanbase://u:p@127.0.0.1:2881/db?oracleMode=maybe"); err == nil {
		t.Fatal("invalid oracle mode should fail")
	}
}

func TestSessionTimeZoneValueFromQuery(t *testing.T) {
	tests := []struct {
		query string
		want  string
	}{
		{"ALTER SESSION SET TIME_ZONE = '+08:00'", "+08:00"},
		{"alter session set time_zone='Asia/Shanghai';", "Asia/Shanghai"},
		{"SET time_zone = '-05:30'", "-05:30"},
		{"SET SESSION time_zone = '+08:00'", "+08:00"},
		{"SET @@session.time_zone = 'UTC'", "UTC"},
		{"ALTER SYSTEM SESSION SET TIME_ZONE = '+00:00'", "+00:00"},
		{"SET SESSION time_zone = ?", "?"},
		{"/* leading */ ALTER SESSION SET TIME_ZONE = '+08:00' /* trailing */;", "+08:00"},
		{"ALTER SESSION SET TIME_ZONE = '+08:00' -- trailing comment\n", "+08:00"},
	}
	for _, tt := range tests {
		got, ok := sessionTimeZoneValueFromQuery(tt.query)
		if !ok || got != tt.want {
			t.Errorf("sessionTimeZoneValueFromQuery(%q) = %q, %v; want %q, true", tt.query, got, ok, tt.want)
		}
	}
	for _, query := range []string{
		"ALTER SESSION SET TIME_ZONE",
		"ALTER SESSION SET TIME_ZONE = '+08:00' trailing",
		"SELECT 1",
	} {
		if _, ok := sessionTimeZoneValueFromQuery(query); ok {
			t.Errorf("sessionTimeZoneValueFromQuery(%q) = true, want false", query)
		}
	}
}

func TestUpdateSessionTimeZoneFromSetSession(t *testing.T) {
	conn := &Conn{sessionLocation: time.UTC}
	conn.updateSessionTimeZoneFromQuery("SET SESSION time_zone = '+08:00'")
	if _, offset := time.Now().In(conn.sessionLocation).Zone(); offset != 8*60*60 {
		t.Fatalf("session location = %v, want +08:00", conn.sessionLocation)
	}
}

func TestUpdateSessionTimeZoneFromQueryIgnoresMySQLMode(t *testing.T) {
	conn := &Conn{tenantMode: "mysql", sessionLocation: time.UTC}
	if err := conn.updateSessionTimeZoneFromQuery("SET SESSION time_zone = 'Asia/Shanghai'"); err != nil {
		t.Fatalf("updateSessionTimeZoneFromQuery() error = %v, want nil in MySQL mode", err)
	}
	if conn.bad.Load() {
		t.Fatal("MySQL session timezone change should not retire the connection")
	}
	if conn.sessionLocation != time.UTC {
		t.Fatalf("session location = %v, want unchanged UTC in MySQL mode", conn.sessionLocation)
	}
}

func TestUpdateSessionTimeZoneFromQueryAllowsTrailingSQLComment(t *testing.T) {
	conn := &Conn{sessionLocation: time.UTC}
	if err := conn.updateSessionTimeZoneFromQuery("ALTER SESSION SET TIME_ZONE = '+08:00' /* comment */"); err != nil {
		t.Fatalf("updateSessionTimeZoneFromQuery() error = %v", err)
	}
	if _, offset := time.Now().In(conn.sessionLocation).Zone(); offset != 8*60*60 {
		t.Fatalf("session location = %v, want +08:00", conn.sessionLocation)
	}
}

func TestUpdateSessionTimeZoneRetiresUnparsedMutation(t *testing.T) {
	conn := &Conn{sessionLocation: time.UTC}
	err := conn.updateSessionTimeZoneFromQuery("ALTER SESSION SET TIME_ZONE = '+08:00'; SELECT 1")
	if err == nil {
		t.Fatal("updateSessionTimeZoneFromQuery() error = nil, want synchronization error")
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired when a session timezone mutation cannot be parsed safely")
	}
}

func TestUpdateSessionTimeZoneFromPreparedArgument(t *testing.T) {
	conn := &Conn{sessionLocation: time.UTC}
	err := conn.updateSessionTimeZoneFromQuery(
		"SET SESSION time_zone = ?",
		[]driver.NamedValue{{Ordinal: 1, Value: "+08:00"}},
	)
	if err != nil {
		t.Fatalf("updateSessionTimeZoneFromQuery() error = %v", err)
	}
	if _, offset := time.Now().In(conn.sessionLocation).Zone(); offset != 8*60*60 {
		t.Fatalf("session location = %v, want +08:00", conn.sessionLocation)
	}
}

func TestUpdateSessionTimeZoneRetiresConnectionWhenLocalDecodeFails(t *testing.T) {
	conn := &Conn{sessionLocation: time.UTC}
	err := conn.updateSessionTimeZoneFromQuery(
		"SET SESSION time_zone = ?",
		[]driver.NamedValue{{Ordinal: 1, Value: 42}},
	)
	if err == nil {
		t.Fatal("updateSessionTimeZoneFromQuery() error = nil, want synchronization error")
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after local timezone synchronization failure")
	}
	if conn.sessionLocation != nil {
		t.Fatalf("session location = %v, want unknown", conn.sessionLocation)
	}
}
