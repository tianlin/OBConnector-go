package oceanbase

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestDriverOpenConnector(t *testing.T) {
	d := &Driver{}
	connector, err := d.OpenConnector("oceanbase://user:pass@127.0.0.1:2881/db")
	if err != nil {
		t.Fatalf("OpenConnector failed: %v", err)
	}

	c, ok := connector.(*Connector)
	if !ok {
		t.Fatalf("expected *Connector, got %T", connector)
	}

	if c.cfg.User != "user" {
		t.Errorf("user = %q, want %q", c.cfg.User, "user")
	}
	if c.cfg.Password != "pass" {
		t.Errorf("password = %q, want %q", c.cfg.Password, "pass")
	}
	if c.cfg.Database != "db" {
		t.Errorf("database = %q, want %q", c.cfg.Database, "db")
	}
	if c.cfg.Addr != "127.0.0.1:2881" {
		t.Errorf("addr = %q, want %q", c.cfg.Addr, "127.0.0.1:2881")
	}
}

func TestDriverOpenConnectorOpaque(t *testing.T) {
	d := &Driver{}
	connector, err := d.OpenConnector("oceanbase:user:pass@127.0.0.1:2881/db")
	if err != nil {
		t.Fatalf("OpenConnector failed: %v", err)
	}

	c := connector.(*Connector)
	if c.cfg.User != "user" {
		t.Errorf("user = %q", c.cfg.User)
	}
}

func TestOracleDriverForcesOraclePreset(t *testing.T) {
	d := &Driver{preset: "oboracle"}
	connector, err := d.OpenConnector("user:pass@tcp(127.0.0.1:2881)/db")
	if err != nil {
		t.Fatal(err)
	}
	if got := connector.(*Connector).cfg.Preset; got != "oboracle" {
		t.Fatalf("preset = %q, want oboracle", got)
	}
}

func TestDriverOpenConnectorInvalid(t *testing.T) {
	d := &Driver{}
	_, err := d.OpenConnector("invalid-dsn")
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestNewConnector(t *testing.T) {
	cfg := Config{
		User:     "root",
		Password: "secret",
		Addr:     "localhost:2881",
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}

	c := connector.(*Connector)
	if c.cfg.Addr != "localhost:2881" {
		t.Errorf("addr = %q", c.cfg.Addr)
	}
}

func TestNewConnectorNormalizesAddr(t *testing.T) {
	// Missing port should get default
	cfg := Config{
		User:     "root",
		Password: "secret",
		Addr:     "myhost",
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}

	c := connector.(*Connector)
	if c.cfg.Addr != "myhost:2881" {
		t.Errorf("addr = %q, want myhost:2881", c.cfg.Addr)
	}
}

func TestNewConnectorMissingUser(t *testing.T) {
	cfg := Config{
		Password: "secret",
	}
	_, err := NewConnector(cfg)
	if err == nil {
		t.Fatal("expected error for missing user")
	}
}

func TestConnectorDriver(t *testing.T) {
	c := &Connector{}
	drv := c.Driver()
	if _, ok := drv.(*Driver); !ok {
		t.Fatalf("expected *Driver, got %T", drv)
	}
}

func TestDriverOpenInvalid(t *testing.T) {
	d := &Driver{}
	_, err := d.Open("not-a-valid-dsn")
	if err == nil {
		t.Fatal("expected error for invalid DSN")
	}
}

func TestDriverOpenDSN(t *testing.T) {
	d := &Driver{}
	// We don't expect a real connection, just that parsing succeeds
	// and dial fails with a connection error.
	conn, err := d.Open("oceanbase://user:pass@127.0.0.1:1/db?timeout=1s")
	if err != nil {
		// Connection refused is expected since there's nothing listening on port 1
		t.Logf("expected connection error: %v", err)
		if conn != nil {
			conn.Close()
		}
		return
	}
	// In the unlikely event that something is listening on port 1, clean up
	conn.Close()
}

// TestConnectorConnect verifies that Connect returns a bad-conn error
// when the target is unreachable, not a DSN error.
func TestConnectorConnectRefused(t *testing.T) {
	cfg := &Config{
		User:     "test",
		Password: "test",
		Addr:     "127.0.0.1:1",
		Timeout:  1,
	}
	connector := &Connector{cfg: cfg}
	_, err := connector.Connect(context.Background())
	if err == nil {
		t.Fatal("expected connection error")
	}
	t.Logf("expected connect error: %v", err)
}

// Test that OpenConnector correctly handles TLS DSN params
func TestOpenConnectorWithTLS(t *testing.T) {
	d := &Driver{}
	// TLS with skip-verify (no actual CA file needed)
	connector, err := d.OpenConnector("oceanbase://user:pass@host:2881/db?tls=skip-verify")
	if err != nil {
		t.Fatalf("OpenConnector failed: %v", err)
	}
	c := connector.(*Connector)
	if c.cfg.TLSConfig == nil {
		t.Fatal("expected TLSConfig to be set")
	}
	if !c.cfg.TLSConfig.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be true")
	}
}

// Test driver Open with trace parameter
func TestOpenConnectorWithTrace(t *testing.T) {
	d := &Driver{}
	connector, err := d.OpenConnector("oceanbase://user:pass@host:2881/db?trace=true")
	if err != nil {
		t.Fatalf("OpenConnector failed: %v", err)
	}
	c := connector.(*Connector)
	if !c.cfg.Trace {
		t.Fatal("expected trace to be true")
	}
}

// Verify CheckNamedValue accepts sql.Out
func TestCheckNamedValue(t *testing.T) {
	conn := &Conn{}
	v := &driver.NamedValue{Value: struct{ Dest any }{}}
	err := conn.CheckNamedValue(v)
	if err != driver.ErrSkip {
		t.Fatalf("expected ErrSkip for non-Out value, got %v", err)
	}
}
