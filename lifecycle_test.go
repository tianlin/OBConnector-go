package oceanbase

import (
	"database/sql/driver"
	"errors"
	"io"
	"testing"
)

func TestIsValid(t *testing.T) {
	conn := &Conn{}
	if !conn.IsValid() {
		t.Fatal("new connection should be valid")
	}
	conn.bad.Store(true)
	if conn.IsValid() {
		t.Fatal("bad connection should be invalid")
	}
	conn.bad.Store(false)
	conn.closed = true
	if conn.IsValid() {
		t.Fatal("closed connection should be invalid")
	}
}

func TestCheckUsableLocked(t *testing.T) {
	conn := &Conn{}
	conn.bad.Store(true)
	if err := conn.checkUsableLocked(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want ErrBadConn", err)
	}
}

func TestMarkBadIfConnErr(t *testing.T) {
	conn := &Conn{}
	if err := conn.markBadIfConnErr(io.ErrUnexpectedEOF); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("err = %v, want ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be marked bad")
	}
}

func TestServerErrorIsNotBadConn(t *testing.T) {
	conn := &Conn{}
	err := &ServerError{Number: 942, SQLState: "42S02", Message: "missing"}
	if got := conn.markBadIfConnErr(err); got != err {
		t.Fatalf("server error changed: %v", got)
	}
	if conn.bad.Load() {
		t.Fatal("server error should not mark bad connection")
	}
}

func TestBadFieldIsAtomic(t *testing.T) {
	conn := &Conn{}
	_ = conn.bad.Load()
}
