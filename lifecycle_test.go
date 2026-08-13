package oceanbase

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
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

func TestResetSessionReappliesOracleTimeZone(t *testing.T) {
	client, server := net.Pipe()
	type exchange struct {
		request []byte
		err     error
	}
	result := make(chan exchange, 1)
	go func() {
		defer server.Close()
		var header [4]byte
		if _, err := io.ReadFull(server, header[:]); err != nil {
			result <- exchange{err: err}
			return
		}
		request := make([]byte, int(header[0])|int(header[1])<<8|int(header[2])<<16)
		if _, err := io.ReadFull(server, request); err != nil {
			result <- exchange{err: err}
			return
		}
		response := []byte{protocol.OKPacket, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}
		binary.LittleEndian.PutUint32(header[:], uint32(len(response)))
		header[3] = 1
		if _, err := server.Write(header[:]); err != nil {
			result <- exchange{err: err}
			return
		}
		if _, err := server.Write(response); err != nil {
			result <- exchange{err: err}
			return
		}
		result <- exchange{request: request}
	}()

	conn := &Conn{
		netConn: client,
		packets: protocol.NewPacketConn(client),
		cfg: &Config{
			Timeout:         time.Second,
			User:            "test",
			OracleMode:      "true",
			SessionTimeZone: "+08:00",
		},
		tenantMode:      "oracle",
		sessionLocation: time.UTC,
	}
	err := conn.ResetSession(context.Background())
	_ = client.Close()
	if err != nil {
		t.Fatalf("ResetSession() error = %v", err)
	}
	ex := <-result
	if ex.err != nil {
		t.Fatal(ex.err)
	}
	if !bytes.Contains(ex.request, []byte("ALTER SESSION SET TIME_ZONE = '+08:00'")) {
		t.Fatalf("reset request = %q, want timezone reset", ex.request)
	}
	if _, offset := time.Now().In(conn.sessionLocation).Zone(); offset != 8*60*60 {
		t.Fatalf("session location = %v, want +08:00", conn.sessionLocation)
	}
}

func TestPrepareContextSkipsMetadataEOFWhenDeprecated(t *testing.T) {
	client, server := net.Pipe()
	result := make(chan error, 1)
	go func() {
		defer server.Close()
		packets := protocol.NewPacketConn(server)
		if _, err := packets.ReadPacket(); err != nil {
			result <- err
			return
		}
		prepareOK := []byte{protocol.OKPacket, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0}
		if err := packets.WritePacket(prepareOK); err != nil {
			result <- err
			return
		}
		if err := packets.WritePacket(testColumnDefinitionPacket(protocol.ColumnTypeVarString)); err != nil {
			result <- err
			return
		}
		result <- packets.WritePacket(testColumnDefinitionPacket(protocol.ColumnTypeVarString))
	}()

	conn := &Conn{
		netConn:      client,
		packets:      protocol.NewPacketConn(client),
		cfg:          &Config{Timeout: time.Second},
		deprecateEOF: true,
	}
	stmt, err := conn.PrepareContext(context.Background(), "SELECT ?")
	_ = client.Close()
	if err != nil {
		t.Fatalf("PrepareContext() error = %v", err)
	}
	if stmt.NumInput() != 1 {
		t.Fatalf("NumInput() = %d, want 1", stmt.NumInput())
	}
	if err := <-result; err != nil {
		t.Fatalf("server exchange error = %v", err)
	}
}

func TestResetSessionRetiresConnectionWhenTimezoneResetFails(t *testing.T) {
	client, server := net.Pipe()
	result := make(chan error, 1)
	go func() {
		defer server.Close()
		packets := protocol.NewPacketConn(server)
		if _, err := packets.ReadPacket(); err != nil {
			result <- err
			return
		}
		result <- packets.WritePacket([]byte{protocol.ErrPacket, 0xae, 0x03, '#', '4', '2', 'S', '0', '2', 'd', 'e', 'n', 'i', 'e', 'd'})
	}()

	conn := &Conn{
		netConn: client,
		packets: protocol.NewPacketConn(client),
		cfg: &Config{
			Timeout:         time.Second,
			OracleMode:      "true",
			SessionTimeZone: "+08:00",
		},
		tenantMode:      "oracle",
		sessionLocation: time.UTC,
	}
	err := conn.ResetSession(context.Background())
	_ = client.Close()
	if !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("ResetSession() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after timezone reset failure")
	}
	if err := <-result; err != nil {
		t.Fatalf("server exchange error = %v", err)
	}
}
