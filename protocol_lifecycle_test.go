package oceanbase

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

func TestReadResultFromFirstPacketRetiresConnectionOnMalformedOK(t *testing.T) {
	conn := &Conn{}

	if _, err := conn.readResultFromFirstPacket([]byte{protocol.OKPacket, 0xff}); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("readResultFromFirstPacket() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after a malformed OK packet")
	}
}

func TestReadResultFromFirstPacketPreservesServerError(t *testing.T) {
	conn := &Conn{}
	packet := []byte{protocol.ErrPacket, 0xae, 0x03, '#', '4', '2', 'S', '0', '2', 'm', 'i', 's', 's', 'i', 'n', 'g'}

	_, err := conn.readResultFromFirstPacket(packet)
	var serverErr *ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("readResultFromFirstPacket() error = %v, want *ServerError", err)
	}
	if conn.bad.Load() {
		t.Fatal("valid server error should not retire the connection")
	}
}

func TestReadQueryResultRetiresConnectionOnMalformedFirstError(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{protocol.ErrPacket})
	conn := &Conn{packets: protocol.NewPacketConn(buf)}

	if _, err := conn.readQueryResult(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("readQueryResult() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after a malformed first error packet")
	}
}

func TestStmtExecLockedRetiresConnectionOnMalformedOK(t *testing.T) {
	conn, serverResult := newStatementResponseConn(t, []byte{protocol.OKPacket, 0xff})
	defer conn.netConn.Close()

	if _, err := conn.stmtExecLocked(context.Background(), 1, nil); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("stmtExecLocked() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after a malformed prepared OK packet")
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("server exchange error = %v", err)
	}
}

func TestStmtBulkExecLockedRetiresConnectionOnMalformedOK(t *testing.T) {
	conn, serverResult := newStatementResponseConn(t, []byte{protocol.OKPacket, 0xff})
	defer conn.netConn.Close()

	argRows := [][]driver.NamedValue{{{Ordinal: 1, Value: int64(1)}}}
	if _, err := conn.stmtBulkExecLocked(context.Background(), 1, argRows); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("stmtBulkExecLocked() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after a malformed bulk OK packet")
	}
	if err := <-serverResult; err != nil {
		t.Fatalf("server exchange error = %v", err)
	}
}

func newStatementResponseConn(t *testing.T, response []byte) (*Conn, <-chan error) {
	t.Helper()
	client, server := net.Pipe()
	result := make(chan error, 1)
	go func() {
		defer server.Close()
		serverPackets := protocol.NewPacketConn(server)
		if _, err := serverPackets.ReadPacket(); err != nil {
			result <- err
			return
		}
		result <- serverPackets.WritePacket(response)
	}()

	return &Conn{
		netConn: client,
		packets: protocol.NewPacketConn(client),
		cfg:     &Config{Timeout: time.Second},
	}, result
}
