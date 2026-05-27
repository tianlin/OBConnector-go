package oceanbase

import (
	"bytes"
	"database/sql/driver"
	"io"
	"reflect"
	"testing"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

func TestParseTextRow(t *testing.T) {
	var packet []byte
	packet = protocol.PutLengthEncodedString(packet, "123")
	packet = protocol.PutLengthEncodedString(packet, "3.14")
	packet = append(packet, protocol.NullColumn)
	packet = protocol.PutLengthEncodedString(packet, "hello")

	row, err := parseTextRow(packet, 4, []byte{
		protocol.ColumnTypeLongLong,
		protocol.ColumnTypeDouble,
		protocol.ColumnTypeVarString,
		protocol.ColumnTypeVarString,
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []driver.Value{int64(123), 3.14, nil, "hello"}
	if !reflect.DeepEqual(row, want) {
		t.Fatalf("row = %#v, want %#v", row, want)
	}
}

func TestRowsColumnTypes(t *testing.T) {
	rows := &Rows{
		columns: []string{"id", "payload"},
		types:   []byte{protocol.ColumnTypeLongLong, protocol.ColumnTypeBlob},
		colDefs: []columnDef{
			{name: "id", typ: protocol.ColumnTypeLongLong, flags: notNullFlag},
			{name: "payload", typ: protocol.ColumnTypeBlob, flags: 0},
		},
	}
	if got := rows.ColumnTypeDatabaseTypeName(0); got != "BIGINT" {
		t.Fatalf("type name = %q", got)
	}
	if got := rows.ColumnTypeScanType(0); got != reflect.TypeOf(int64(0)) {
		t.Fatalf("scan type = %v", got)
	}
	if got := rows.ColumnTypeScanType(1); got != reflect.TypeOf([]byte{}) {
		t.Fatalf("blob scan type = %v", got)
	}
}

func TestRowsColumnTypeNullable(t *testing.T) {
	rows := &Rows{
		columns: []string{"id", "payload"},
		types:   []byte{protocol.ColumnTypeLongLong, protocol.ColumnTypeBlob},
		colDefs: []columnDef{
			{name: "id", typ: protocol.ColumnTypeLongLong, flags: notNullFlag},
			{name: "payload", typ: protocol.ColumnTypeBlob, flags: 0},
		},
	}
	nullable, ok := rows.ColumnTypeNullable(0)
	if !ok || nullable {
		t.Fatalf("id should be NOT NULL, got nullable=%v ok=%v", nullable, ok)
	}
	nullable, ok = rows.ColumnTypeNullable(1)
	if !ok || !nullable {
		t.Fatalf("payload should be nullable, got nullable=%v ok=%v", nullable, ok)
	}
}

func TestRowsColumnTypeLength(t *testing.T) {
	rows := &Rows{
		colDefs: []columnDef{
			{name: "id", typ: protocol.ColumnTypeLongLong, columnLength: 20},
		},
	}
	length, ok := rows.ColumnTypeLength(0)
	if !ok || length != 20 {
		t.Fatalf("length = %d, ok = %v, want 20, true", length, ok)
	}
}

func TestRowsColumnTypePrecisionScale(t *testing.T) {
	rows := &Rows{
		colDefs: []columnDef{
			{name: "val", typ: protocol.ColumnTypeNewDecimal, columnLength: 10, decimals: 2},
		},
	}
	precision, scale, ok := rows.ColumnTypePrecisionScale(0)
	if !ok || precision != 10 || scale != 2 {
		t.Fatalf("precision=%d scale=%d ok=%v, want 10, 2, true", precision, scale, ok)
	}
}

func TestStreamingRowsNextAndRelease(t *testing.T) {
	var row []byte
	row = protocol.PutLengthEncodedString(row, "1")
	row = protocol.PutLengthEncodedString(row, "hello")

	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, row)
	writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})

	released := false
	rows := &Rows{
		conn:      &Conn{packets: protocol.NewPacketConn(buf)},
		columns:   []string{"one", "text"},
		types:     []byte{protocol.ColumnTypeLongLong, protocol.ColumnTypeVarString},
		streaming: true,
		release: func() {
			released = true
		},
	}

	dest := make([]driver.Value, 2)
	if err := rows.Next(dest); err != nil {
		t.Fatal(err)
	}
	if want := []driver.Value{int64(1), "hello"}; !reflect.DeepEqual(dest, want) {
		t.Fatalf("dest = %#v, want %#v", dest, want)
	}
	if err := rows.Next(dest); err != io.EOF {
		t.Fatalf("second Next err = %v, want EOF", err)
	}
	if !released {
		t.Fatal("streaming rows did not release connection")
	}
}

func TestStreamingRowsCloseDrainsAndRelease(t *testing.T) {
	var row []byte
	row = protocol.PutLengthEncodedString(row, "1")

	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, row)
	writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})

	released := false
	rows := &Rows{
		conn:      &Conn{packets: protocol.NewPacketConn(buf)},
		columns:   []string{"one"},
		types:     []byte{protocol.ColumnTypeLongLong},
		streaming: true,
		release: func() {
			released = true
		},
	}

	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("streaming rows did not release connection on close")
	}
}

func writePacket(t *testing.T, buf *bytes.Buffer, seq byte, payload []byte) {
	t.Helper()
	if len(payload) > 1<<24-1 {
		t.Fatal("payload too large")
	}
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), seq}
	if _, err := buf.Write(header); err != nil {
		t.Fatal(err)
	}
	if _, err := buf.Write(payload); err != nil {
		t.Fatal(err)
	}
}
