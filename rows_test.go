package oceanbase

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

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

func TestParseTextRowOracleTimes(t *testing.T) {
	base := oracleTimestampPayloadForRows()
	withOffset := append(append([]byte(nil), base...), 8, 0, 0, 0)
	packet := protocol.PutLengthEncodedString(nil, string(base))
	packet = append(packet, protocol.PutLengthEncodedString(nil, string(withOffset))...)
	packet = append(packet, protocol.PutLengthEncodedString(nil, string(base))...)

	row, err := parseTextRowInLocation(packet, 3, []byte{
		protocol.ColumnTypeOracleTimestampNano,
		protocol.ColumnTypeOracleTimestampTZ,
		protocol.ColumnTypeOracleTimestampLTZ,
	}, time.FixedZone("UTC+08:00", 8*60*60))
	if err != nil {
		t.Fatalf("parseTextRowInLocation() error = %v", err)
	}
	want := []driver.Value{
		time.Date(2026, 8, 12, 14, 45, 44, 873432000, time.UTC),
		time.Date(2026, 8, 12, 6, 45, 44, 873432000, time.UTC),
		time.Date(2026, 8, 12, 6, 45, 44, 873432000, time.UTC),
	}
	if !reflect.DeepEqual(row, want) {
		t.Fatalf("row = %#v, want %#v", row, want)
	}
}

func TestParseTextRowRejectsTrailingBytes(t *testing.T) {
	packet := protocol.PutLengthEncodedString(nil, "value")
	packet = append(packet, 0xff)

	if _, err := parseTextRow(packet, 1, []byte{protocol.ColumnTypeVarString}); err == nil {
		t.Fatal("parseTextRow() error = nil, want trailing-byte error")
	}
}

func TestStreamingRowsNextOracleTimestamp(t *testing.T) {
	base := oracleTimestampPayloadForRows()
	tests := []struct {
		name   string
		binary bool
		packet []byte
	}{
		{
			name:   "text result",
			packet: protocol.PutLengthEncodedString(nil, string(base)),
		},
		{
			name:   "text local timestamp result",
			packet: protocol.PutLengthEncodedString(nil, string(base)),
		},
		{
			name:   "binary result",
			binary: true,
			packet: append([]byte{0x00, 0x00}, protocol.PutLengthEncodedString(nil, string(base))...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			writePacket(t, buf, 0, tt.packet)
			writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
			rows := &Rows{
				conn:            &Conn{packets: protocol.NewPacketConn(buf)},
				columns:         []string{"event_time"},
				types:           []byte{protocol.ColumnTypeOracleTimestampNano},
				binary:          tt.binary,
				streaming:       true,
				sessionLocation: time.FixedZone("UTC+08:00", 8*60*60),
			}
			if tt.name == "text local timestamp result" {
				rows.types[0] = protocol.ColumnTypeOracleTimestampLTZ
			}
			want := time.Date(2026, 8, 12, 6, 45, 44, 873432000, time.UTC)
			if tt.name != "text local timestamp result" {
				want = time.Date(2026, 8, 12, 14, 45, 44, 873432000, time.UTC)
			}
			got := make([]driver.Value, 1)
			if err := rows.Next(got); err != nil {
				t.Fatal(err)
			}
			if got[0] != want {
				t.Fatalf("value = %#v, want %#v", got[0], want)
			}
		})
	}
}

func TestStreamingRowsBinaryOracleTimestampWithOKTerminator(t *testing.T) {
	base := oracleTimestampPayloadForRows()
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, append([]byte{0x00, 0x00}, protocol.PutLengthEncodedString(nil, string(base))...))
	writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})
	rows := &Rows{
		conn:        &Conn{packets: protocol.NewPacketConn(buf)},
		columns:     []string{"event_time"},
		types:       []byte{protocol.ColumnTypeOracleTimestampNano},
		binary:      true,
		streaming:   true,
		resultSetOK: true,
	}

	got := make([]driver.Value, 1)
	if err := rows.Next(got); err != nil {
		t.Fatal(err)
	}
	if err := rows.Next(got); err != io.EOF {
		t.Fatalf("terminator error = %v, want EOF", err)
	}
}

func TestStreamingRowsBinaryResultSetOKTerminatorDoesNotBecomeDataRow(t *testing.T) {
	okPacket := []byte{protocol.EOFPacket, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}
	for _, typ := range []byte{protocol.ColumnTypeTiny, protocol.ColumnTypeVarString} {
		t.Run(databaseTypeName(typ), func(t *testing.T) {
			buf := &bytes.Buffer{}
			writePacket(t, buf, 0, okPacket)
			rows := &Rows{
				conn:        &Conn{packets: protocol.NewPacketConn(buf)},
				columns:     []string{"value"},
				types:       []byte{typ},
				binary:      true,
				streaming:   true,
				resultSetOK: true,
			}
			if err := rows.Next(make([]driver.Value, 1)); err != io.EOF {
				t.Fatalf("Next() error = %v, want EOF", err)
			}
		})
	}
}

func TestStreamingRowsBinaryResultSetOKDoesNotConsumeValidDataRow(t *testing.T) {
	// A binary row starts with 0x00, followed by the null bitmap and typed
	// values. This packet is a valid five-column TINYINT row, even though its
	// bytes also look like the old 0x00 OK terminator shape.
	packet := []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, packet)
	rows := &Rows{
		conn:        &Conn{packets: protocol.NewPacketConn(buf)},
		columns:     []string{"a", "b", "c", "d", "e"},
		types:       []byte{protocol.ColumnTypeTiny, protocol.ColumnTypeTiny, protocol.ColumnTypeTiny, protocol.ColumnTypeTiny, protocol.ColumnTypeTiny},
		binary:      true,
		streaming:   true,
		resultSetOK: true,
	}

	got := make([]driver.Value, 5)
	if err := rows.Next(got); err != nil {
		t.Fatalf("Next() error = %v, want data row", err)
	}
	want := []driver.Value{int64(0), int64(2), int64(0), int64(0), int64(0)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("row = %#v, want %#v", got, want)
	}
}

func TestIsEOFOrOKRecognizesResultSetOKWithEOFMarker(t *testing.T) {
	packet := []byte{protocol.EOFPacket, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}
	if !isEOFOrOK(packet) {
		t.Fatal("isEOFOrOK() = false for result-set OK packet with 0xfe marker")
	}
}

func TestIsEOFOrOKRecognizesEOFWithoutProtocol41(t *testing.T) {
	packet := []byte{protocol.EOFPacket}
	if !isEOFOrOK(packet) {
		t.Fatal("isEOFOrOK() = false for a one-byte EOF packet")
	}
	if resultSetTerminatorHasMoreResults(packet) {
		t.Fatal("one-byte EOF packet should not report more results")
	}
}

func TestResultSetTerminatorHasMoreResultsRecognizesEOFAndOK(t *testing.T) {
	packets := [][]byte{
		{protocol.EOFPacket, 0x00, 0x00, 0x08, 0x00},
		{protocol.EOFPacket, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00},
	}
	for _, packet := range packets {
		if !resultSetTerminatorHasMoreResults(packet) {
			t.Errorf("resultSetTerminatorHasMoreResults(%x) = false, want true", packet)
		}
	}
}

func TestStreamingRowsNextOracleTimestampRejectsInvalidPayload(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, protocol.PutLengthEncodedString(nil, "short"))
	writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
	rows := &Rows{
		conn:      &Conn{packets: protocol.NewPacketConn(buf)},
		columns:   []string{"event_time"},
		types:     []byte{protocol.ColumnTypeOracleTimestampNano},
		streaming: true,
	}
	if err := rows.Next(make([]driver.Value, 1)); err == nil {
		t.Fatal("Next() error = nil, want invalid Oracle timestamp error")
	}
	if !rows.conn.bad.Load() {
		t.Fatal("connection should be retired after a text row decode error")
	}
	if err := rows.conn.checkUsableLocked(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("connection usability error = %v, want driver.ErrBadConn", err)
	}
}

func TestStreamingRowsNextBinaryOracleTimestampRejectsInvalidPayload(t *testing.T) {
	buf := &bytes.Buffer{}
	packet := append([]byte{0x00, 0x00}, protocol.PutLengthEncodedString(nil, "short")...)
	writePacket(t, buf, 0, packet)
	rows := &Rows{
		conn:      &Conn{packets: protocol.NewPacketConn(buf)},
		columns:   []string{"event_time"},
		types:     []byte{protocol.ColumnTypeOracleTimestampNano},
		binary:    true,
		streaming: true,
	}
	if err := rows.Next(make([]driver.Value, 1)); err == nil {
		t.Fatal("Next() error = nil, want invalid Oracle timestamp error")
	}
	if !rows.conn.bad.Load() {
		t.Fatal("connection should be retired after a binary row decode error")
	}
}

func TestReadQueryResultAfterColumnCountDrainsMoreResults(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{0x00})
	writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
	writePacket(t, buf, 2, protocol.PutLengthEncodedString(nil, "first"))
	writePacket(t, buf, 3, []byte{protocol.EOFPacket, 0x00, 0x00, 0x08, 0x00})
	writePacket(t, buf, 4, []byte{0x01})
	writePacket(t, buf, 5, []byte{0x00})
	writePacket(t, buf, 6, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
	writePacket(t, buf, 7, protocol.PutLengthEncodedString(nil, "second"))
	writePacket(t, buf, 8, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})

	conn := &Conn{packets: protocol.NewPacketConn(buf)}
	if _, err := conn.readQueryResultAfterColumnCount([]byte{0x01}); err != nil {
		t.Fatalf("readQueryResultAfterColumnCount() error = %v", err)
	}
	if _, err := conn.packets.ReadPacket(); !errors.Is(err, io.EOF) {
		t.Fatalf("remaining packet error = %v, want EOF after all result sets are drained", err)
	}
}

func TestReadQueryResultSkipsMetadataEOFWhenDeprecated(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{0x01})
	writePacket(t, buf, 1, testColumnDefinitionPacket(protocol.ColumnTypeVarString))
	writePacket(t, buf, 2, protocol.PutLengthEncodedString(nil, "first"))
	writePacket(t, buf, 3, resultSetOKPacket(0))

	conn := &Conn{packets: protocol.NewPacketConn(buf), deprecateEOF: true}
	rowsValue, err := conn.readQueryResult()
	if err != nil {
		t.Fatalf("readQueryResult() error = %v", err)
	}
	rows := rowsValue.(*Rows)
	got := make([]driver.Value, 1)
	if err := rows.Next(got); err != nil {
		t.Fatalf("first row error = %v", err)
	}
	if got[0] != "first" {
		t.Fatalf("first row = %#v, want %q", got[0], "first")
	}
	if err := rows.Next(got); err != io.EOF {
		t.Fatalf("terminator error = %v, want EOF", err)
	}
}

func TestReadQueryResultAfterColumnCountSkipsMetadataEOFWhenDeprecated(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, testColumnDefinitionPacket(protocol.ColumnTypeVarString))
	writePacket(t, buf, 1, protocol.PutLengthEncodedString(nil, "first"))
	writePacket(t, buf, 2, resultSetOKPacket(0))

	conn := &Conn{packets: protocol.NewPacketConn(buf), deprecateEOF: true}
	if _, err := conn.readQueryResultAfterColumnCount([]byte{0x01}); err != nil {
		t.Fatalf("readQueryResultAfterColumnCount() error = %v", err)
	}
	if _, err := conn.packets.ReadPacket(); !errors.Is(err, io.EOF) {
		t.Fatalf("remaining packet error = %v, want EOF after modern result set", err)
	}
}

func TestDrainRemainingResultsDrainsResultSetRows(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{0x01})
	writePacket(t, buf, 1, []byte{0x00})
	writePacket(t, buf, 2, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
	writePacket(t, buf, 3, protocol.PutLengthEncodedString(nil, "row"))
	writePacket(t, buf, 4, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})

	conn := &Conn{packets: protocol.NewPacketConn(buf)}
	if err := conn.drainRemainingResults(); err != nil {
		t.Fatalf("drainRemainingResults() error = %v", err)
	}
	if _, err := conn.packets.ReadPacket(); !errors.Is(err, io.EOF) {
		t.Fatalf("remaining packet error = %v, want EOF after result-set rows are drained", err)
	}
}

func TestStreamingRowsCloseDrainsMoreResults(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, protocol.PutLengthEncodedString(nil, "current"))
	writePacket(t, buf, 1, []byte{protocol.EOFPacket, 0x00, 0x00, 0x08, 0x00})
	writePacket(t, buf, 2, []byte{0x01})
	writePacket(t, buf, 3, []byte{0x00})
	writePacket(t, buf, 4, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
	writePacket(t, buf, 5, protocol.PutLengthEncodedString(nil, "next"))
	writePacket(t, buf, 6, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})

	rows := &Rows{
		conn:      &Conn{packets: protocol.NewPacketConn(buf)},
		columns:   []string{"value"},
		types:     []byte{protocol.ColumnTypeVarString},
		streaming: true,
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("Rows.Close() error = %v", err)
	}
	if _, err := rows.conn.packets.ReadPacket(); !errors.Is(err, io.EOF) {
		t.Fatalf("remaining packet error = %v, want EOF after all result sets are drained", err)
	}
}

func TestReadQueryResultDrainsMoreResultsAfterOK(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{protocol.OKPacket, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00})
	writePacket(t, buf, 1, []byte{0x01})
	writePacket(t, buf, 2, testColumnDefinitionPacket(protocol.ColumnTypeVarString))
	writePacket(t, buf, 3, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})
	writePacket(t, buf, 4, protocol.PutLengthEncodedString(nil, "next"))
	writePacket(t, buf, 5, []byte{protocol.EOFPacket, 0x00, 0x00, 0x00, 0x00})

	conn := &Conn{packets: protocol.NewPacketConn(buf)}
	if _, err := conn.readQueryResult(); err != nil {
		t.Fatalf("readQueryResult() error = %v", err)
	}
	if _, err := conn.packets.ReadPacket(); !errors.Is(err, io.EOF) {
		t.Fatalf("remaining packet error = %v, want EOF after an OK result with MORE_RESULTS", err)
	}
}

func TestReadQueryResultRetiresConnectionOnMalformedErrorAfterOK(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{protocol.OKPacket, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00})
	writePacket(t, buf, 1, []byte{protocol.ErrPacket})

	conn := &Conn{packets: protocol.NewPacketConn(buf)}
	if _, err := conn.readQueryResult(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("readQueryResult() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after a malformed result error packet")
	}
}

func TestReadQueryResultRetiresConnectionOnMalformedTerminator(t *testing.T) {
	buf := &bytes.Buffer{}
	writePacket(t, buf, 0, []byte{0x01})
	writePacket(t, buf, 1, testColumnDefinitionPacket(protocol.ColumnTypeVarString))
	writePacket(t, buf, 2, []byte{protocol.EOFPacket, 0x00})

	conn := &Conn{packets: protocol.NewPacketConn(buf)}
	if _, err := conn.readQueryResult(); !errors.Is(err, driver.ErrBadConn) {
		t.Fatalf("readQueryResult() error = %v, want driver.ErrBadConn", err)
	}
	if !conn.bad.Load() {
		t.Fatal("connection should be retired after a malformed result-set terminator")
	}
}

func oracleTimestampPayloadForRows() []byte {
	return []byte{0x14, 0x1a, 0x08, 0x0c, 0x0e, 0x2d, 0x2c, 0xc0, 0x83, 0x0f, 0x34, 0x06}
}

func resultSetOKPacket(status uint16) []byte {
	return []byte{protocol.EOFPacket, 0x00, 0x00, byte(status), byte(status >> 8), 0x00, 0x00}
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

func TestRowsColumnTypesOracleTimes(t *testing.T) {
	rows := &Rows{
		columns: []string{"ts", "tstz", "tsltz"},
		types: []byte{
			protocol.ColumnTypeOracleTimestampNano,
			protocol.ColumnTypeOracleTimestampTZ,
			protocol.ColumnTypeOracleTimestampLTZ,
		},
	}
	wantNames := []string{"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"}
	for i, want := range wantNames {
		if got := rows.ColumnTypeDatabaseTypeName(i); got != want {
			t.Errorf("type name[%d] = %q, want %q", i, got, want)
		}
		if got := rows.ColumnTypeScanType(i); got != reflect.TypeOf(time.Time{}) {
			t.Errorf("scan type[%d] = %v, want time.Time", i, got)
		}
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

func testColumnDefinitionPacket(typ byte) []byte {
	var packet []byte
	for _, value := range []string{"def", "", "", "", "value", ""} {
		packet = protocol.PutLengthEncodedString(packet, value)
	}
	packet = append(packet,
		0x0c,
		0x2d, 0x00,
		0x10, 0x00, 0x00, 0x00,
		typ,
		0x00, 0x00,
		0x00,
		0x00, 0x00,
	)
	return packet
}
