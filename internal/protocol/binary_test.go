package protocol

import (
	"bytes"
	"math"
	"testing"
	"time"
)

func TestGetBinaryParamType(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  byte
	}{
		{"nil", nil, ColumnTypeNull},
		{"int64", int64(42), ColumnTypeLongLong},
		{"float64", float64(3.14), ColumnTypeDouble},
		{"string", "hello", ColumnTypeVarString},
		{"[]byte", []byte("hello"), ColumnTypeVarString},
		{"time.Time", time.Now(), ColumnTypeDateTime},
		{"bool_true", true, ColumnTypeTiny},
		{"bool_false", false, ColumnTypeTiny},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetBinaryParamType(tt.value)
			if got != tt.want {
				t.Errorf("got 0x%02x, want 0x%02x", got, tt.want)
			}
		})
	}
}

func TestAppendBinaryParam(t *testing.T) {
	tests := []struct {
		name  string
		typ   byte
		value any
		check func(t *testing.T, data []byte)
	}{
		{
			"nil", ColumnTypeNull, nil,
			func(t *testing.T, data []byte) {
				if len(data) != 0 {
					t.Fatalf("expected empty, got %v", data)
				}
			},
		},
		{
			"int64", ColumnTypeLongLong, int64(42),
			func(t *testing.T, data []byte) {
				if len(data) != 8 {
					t.Fatalf("expected 8 bytes, got %d", len(data))
				}
				if data[0] != 42 {
					t.Fatalf("expected 42, got %d", data[0])
				}
			},
		},
		{
			"float64", ColumnTypeDouble, float64(3.14),
			func(t *testing.T, data []byte) {
				if len(data) != 8 {
					t.Fatalf("expected 8 bytes, got %d", len(data))
				}
				got := math.Float64frombits(binaryLittleEndianUint64(data))
				if math.Abs(got-3.14) > 0.001 {
					t.Fatalf("expected ~3.14, got %f", got)
				}
			},
		},
		{
			"string", ColumnTypeVarString, "hello",
			func(t *testing.T, data []byte) {
				if data[0] != 0x05 {
					t.Fatalf("expected length 5, got %d", data[0])
				}
				if string(data[1:]) != "hello" {
					t.Fatalf("expected 'hello', got %q", string(data[1:]))
				}
			},
		},
		{
			"bytes", ColumnTypeVarString, []byte("world"),
			func(t *testing.T, data []byte) {
				if string(data[1:]) != "world" {
					t.Fatalf("expected 'world', got %q", string(data[1:]))
				}
			},
		},
		{
			"bool_true", ColumnTypeTiny, true,
			func(t *testing.T, data []byte) {
				if len(data) != 1 || data[0] != 1 {
					t.Fatalf("expected [1], got %v", data)
				}
			},
		},
		{
			"bool_false", ColumnTypeTiny, false,
			func(t *testing.T, data []byte) {
				if len(data) != 1 || data[0] != 0 {
					t.Fatalf("expected [0], got %v", data)
				}
			},
		},
		{
			"time_zero", ColumnTypeDateTime, time.Time{},
			func(t *testing.T, data []byte) {
				if len(data) != 1 || data[0] != 0 {
					t.Fatalf("zero time should encode as [0], got %v", data)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := AppendBinaryParam(nil, tt.typ, tt.value)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.check(t, data)
		})
	}

	// Unknown type should error
	_, err := AppendBinaryParam(nil, ColumnTypeLong, complex(1, 2))
	if err == nil {
		t.Fatal("expected error for unsupported type")
	}
}

func TestAppendBinaryTime(t *testing.T) {
	tests := []struct {
		name string
		time time.Time
		size int // expected encoded size
	}{
		{"zero", time.Time{}, 1},
		{"date_only", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), 5},
		{"with_time", time.Date(2024, 1, 15, 10, 30, 45, 0, time.UTC), 8},
		{"with_nanos", time.Date(2024, 1, 15, 10, 30, 45, 123456000, time.UTC), 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := AppendBinaryTime(nil, tt.time)
			if len(data) != tt.size {
				t.Fatalf("len = %d, want %d", len(data), tt.size)
			}
			if tt.time.IsZero() && data[0] != 0 {
				t.Fatalf("zero time: expected [0], got %v", data)
			}
		})
	}
}

func TestParseBinaryTime(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		check func(t *testing.T, val any)
		err   bool
	}{
		{
			"zero", []byte{0x00},
			func(t *testing.T, val any) {
				v, ok := val.(time.Time)
				if !ok || !v.IsZero() {
					t.Fatalf("expected zero time, got %v", val)
				}
			},
			false,
		},
		{
			"date_only", []byte{0x04, 0xE8, 0x07, 0x01, 0x0F},
			func(t *testing.T, val any) {
				v := val.(time.Time)
				if v.Year() != 2024 || v.Month() != 1 || v.Day() != 15 {
					t.Fatalf("expected 2024-01-15, got %v", v)
				}
			},
			false,
		},
		{
			"truncated", []byte{0x04, 0xE8},
			nil, true,
		},
		{"empty", nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, _, err := ParseBinaryTime(tt.data)
			if tt.err && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.check != nil {
				tt.check(t, val)
			}
		})
	}
}

func TestParseBinaryValue(t *testing.T) {
	tests := []struct {
		name string
		typ  byte
		data []byte
		want any
		err  bool
	}{
		{"tiny_signed", ColumnTypeTiny, []byte{0xFE}, int64(-2), false},
		{"tiny_min", ColumnTypeTiny, []byte{0x80}, int64(-128), false},
		{"short", ColumnTypeShort, []byte{0xFF, 0x7F}, int64(32767), false},
		{"year", ColumnTypeYear, []byte{0xE8, 0x07}, int64(2024), false},
		{"long", ColumnTypeLong, []byte{0xEF, 0xBE, 0xAD, 0xDE}, int64(-559038737), false},
		{"int24", ColumnTypeInt24, []byte{0xFF, 0xFF, 0xFF, 0x7F}, int64(2147483647), false},
		{"longlong", ColumnTypeLongLong, []byte{0x2A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, int64(42), false},
		{"float", ColumnTypeFloat, []byte{0x00, 0x00, 0x48, 0x42}, float64(50.0), false},
		{"double", ColumnTypeDouble, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40}, float64(10.0), false},
		{"string", ColumnTypeVarString, []byte{0x03, 'a', 'b', 'c'}, "abc", false},
		{"truncated_tiny", ColumnTypeTiny, nil, nil, true},
		{"unsupported", 0xFF, nil, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, _, err := ParseBinaryValue(tt.data, tt.typ)
			if tt.err && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if val != tt.want {
				t.Errorf("got %v (type %T), want %v (type %T)", val, val, tt.want, tt.want)
			}
		})
	}
}

func TestParseBinaryValueOracleTimestamp(t *testing.T) {
	raw := oracleTimestampPayload()
	data := PutLengthEncodedString(nil, string(raw))

	got, used, err := ParseBinaryValue(data, ColumnTypeOracleTimestampNano)
	if err != nil {
		t.Fatalf("ParseBinaryValue() error = %v", err)
	}
	want := time.Date(2026, 8, 12, 14, 45, 44, 873432000, time.UTC)
	if got != want {
		t.Fatalf("value = %#v, want %#v", got, want)
	}
	if used != len(data) {
		t.Fatalf("used = %d, want %d", used, len(data))
	}
}

func TestParseBinaryRowRejectsTrailingBytes(t *testing.T) {
	packet := []byte{0x00, 0x00, 0x01, 0x7f}
	if _, err := ParseBinaryRow(packet, 1, []byte{ColumnTypeTiny}); err == nil {
		t.Fatal("ParseBinaryRow() error = nil, want trailing-byte error")
	}
}

func TestParseBinaryRowOracleLocalTimestampUsesSessionLocation(t *testing.T) {
	raw := PutLengthEncodedString(nil, string(oracleTimestampPayload()))
	packet := append([]byte{0x00, 0x00}, raw...)

	row, err := ParseBinaryRowInLocation(packet, 1, []byte{ColumnTypeOracleTimestampLTZ}, time.FixedZone("UTC+08:00", 8*60*60))
	if err != nil {
		t.Fatalf("ParseBinaryRowInLocation() error = %v", err)
	}
	want := time.Date(2026, 8, 12, 6, 45, 44, 873432000, time.UTC)
	if row[0] != want {
		t.Fatalf("row[0] = %#v, want %#v", row[0], want)
	}
}

func TestParseBinaryRow(t *testing.T) {
	// Build a row with 3 columns: TINY(1), VARCHAR("hi"), NULL
	// Binary row format: header(0x00) + null-bitmap + values
	// null-bitmap: ceil((3+2)/8) = 1 byte, bit (2+2)%8=4 set for col2
	types := []byte{ColumnTypeTiny, ColumnTypeVarString, ColumnTypeTiny}
	raw := []byte{0x00, 0x10, 0x2A}
	raw = append(raw, 0x02, 'h', 'i')

	row, err := ParseBinaryRow(raw, 3, types)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(row) != 3 {
		t.Fatalf("len=%d, want 3", len(row))
	}
	if row[0] != int64(42) {
		t.Errorf("col0 = %v, want 42", row[0])
	}
	if row[1] != "hi" {
		t.Errorf("col1 = %v, want 'hi'", row[1])
	}
	if row[2] != nil {
		t.Errorf("col2 = %v, want nil", row[2])
	}
}

func TestParseBinaryRowErrors(t *testing.T) {
	// Empty
	_, err := ParseBinaryRow(nil, 1, []byte{ColumnTypeTiny})
	if err == nil {
		t.Fatal("expected error for nil packet")
	}

	// Wrong header byte
	_, err = ParseBinaryRow([]byte{0x01}, 1, []byte{ColumnTypeTiny})
	if err == nil {
		t.Fatal("expected error for non-0x00 header")
	}

	// Truncated (need at least 1 header + null-bitmap bytes)
	_, err = ParseBinaryRow([]byte{0x00}, 10, []byte{ColumnTypeTiny})
	if err == nil {
		t.Fatal("expected error for truncated packet")
	}
}

// Helper for binary tests
func binaryLittleEndianUint64(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
}

func TestAppendBinaryParamRoundtrip(t *testing.T) {
	// String roundtrip
	orig := "hello world"
	buf, err := AppendBinaryParam(nil, ColumnTypeVarString, orig)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, _, err := ReadLengthEncodedString(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(decoded) != orig {
		t.Fatalf("roundtrip: got %q, want %q", string(decoded), orig)
	}

	// []byte roundtrip
	buf, err = AppendBinaryParam(nil, ColumnTypeVarString, []byte("bytes"))
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, _, err = ReadLengthEncodedString(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, []byte("bytes")) {
		t.Fatalf("roundtrip: got %v, want %v", decoded, []byte("bytes"))
	}
}
