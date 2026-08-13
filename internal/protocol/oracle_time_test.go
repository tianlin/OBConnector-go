package protocol

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestParseOracleTime(t *testing.T) {
	base := oracleTimestampPayload()
	withOffset := append(append([]byte(nil), base...), 8, 0, 0, 0)
	plusEight := time.FixedZone("UTC+08:00", 8*60*60)

	tests := []struct {
		name            string
		typ             byte
		data            []byte
		sessionLocation *time.Location
		want            time.Time
	}{
		{
			name:            "timestamp",
			typ:             ColumnTypeOracleTimestampNano,
			data:            base,
			sessionLocation: plusEight,
			want:            time.Date(2026, 8, 12, 14, 45, 44, 873432000, time.UTC),
		},
		{
			name:            "timestamp with time zone",
			typ:             ColumnTypeOracleTimestampTZ,
			data:            withOffset,
			sessionLocation: time.UTC,
			want:            time.Date(2026, 8, 12, 6, 45, 44, 873432000, time.UTC),
		},
		{
			name:            "timestamp with local time zone",
			typ:             ColumnTypeOracleTimestampLTZ,
			sessionLocation: time.UTC,
			data:            base,
			want:            time.Date(2026, 8, 12, 14, 45, 44, 873432000, time.UTC),
		},
		{
			name:            "timestamp with local time zone converts session time",
			typ:             ColumnTypeOracleTimestampLTZ,
			sessionLocation: plusEight,
			data:            base,
			want:            time.Date(2026, 8, 12, 6, 45, 44, 873432000, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseOracleTime(tt.data, tt.typ, tt.sessionLocation)
			if err != nil {
				t.Fatalf("ParseOracleTime() error = %v", err)
			}
			if !got.Equal(tt.want) || got.Location() != time.UTC {
				t.Fatalf("ParseOracleTime() = %v (%s), want %v (UTC)", got, got.Location(), tt.want)
			}
		})
	}
}

func TestParseOracleTimeRejectsInvalidPayload(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		typ  byte
	}{
		{name: "short", data: nil, typ: ColumnTypeOracleTimestampNano},
		{name: "invalid month", data: mutateOracleTimestampPayload(func(data []byte) { data[2] = 13 }), typ: ColumnTypeOracleTimestampNano},
		{name: "invalid nanoseconds", data: mutateOracleTimestampPayload(func(data []byte) { binary.LittleEndian.PutUint32(data[7:11], 1_000_000_000) }), typ: ColumnTypeOracleTimestampNano},
		{name: "invalid scale", data: mutateOracleTimestampPayload(func(data []byte) { data[11] = 10 }), typ: ColumnTypeOracleTimestampNano},
		{name: "timestamp trailing bytes", data: append(oracleTimestampPayload(), 0xff), typ: ColumnTypeOracleTimestampNano},
		{name: "timezone offset too large", data: append(oracleTimestampPayload(), 24, 0), typ: ColumnTypeOracleTimestampTZ},
		{name: "timezone offset past positive limit", data: append(oracleTimestampPayload(), 14, 1), typ: ColumnTypeOracleTimestampTZ},
		{name: "timezone offset past negative limit", data: append(oracleTimestampPayload(), 0xf3, 0), typ: ColumnTypeOracleTimestampTZ},
		{name: "timezone offset has mixed signs", data: append(oracleTimestampPayload(), 8, 0xe2), typ: ColumnTypeOracleTimestampTZ},
		{name: "missing timezone name length", data: append(oracleTimestampPayload(), 8, 0), typ: ColumnTypeOracleTimestampTZ},
		{name: "missing timezone abbreviation length", data: append(oracleTimestampPayload(), 8, 0, 0), typ: ColumnTypeOracleTimestampTZ},
		{name: "truncated timezone name", data: append(oracleTimestampPayload(), 8, 0, 2), typ: ColumnTypeOracleTimestampTZ},
		{name: "timezone trailing bytes", data: append(oracleTimestampPayload(), 8, 0, 1, 'X', 0, 0xff), typ: ColumnTypeOracleTimestampTZ},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseOracleTime(tt.data, tt.typ, time.UTC); err == nil {
				t.Fatal("ParseOracleTime() error = nil, want error")
			}
		})
	}
}

func TestIsOracleTimeType(t *testing.T) {
	for _, typ := range []byte{
		ColumnTypeOracleTimestampTZ,
		ColumnTypeOracleTimestampLTZ,
		ColumnTypeOracleTimestampNano,
	} {
		if !IsOracleTimeType(typ) {
			t.Errorf("IsOracleTimeType(0x%02x) = false, want true", typ)
		}
	}
	if IsOracleTimeType(ColumnTypeDateTime) {
		t.Error("IsOracleTimeType(DATETIME) = true, want false")
	}
}

func oracleTimestampPayload() []byte {
	return []byte{0x14, 0x1a, 0x08, 0x0c, 0x0e, 0x2d, 0x2c, 0xc0, 0x83, 0x0f, 0x34, 0x06}
}

func mutateOracleTimestampPayload(mutator func([]byte)) []byte {
	data := oracleTimestampPayload()
	mutator(data)
	return data
}

func TestParseOracleTimeUsesExpectedPayloadFixture(t *testing.T) {
	want := time.Date(2026, 8, 12, 14, 45, 44, 873432000, time.UTC)
	got, err := ParseOracleTime(oracleTimestampPayload(), ColumnTypeOracleTimestampNano, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestParseOracleTimeRejectsAmbiguousSessionLocalTime(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	data := oracleTimestampPayloadForDate(2024, 11, 3, 1, 30, 0)
	if _, err := ParseOracleTime(data, ColumnTypeOracleTimestampLTZ, location); err == nil {
		t.Fatal("ParseOracleTime() error = nil for an ambiguous DST local time")
	}
}

func TestParseOracleTimeRejectsNonexistentSessionLocalTime(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	data := oracleTimestampPayloadForDate(2024, 3, 10, 2, 30, 0)
	if _, err := ParseOracleTime(data, ColumnTypeOracleTimestampLTZ, location); err == nil {
		t.Fatal("ParseOracleTime() error = nil for a nonexistent DST local time")
	}
}

func TestParseOracleTimeResolvesUnambiguousSessionRegion(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	data := oracleTimestampPayloadForDate(2024, 7, 1, 12, 0, 0)
	got, err := ParseOracleTime(data, ColumnTypeOracleTimestampLTZ, location)
	if err != nil {
		t.Fatalf("ParseOracleTime() error = %v", err)
	}
	want := time.Date(2024, 7, 1, 16, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("ParseOracleTime() = %v, want %v", got, want)
	}
}

func TestParseOracleTimeRejectsUnknownSessionRegion(t *testing.T) {
	if _, err := ParseOracleTime(oracleTimestampPayload(), ColumnTypeOracleTimestampLTZ, nil); err == nil {
		t.Fatal("ParseOracleTime() error = nil with unknown session time zone")
	}
}

func oracleTimestampPayloadForDate(year, month, day, hour, minute, second int) []byte {
	return []byte{byte(year / 100), byte(year % 100), byte(month), byte(day), byte(hour), byte(minute), byte(second), 0, 0, 0, 0, 9}
}
