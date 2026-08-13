package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const oracleTimestampPayloadLength = 12

// IsOracleTimeType reports whether typ uses OceanBase's Oracle timestamp
// payload rather than the MySQL datetime payload.
func IsOracleTimeType(typ byte) bool {
	switch typ {
	case ColumnTypeOracleTimestampTZ, ColumnTypeOracleTimestampLTZ, ColumnTypeOracleTimestampNano:
		return true
	default:
		return false
	}
}

// ParseOracleTime decodes an OceanBase Oracle timestamp payload and returns a
// UTC time. TIMESTAMP has no timezone and is interpreted as UTC. The local
// timezone variant is interpreted in sessionLocation before it is normalized.
func ParseOracleTime(data []byte, typ byte, sessionLocation *time.Location) (time.Time, error) {
	if !IsOracleTimeType(typ) {
		return time.Time{}, fmt.Errorf("oceanbase: unsupported Oracle time type 0x%02x", typ)
	}
	if len(data) < oracleTimestampPayloadLength {
		return time.Time{}, fmt.Errorf("oceanbase: Oracle time payload has %d bytes, want at least %d: %w", len(data), oracleTimestampPayloadLength, io.ErrUnexpectedEOF)
	}
	if typ != ColumnTypeOracleTimestampTZ && len(data) != oracleTimestampPayloadLength {
		return time.Time{}, fmt.Errorf("oceanbase: Oracle timestamp payload has %d bytes, want %d", len(data), oracleTimestampPayloadLength)
	}

	year := oracleTimestampYear(data[0], data[1])
	month := int(data[2])
	day := int(data[3])
	hour := int(data[4])
	minute := int(data[5])
	second := int(data[6])
	nanosecond := binary.LittleEndian.Uint32(data[7:11])
	scale := data[11]
	if year < 1 || year > 9999 {
		return time.Time{}, fmt.Errorf("oceanbase: invalid Oracle time year %d", year)
	}
	if month < 1 || month > 12 {
		return time.Time{}, fmt.Errorf("oceanbase: invalid Oracle time month %d", month)
	}
	if day < 1 || day > daysInMonth(year, month) {
		return time.Time{}, fmt.Errorf("oceanbase: invalid Oracle time day %d for %04d-%02d", day, year, month)
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59 {
		return time.Time{}, fmt.Errorf("oceanbase: invalid Oracle time clock %02d:%02d:%02d", hour, minute, second)
	}
	if nanosecond >= 1_000_000_000 {
		return time.Time{}, fmt.Errorf("oceanbase: invalid Oracle time nanoseconds %d", nanosecond)
	}
	if scale > 9 {
		return time.Time{}, fmt.Errorf("oceanbase: invalid Oracle time scale %d", scale)
	}

	location := time.UTC
	switch typ {
	case ColumnTypeOracleTimestampTZ:
		offsetMinutes, err := parseOracleTimeZone(data)
		if err != nil {
			return time.Time{}, err
		}
		location = time.FixedZone("", offsetMinutes*60)
	case ColumnTypeOracleTimestampLTZ:
		if sessionLocation == nil {
			return time.Time{}, fmt.Errorf("oceanbase: session time zone is unknown for Oracle timestamp with local time zone")
		}
		location = sessionLocation
	}

	return oracleTimeInLocation(year, time.Month(month), day, hour, minute, second, int(nanosecond), location)
}

func oracleTimeInLocation(year int, month time.Month, day, hour, minute, second, nanosecond int, location *time.Location) (time.Time, error) {
	wall := time.Date(year, month, day, hour, minute, second, nanosecond, time.UTC)
	candidate := time.Date(year, month, day, hour, minute, second, nanosecond, location)
	if !sameOracleWall(candidate, wall) {
		return time.Time{}, fmt.Errorf("oceanbase: Oracle local timestamp %04d-%02d-%02d %02d:%02d:%02d.%09d does not exist in %s", year, month, day, hour, minute, second, nanosecond, location)
	}

	// time.Date chooses one side of a repeated DST wall clock value. Inspect the
	// actual zone interval boundaries around the candidate and verify every
	// possible inverse mapping so that a repeated local value is rejected
	// instead of silently mis-decoded.
	offsets := make(map[int]struct{})
	zonePoints := []time.Time{candidate}
	zoneStart, zoneEnd := candidate.ZoneBounds()
	if !zoneStart.IsZero() {
		zonePoints = append(zonePoints, zoneStart.Add(-time.Nanosecond), zoneStart)
	}
	if !zoneEnd.IsZero() {
		zonePoints = append(zonePoints, zoneEnd.Add(-time.Nanosecond), zoneEnd)
	}
	for _, point := range zonePoints {
		_, offset := point.In(location).Zone()
		offsets[offset] = struct{}{}
	}

	var matches []time.Time
	for offset := range offsets {
		instant := wall.Add(-time.Duration(offset) * time.Second)
		if !sameOracleWall(instant.In(location), wall) {
			continue
		}
		duplicate := false
		for _, match := range matches {
			if match.Equal(instant) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			matches = append(matches, instant)
		}
	}

	switch len(matches) {
	case 0:
		return time.Time{}, fmt.Errorf("oceanbase: Oracle local timestamp %04d-%02d-%02d %02d:%02d:%02d.%09d cannot be resolved in %s", year, month, day, hour, minute, second, nanosecond, location)
	case 1:
		return matches[0].UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("oceanbase: Oracle local timestamp %04d-%02d-%02d %02d:%02d:%02d.%09d is ambiguous in %s", year, month, day, hour, minute, second, nanosecond, location)
	}
}

func sameOracleWall(got, want time.Time) bool {
	return got.Year() == want.Year() &&
		got.Month() == want.Month() &&
		got.Day() == want.Day() &&
		got.Hour() == want.Hour() &&
		got.Minute() == want.Minute() &&
		got.Second() == want.Second() &&
		got.Nanosecond() == want.Nanosecond()
}

func oracleTimestampYear(centuryByte, yearByte byte) int {
	century := int(int8(centuryByte))
	year := int(int8(yearByte))
	sign := 1
	if century < 0 || year < 0 {
		sign = -1
	}
	if century < 0 {
		century = -century
	}
	if year < 0 {
		year = -year
	}
	return sign * (century*100 + year)
}

func daysInMonth(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func parseOracleTimeZone(data []byte) (int, error) {
	if len(data) < 14 {
		return 0, fmt.Errorf("oceanbase: Oracle timestamp with time zone payload has %d bytes, want at least 14: %w", len(data), io.ErrUnexpectedEOF)
	}

	hour := int(int8(data[12]))
	minute := int(int8(data[13]))
	if minute < -59 || minute > 59 {
		return 0, fmt.Errorf("oceanbase: invalid Oracle time zone minute %d", minute)
	}
	if hour != 0 && minute != 0 && (hour < 0) != (minute < 0) {
		return 0, fmt.Errorf("oceanbase: Oracle time zone hour %d and minute %d have mixed signs", hour, minute)
	}
	sign := 1
	if hour < 0 || minute < 0 {
		sign = -1
	}
	offsetMinutes := sign * (absInt(hour)*60 + absInt(minute))
	if offsetMinutes < -(12*60+59) || offsetMinutes > 14*60 {
		return 0, fmt.Errorf("oceanbase: invalid Oracle time zone offset %d minutes", offsetMinutes)
	}

	if len(data) < 15 {
		return 0, fmt.Errorf("oceanbase: truncated Oracle time zone name length: %w", io.ErrUnexpectedEOF)
	}
	nameLength := int(data[14])
	nameEnd := 15 + nameLength
	if len(data) < nameEnd {
		return 0, fmt.Errorf("oceanbase: truncated Oracle time zone name: %w", io.ErrUnexpectedEOF)
	}
	if len(data) < nameEnd+1 {
		return 0, fmt.Errorf("oceanbase: truncated Oracle time zone abbreviation length: %w", io.ErrUnexpectedEOF)
	}
	abbreviationLength := int(data[nameEnd])
	payloadEnd := nameEnd + 1 + abbreviationLength
	if len(data) < payloadEnd {
		return 0, fmt.Errorf("oceanbase: truncated Oracle time zone abbreviation: %w", io.ErrUnexpectedEOF)
	}
	if len(data) != payloadEnd {
		return 0, fmt.Errorf("oceanbase: Oracle time zone payload has %d trailing bytes", len(data)-payloadEnd)
	}
	return offsetMinutes, nil
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}
