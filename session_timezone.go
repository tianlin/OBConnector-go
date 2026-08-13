package oceanbase

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func normalizeOracleMode(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return "auto", nil
	case "true", "oracle":
		return "true", nil
	case "false", "mysql":
		return "false", nil
	default:
		return "", fmt.Errorf("invalid oracle mode %q: want true, false, or auto", value)
	}
}

func isTwoDigits(value string) bool {
	return len(value) == 2 && value[0] >= '0' && value[0] <= '9' && value[1] >= '0' && value[1] <= '9'
}

func parseSessionTimeZone(value string) (*time.Location, string, error) {
	value = strings.TrimSpace(value)
	if strings.EqualFold(value, "utc") || strings.EqualFold(value, "z") {
		return time.UTC, "+00:00", nil
	}
	if len(value) == 6 && (value[0] == '+' || value[0] == '-') && value[3] == ':' {
		if !isTwoDigits(value[1:3]) {
			return nil, "", fmt.Errorf("invalid hour in %q", value)
		}
		hour, err := strconv.Atoi(value[1:3])
		if err != nil {
			return nil, "", fmt.Errorf("invalid hour in %q", value)
		}
		if !isTwoDigits(value[4:6]) {
			return nil, "", fmt.Errorf("invalid minute in %q", value)
		}
		minute, err := strconv.Atoi(value[4:6])
		if err != nil {
			return nil, "", fmt.Errorf("invalid minute in %q", value)
		}
		if minute > 59 {
			return nil, "", fmt.Errorf("invalid minute in %q", value)
		}
		if value[0] == '+' {
			if hour > 13 || (hour == 13 && minute != 0) {
				return nil, "", fmt.Errorf("session time zone %q is outside OceanBase range", value)
			}
		} else if hour > 12 {
			return nil, "", fmt.Errorf("session time zone %q is outside OceanBase range", value)
		}
		offset := hour*60*60 + minute*60
		if value[0] == '-' {
			offset = -offset
		}
		canonical := fmt.Sprintf("%c%02d:%02d", value[0], hour, minute)
		return time.FixedZone("UTC"+canonical, offset), canonical, nil
	}

	if value == "Local" || strings.ContainsAny(value, "'\\\";\r\n") || strings.Contains(value, "..") {
		return nil, "", fmt.Errorf("invalid session time zone region %q", value)
	}
	return nil, "", fmt.Errorf("expected UTC or a fixed offset ±HH:MM; IANA regions are unsupported for deterministic Oracle timestamp decoding, got %q", value)
}

func resolveSessionTimeZone(tenantMode, value string) (*time.Location, string, bool, error) {
	if tenantMode != "oracle" {
		return time.UTC, "", false, nil
	}
	if strings.TrimSpace(value) == "" {
		value = "UTC"
	}
	location, canonical, err := parseSessionTimeZone(value)
	if err != nil {
		return nil, "", false, err
	}
	return location, canonical, true, nil
}

func sessionTimeZoneSQL(canonical string) string {
	return "ALTER SESSION SET TIME_ZONE = '" + canonical + "'"
}

func sessionTimeZoneValueFromQuery(query string) (string, bool) {
	value, _, ok := sessionTimeZoneAssignmentFromQuery(query)
	return value, ok
}

func sessionTimeZoneAssignmentFromQuery(query string) (value string, parameter bool, ok bool) {
	query = stripSQLComments(query)
	query = strings.TrimSpace(query)
	query = strings.TrimSpace(strings.TrimSuffix(query, ";"))
	equal := strings.IndexByte(query, '=')
	if equal <= 0 {
		return "", false, false
	}

	lhs := strings.ToUpper(strings.Join(strings.Fields(query[:equal]), " "))
	switch lhs {
	case "ALTER SESSION SET TIME_ZONE",
		"ALTER SYSTEM SESSION SET TIME_ZONE",
		"SET TIME_ZONE",
		"SET SESSION TIME_ZONE",
		"SET @@TIME_ZONE",
		"SET @@SESSION.TIME_ZONE":
	default:
		return "", false, false
	}

	rest := strings.TrimSpace(query[equal+1:])
	if rest == "?" {
		return rest, true, true
	}
	if len(rest) >= 2 && (rest[0] == '\'' || rest[0] == '"') {
		quote := rest[0]
		for i := 1; i < len(rest); i++ {
			if rest[i] != quote {
				continue
			}
			if i+1 < len(rest) && rest[i+1] == quote {
				i++
				continue
			}
			if strings.TrimSpace(rest[i+1:]) != "" {
				return "", false, false
			}
			return strings.ReplaceAll(rest[1:i], string([]byte{quote, quote}), string(quote)), false, true
		}
		return "", false, false
	}
	if rest == "" || strings.ContainsAny(rest, " \t\r\n") {
		return "", false, false
	}
	return rest, false, true
}

func isSessionTimeZoneMutation(query string) bool {
	normalized := strings.ToUpper(strings.Join(strings.Fields(stripSQLComments(query)), " "))
	for _, prefix := range []string{
		"ALTER SESSION SET TIME_ZONE",
		"ALTER SYSTEM SESSION SET TIME_ZONE",
		"SET TIME_ZONE",
		"SET SESSION TIME_ZONE",
		"SET @@TIME_ZONE",
		"SET @@SESSION.TIME_ZONE",
	} {
		if normalized == prefix || strings.HasPrefix(normalized, prefix+" ") || strings.HasPrefix(normalized, prefix+"=") {
			return true
		}
	}
	return false
}

func stripSQLComments(query string) string {
	var stripped strings.Builder
	stripped.Grow(len(query))
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(query); {
		ch := query[i]
		if inSingleQuote {
			stripped.WriteByte(ch)
			i++
			if ch == '\'' {
				if i < len(query) && query[i] == '\'' {
					stripped.WriteByte(query[i])
					i++
					continue
				}
				inSingleQuote = false
			}
			continue
		}
		if inDoubleQuote {
			stripped.WriteByte(ch)
			i++
			if ch == '"' {
				if i < len(query) && query[i] == '"' {
					stripped.WriteByte(query[i])
					i++
					continue
				}
				inDoubleQuote = false
			}
			continue
		}

		if ch == '\'' {
			inSingleQuote = true
			stripped.WriteByte(ch)
			i++
			continue
		}
		if ch == '"' {
			inDoubleQuote = true
			stripped.WriteByte(ch)
			i++
			continue
		}
		if ch == '/' && i+1 < len(query) && query[i+1] == '*' {
			stripped.WriteByte(' ')
			i += 2
			for i+1 < len(query) && !(query[i] == '*' && query[i+1] == '/') {
				i++
			}
			if i+1 < len(query) {
				i += 2
			}
			continue
		}
		if ch == '-' && i+1 < len(query) && query[i+1] == '-' && (i+2 == len(query) || isSQLSpace(query[i+2])) {
			stripped.WriteByte(' ')
			i += 2
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}
		if ch == '#' {
			stripped.WriteByte(' ')
			i++
			for i < len(query) && query[i] != '\n' {
				i++
			}
			continue
		}

		stripped.WriteByte(ch)
		i++
	}
	return stripped.String()
}

func isSQLSpace(ch byte) bool {
	switch ch {
	case ' ', '\t', '\r', '\n', '\f', '\v':
		return true
	default:
		return false
	}
}

func (c *Conn) updateSessionTimeZoneFromQuery(query string, namedArgs ...[]driver.NamedValue) error {
	if c.tenantMode == "mysql" {
		return nil
	}
	value, parameter, ok := sessionTimeZoneAssignmentFromQuery(query)
	if !ok {
		if isSessionTimeZoneMutation(query) {
			return c.sessionTimeZoneSyncError(query, fmt.Errorf("unsupported session time zone mutation syntax"))
		}
		return nil
	}
	var args []driver.NamedValue
	if len(namedArgs) > 0 {
		args = namedArgs[0]
	}
	if parameter {
		var err error
		value, err = sessionTimeZoneValueFromArgs(args)
		if err != nil {
			return c.sessionTimeZoneSyncError(query, err)
		}
	}
	location, canonical, err := parseSessionTimeZone(value)
	if err != nil {
		c.tracef("session time zone query was accepted but cannot be decoded locally: %q: %v", value, err)
		return c.sessionTimeZoneSyncError(query, err)
	}
	c.sessionLocation = location
	c.tracef("session time zone changed by query: %s", canonical)
	return nil
}

func sessionTimeZoneValueFromArgs(args []driver.NamedValue) (string, error) {
	if len(args) != 1 {
		return "", fmt.Errorf("session time zone placeholder requires exactly one argument, got %d", len(args))
	}
	switch value := args[0].Value.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	default:
		return "", fmt.Errorf("session time zone argument has unsupported type %T", args[0].Value)
	}
}

func (c *Conn) sessionTimeZoneSyncError(query string, err error) error {
	// The server has already accepted the mutation, but the driver cannot
	// safely decode a subsequent TSLTZ value with the old location. Retire the
	// connection instead of allowing stale state to produce a wrong instant.
	c.sessionLocation = nil
	c.bad.Store(true)
	return fmt.Errorf("oceanbase: cannot synchronize session time zone for %q: %v: %w", query, err, driver.ErrBadConn)
}
