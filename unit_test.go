package oceanbase

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
)

func TestQuoteIdent(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"my_table", "my_table"},
		{"col_1", "col_1"},
		{"MixedCase", "MixedCase"},
		{"with space", `"with space"`},
		{"with-dash", `"with-dash"`},
		{"1startnum", `"1startnum"`},
		{"", `""`},
		{`has"quote`, `"has""quote"`},
		{`with.dot`, `"with.dot"`},
	}
	for _, tc := range cases {
		got := quoteIdent(tc.input)
		if got != tc.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestIsSimpleIdent(t *testing.T) {
	cases := []struct {
		input string
		want  bool
	}{
		{"abc", true},
		{"Abc", true},
		{"a_b", true},
		{"a1b", true},
		{"_abc", false},
		{"", false},
		{"1abc", false},
		{"a b", false},
		{"a-b", false},
	}
	for _, tc := range cases {
		got := isSimpleIdent(tc.input)
		if got != tc.want {
			t.Errorf("isSimpleIdent(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestIsAlpha(t *testing.T) {
	if !isAlpha('a') || !isAlpha('Z') {
		t.Error("alpha mismatch")
	}
	if isAlpha('1') || isAlpha('_') {
		t.Error("non-alpha match")
	}
}

func TestIsAlnum(t *testing.T) {
	if !isAlnum('a') || !isAlnum('9') {
		t.Error("alnum mismatch")
	}
	if isAlnum('_') || isAlnum('-') {
		t.Error("non-alnum match")
	}
}

func TestQuoteIdentList(t *testing.T) {
	got := quoteIdentList([]string{"a", "b", "c"})
	if got != "a, b, c" {
		t.Errorf("got %q, want %q", got, "a, b, c")
	}
}

func TestValuesToNamed(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if got := valuesToNamed(nil); got != nil {
			t.Error("nil should return nil")
		}
		if got := valuesToNamed([]driver.Value{}); got != nil {
			t.Error("empty should return nil")
		}
	})
	t.Run("converts", func(t *testing.T) {
		vals := []driver.Value{int64(1), "hello"}
		named := valuesToNamed(vals)
		if len(named) != 2 {
			t.Fatalf("len = %d, want 2", len(named))
		}
		if named[0].Ordinal != 1 {
			t.Errorf("ordinal = %d, want 1", named[0].Ordinal)
		}
		if named[1].Ordinal != 2 {
			t.Errorf("ordinal = %d, want 2", named[1].Ordinal)
		}
	})
}

func TestUnwrapOutParam(t *testing.T) {
	t.Run("plain value", func(t *testing.T) {
		v := unwrapOutParam(int64(42))
		if v != int64(42) {
			t.Errorf("got %v, want 42", v)
		}
	})
	t.Run("sql.Out with dest", func(t *testing.T) {
		dest := "hello"
		v := unwrapOutParam(sql.Out{Dest: &dest})
		if v != "hello" {
			t.Errorf("got %v, want hello", v)
		}
	})
	t.Run("*sql.Out with dest", func(t *testing.T) {
		dest := int64(123)
		v := unwrapOutParam(&sql.Out{Dest: &dest})
		if v != int64(123) {
			t.Errorf("got %v, want 123", v)
		}
	})
}

func TestDerefValue(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		if derefValue(nil) != nil {
			t.Error("nil should stay nil")
		}
	})
	t.Run("non-pointer", func(t *testing.T) {
		if derefValue("hello") != "hello" {
			t.Error("string should stay string")
		}
	})
	t.Run("pointer", func(t *testing.T) {
		s := "world"
		v := derefValue(&s)
		if v != "world" {
			t.Errorf("got %v, want world", v)
		}
	})
	t.Run("double pointer", func(t *testing.T) {
		s := "nested"
		ps := &s
		v := derefValue(&ps)
		if v != "nested" {
			t.Errorf("got %v, want nested", v)
		}
	})
}

func TestIsEnvClosed(t *testing.T) {
	cases := []struct {
		env  string
		want bool
	}{
		{"", false},
		{"1", false},
		{"0", true},
		{"false", true},
		{"FALSE", true},
		{"off", true},
		{"OFF", true},
		{"no", true},
		{"No", true},
		{"true", false},
		{"on", false},
	}
	for _, tc := range cases {
		got := isEnvClosed(tc.env)
		if got != tc.want {
			t.Errorf("isEnvClosed(%q) = %v, want %v", tc.env, got, tc.want)
		}
	}
}

func TestBoolToOnOff(t *testing.T) {
	if boolToOnOff(true) != "on" {
		t.Error("true should be on")
	}
	if boolToOnOff(false) != "off" {
		t.Error("false should be off")
	}
}

func TestNegotiationResultLabel(t *testing.T) {
	if got := negotiationResultLabel(true, true, true); got != "ENABLED" {
		t.Errorf("got %q", got)
	}
	if got := negotiationResultLabel(false, true, false); got != "downgraded to uncompressed" {
		t.Errorf("got %q", got)
	}
	if got := negotiationResultLabel(false, false, true); got != "uncompressed (client opted out)" {
		t.Errorf("got %q", got)
	}
	if got := negotiationResultLabel(false, false, false); got != "uncompressed" {
		t.Errorf("got %q", got)
	}
}

func TestContextFunctions(t *testing.T) {
	ctx := context.Background()

	t.Run("PartitionID", func(t *testing.T) {
		ctx2 := WithPartitionID(ctx, 42)
		id, ok := partitionIDFromContext(ctx2)
		if !ok || id != 42 {
			t.Errorf("got %d, %v; want 42, true", id, ok)
		}
		_, ok = partitionIDFromContext(ctx)
		if ok {
			t.Error("should be missing in parent context")
		}
	})

	t.Run("TraceID", func(t *testing.T) {
		ctx2 := WithTraceID(ctx, "trace-123")
		id, ok := traceIDFromContext(ctx2)
		if !ok || id != "trace-123" {
			t.Errorf("got %q, %v", id, ok)
		}
		_, ok = traceIDFromContext(ctx)
		if ok {
			t.Error("should be missing in parent context")
		}
	})

	t.Run("SpanID", func(t *testing.T) {
		ctx2 := WithSpanID(ctx, "span-456")
		id, ok := spanIDFromContext(ctx2)
		if !ok || id != "span-456" {
			t.Errorf("got %q, %v", id, ok)
		}
		_, ok = spanIDFromContext(ctx)
		if ok {
			t.Error("should be missing in parent context")
		}
	})
}

func TestPresetCapabilities(t *testing.T) {
	tests := []struct {
		preset string
		want   bool // has ClientMultiStatements
	}{
		{"oboracle", true},
		{"obclient", true},
		{"connector-c", true},
		{"default", false},
		{"unknown", false},
	}
	for _, tc := range tests {
		caps := presetCapabilities(tc.preset)
		has := caps&0x00010000 != 0 // ClientMultiStatements
		if has != tc.want {
			t.Errorf("presetCapabilities(%q).MultiStatements = %v, want %v", tc.preset, has, tc.want)
		}
	}
}

func TestPresetAttributes(t *testing.T) {
	t.Run("connector-j", func(t *testing.T) {
		attrs := presetAttributes("connector-j")
		if attrs["_client_name"] != "OceanBase Connector/J" {
			t.Errorf("connector-j _client_name = %q", attrs["_client_name"])
		}
	})
	t.Run("default", func(t *testing.T) {
		attrs := presetAttributes("default")
		if attrs["_client_name"] != "OceanBase Connector/Go" {
			t.Errorf("default _client_name = %q", attrs["_client_name"])
		}
	})
}

func TestStmtCloseDoubleCall(t *testing.T) {
	s := &Stmt{}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("double close should succeed: %v", err)
	}
}

func TestResult(t *testing.T) {
	r := result{affectedRows: 10, lastInsertID: 5}
	id, err := r.LastInsertId()
	if err != nil || id != 5 {
		t.Errorf("LastInsertId: (%v, %v)", id, err)
	}
	rows, err := r.RowsAffected()
	if err != nil || rows != 10 {
		t.Errorf("RowsAffected: (%v, %v)", rows, err)
	}
}

func TestParseServerErrorBoundary(t *testing.T) {
	// Too short
	err := parseServerError([]byte{0xff})
	if err == nil {
		t.Error("should fail on truncated packet")
	}
	// Wrong header
	err = parseServerError([]byte{0x00, 0x00, 0x00})
	if err == nil {
		t.Error("should fail on non-error header")
	}
}

func TestInterpolateParamsHashComment(t *testing.T) {
	// # comment containing ? should be skipped
	got, err := interpolateParams(
		"SELECT 1 # ? comment\nFROM DUAL WHERE x = ?",
		[]driver.NamedValue{{Ordinal: 1, Value: "val"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT 1 # ? comment\nFROM DUAL WHERE x = 'val'"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestCountPlaceholdersHashComment(t *testing.T) {
	query := "SELECT 1 # ? comment\nFROM DUAL WHERE x = ?"
	if got := countPlaceholders(query); got != 1 {
		t.Fatalf("countPlaceholders = %d, want 1", got)
	}
}

func TestNilParamInInterpolation(t *testing.T) {
	got, err := interpolateParams(
		"INSERT INTO t VALUES (?)",
		[]driver.NamedValue{{Ordinal: 1, Value: nil}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != "INSERT INTO t VALUES (NULL)" {
		t.Fatalf("got %q", got)
	}
}

func TestBoolParamInInterpolation(t *testing.T) {
	got, _ := interpolateParams("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: true}})
	if got != "SELECT 1" {
		t.Fatalf("got %q, want 'SELECT 1'", got)
	}
	got, _ = interpolateParams("SELECT ?", []driver.NamedValue{{Ordinal: 1, Value: false}})
	if got != "SELECT 0" {
		t.Fatalf("got %q, want 'SELECT 0'", got)
	}
}

func TestByteSliceParam(t *testing.T) {
	got, err := interpolateParams(
		"SELECT ?",
		[]driver.NamedValue{{Ordinal: 1, Value: []byte{0xDE, 0xAD, 0xBE, 0xEF}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT hextoraw('DEADBEEF')" {
		t.Fatalf("got %q", got)
	}
}

func TestCheckNamedValueAcceptsOut(t *testing.T) {
	c := &Conn{}
	if err := c.CheckNamedValue(&driver.NamedValue{Value: sql.Out{Dest: new(int64)}}); err != nil {
		t.Errorf("should accept sql.Out: %v", err)
	}
	if err := c.CheckNamedValue(&driver.NamedValue{Value: &sql.Out{Dest: new(string)}}); err != nil {
		t.Errorf("should accept *sql.Out: %v", err)
	}
}

func TestAssignOutParamEdgeCases(t *testing.T) {
	c := &Conn{}

	t.Run("nilDest", func(t *testing.T) {
		if err := c.assignOutParam(nil, "whatever"); err != nil {
			t.Errorf("nil dest should be no-op: %v", err)
		}
	})

	t.Run("nilValueToNonNullPointer", func(t *testing.T) {
		var s string = "before"
		if err := c.assignOutParam(&s, nil); err != nil {
			t.Errorf("nil value assign: %v", err)
		}
		if s != "" {
			t.Errorf("string should be zero after nil assign, got %q", s)
		}
	})

	t.Run("scanner", func(t *testing.T) {
		var ns sql.NullString
		if err := c.assignOutParam(&ns, "hello"); err != nil {
			t.Fatalf("scanner assign: %v", err)
		}
		if ns.String != "hello" {
			t.Errorf("got %q", ns.String)
		}
	})

	t.Run("incompatibleType", func(t *testing.T) {
		type myType struct{ X int }
		var m myType
		if err := c.assignOutParam(&m, "string"); err == nil {
			t.Error("incompatible type should fail")
		}
	})

	t.Run("nilPointerDest", func(t *testing.T) {
		var p *int
		if err := c.assignOutParam(&p, nil); err != nil {
			t.Errorf("nil pointer dest: %v", err)
		}
	})
}

func TestRowsColumnTypeMethodsOutOfBounds(t *testing.T) {
	r := &Rows{
		colDefs: []columnDef{{name: "a", typ: 0x0f, columnLength: 64}},
		columns: []string{"a"},
		types:   []byte{0x0f},
	}

	if name := r.ColumnTypeDatabaseTypeName(-1); name != "" {
		t.Error("out of bounds should return empty")
	}
	if name := r.ColumnTypeDatabaseTypeName(1); name != "" {
		t.Error("out of bounds should return empty")
	}
	if _, ok := r.ColumnTypeLength(-1); ok {
		t.Error("out of bounds should return false")
	}
	if _, _, ok := r.ColumnTypePrecisionScale(-1); ok {
		t.Error("out of bounds should return false")
	}
	nullable, ok := r.ColumnTypeNullable(-1)
	if !nullable || !ok {
		t.Error("out of bounds should return true, true")
	}
	nullable, ok = r.ColumnTypeNullable(0)
	if !nullable || !ok {
		t.Errorf("inbounds: nullable=%v ok=%v", nullable, ok)
	}
	if scan := r.ColumnTypeScanType(-1); scan != reflect.TypeOf("") {
		t.Error("out of bounds scan type should default to string")
	}
	if cols := r.Columns(); len(cols) != 1 || cols[0] != "a" {
		t.Errorf("Columns: %v", cols)
	}
}

func TestRowNextEmptyBuffer(t *testing.T) {
	r := &Rows{values: nil, pos: 0}
	if err := r.Next(nil); err == nil {
		t.Error("Next on empty buffer should return io.EOF")
	}
}

func TestServerErrorOutput(t *testing.T) {
	e := &ServerError{Number: 1062, Message: "Duplicate entry"}
	if e.Error() != "oceanbase: error 1062: Duplicate entry" {
		t.Errorf("got %q", e.Error())
	}

	e2 := &ServerError{Number: 1062, SQLState: "23000", Message: "Duplicate entry"}
	if e2.Error() != "oceanbase: error 1062 (23000): Duplicate entry" {
		t.Errorf("got %q", e2.Error())
	}
}
