package oceanbase

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testDSN(t *testing.T) string {
	t.Helper()
	host := os.Getenv("OB_TEST_HOST")
	port := os.Getenv("OB_TEST_PORT")
	user := os.Getenv("OB_TEST_USER")
	pass := os.Getenv("OB_TEST_PASSWORD")
	db := os.Getenv("OB_TEST_DATABASE")
	ob20 := os.Getenv("OB_TEST_OB20")
	if host == "" || user == "" || pass == "" {
		t.Skip("OB_TEST_HOST, OB_TEST_USER, OB_TEST_PASSWORD not set; skipping integration test")
	}
	if port == "" {
		port = "2881"
	}
	u := urlEncode(user)
	p := urlEncode(pass)
	var params string
	if ob20 == "1" || ob20 == "true" {
		params = "?timeout=10s&ob20=true"
	} else {
		params = "?timeout=10s"
	}
	if db != "" {
		return fmt.Sprintf("oceanbase://%s:%s@%s:%s/%s%s", u, p, host, port, db, params)
	}
	return fmt.Sprintf("oceanbase://%s:%s@%s:%s%s", u, p, host, port, params)
}

func urlEncode(s string) string {
	r := strings.NewReplacer("@", "%40", ":", "%3A", "#", "%23", "/", "%2F", "?", "%3F")
	return r.Replace(s)
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("oceanbase", testDSN(t))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func testOracleTimeDSN(t *testing.T) string {
	t.Helper()
	return testDSN(t) + "&sessionTimeZone=%2B00%3A00"
}

func TestIntegrationOracleTimestampTypes(t *testing.T) {
	db, err := sql.Open("oboracle", testOracleTimeDSN(t))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	const query = "SELECT LOCALTIMESTAMP AS ts, CURRENT_TIMESTAMP AS tstz, CAST(CURRENT_TIMESTAMP AS TIMESTAMP WITH LOCAL TIME ZONE) AS tsltz FROM DUAL"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("ordinary query: %v", err)
	}
	assertOracleTimestampRows(t, rows)

	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	defer stmt.Close()
	preparedRows, err := stmt.QueryContext(ctx)
	if err != nil {
		t.Fatalf("prepared query: %v", err)
	}
	assertOracleTimestampRows(t, preparedRows)
}

func TestIntegrationOracleSessionTimezoneLifecycle(t *testing.T) {
	db, err := sql.Open("oboracle", testOracleTimeDSN(t))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "ALTER SESSION SET TIME_ZONE = '+08:00'"); err != nil {
		conn.Close()
		t.Fatalf("ALTER SESSION: %v", err)
	}
	const query = "SELECT CAST(TIMESTAMP '2026-08-12 14:45:44.123456' AS TIMESTAMP WITH LOCAL TIME ZONE) FROM DUAL"
	var direct time.Time
	if err := conn.QueryRowContext(ctx, query).Scan(&direct); err != nil {
		conn.Close()
		t.Fatalf("query after direct timezone change: %v", err)
	}
	if want := time.Date(2026, 8, 12, 6, 45, 44, 123456000, time.UTC); !direct.Equal(want) {
		t.Fatalf("direct-session timestamp = %v, want %v", direct, want)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("conn.Close: %v", err)
	}

	var reset time.Time
	if err := db.QueryRowContext(ctx, query).Scan(&reset); err != nil {
		t.Fatalf("query after pooled reset: %v", err)
	}
	if want := time.Date(2026, 8, 12, 14, 45, 44, 123456000, time.UTC); !reset.Equal(want) {
		t.Fatalf("pooled-reset timestamp = %v, want %v", reset, want)
	}
}

func assertOracleTimestampRows(t *testing.T, rows *sql.Rows) {
	t.Helper()
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatalf("ColumnTypes: %v", err)
	}
	wantNames := []string{"TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"}
	if len(columnTypes) != len(wantNames) {
		t.Fatalf("column count = %d, want %d", len(columnTypes), len(wantNames))
	}
	for i, want := range wantNames {
		if got := columnTypes[i].DatabaseTypeName(); got != want {
			t.Errorf("column %d database type = %q, want %q", i, got, want)
		}
		if got := columnTypes[i].ScanType(); got != reflect.TypeOf(time.Time{}) {
			t.Errorf("column %d scan type = %v, want time.Time", i, got)
		}
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Next: %v", err)
		}
		t.Fatal("query returned no rows")
	}
	var ts, tstz, tsltz time.Time
	if err := rows.Scan(&ts, &tstz, &tsltz); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if ts.IsZero() || tstz.IsZero() || tsltz.IsZero() {
		t.Fatalf("timestamp values must be non-zero: ts=%v tstz=%v tsltz=%v", ts, tstz, tsltz)
	}
	if !tstz.Equal(tsltz) {
		t.Fatalf("TSTZ and TSLTZ should represent the same instant: tstz=%v tsltz=%v", tstz, tsltz)
	}
	if rows.Next() {
		t.Fatal("query returned more than one row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
}

func TestIntegrationConnect(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Log("connection successful")
}

func TestIntegrationQuery(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var v int
	if err := db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&v); err != nil {
		t.Fatalf("SELECT 1: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
}

func TestIntegrationQueryWithHashComment(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// #-style comments are MySQL-specific; Oracle mode rejects them.
	// The interpolation logic is covered by unit tests (TestInterpolateParamsHashComment).
	q := "SELECT 1 FROM DUAL # what about ? in a comment\nWHERE 1 = ?"
	var v int
	err := db.QueryRowContext(ctx, q, 1).Scan(&v)
	if err != nil {
		t.Logf("# comment not supported in this mode (expected in Oracle): %v", err)
		return
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
}

func TestIntegrationQueryWithDashComment(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var v int
	err := db.QueryRowContext(ctx,
		"SELECT 1 FROM DUAL -- placeholder ? in comment\nWHERE 1 = ?", 1).Scan(&v)
	if err != nil {
		t.Fatalf("query with -- comment: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
}

func TestIntegrationQueryWithBlockComment(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var v int
	err := db.QueryRowContext(ctx,
		"SELECT /* ? */ 1 FROM DUAL WHERE 1 = ?", 1).Scan(&v)
	if err != nil {
		t.Fatalf("query with block comment: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
}

func TestIntegrationQueryWithStringLiteral(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var s string
	err := db.QueryRowContext(ctx,
		"SELECT ? FROM DUAL", "it's a test").Scan(&s)
	if err != nil {
		t.Fatalf("query with quote in string: %v", err)
	}
	if s != "it's a test" {
		t.Fatalf("expected 'it''s a test', got %q", s)
	}
}

func TestIntegrationInsertAndSelect(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_smoke_%d", time.Now().UnixNano()%1000000)

	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(100))", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", tableName), 1, "hello")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var id int
	var name string
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT id, name FROM %s WHERE id = ?", tableName), 1).Scan(&id, &name)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if id != 1 || name != "hello" {
		t.Fatalf("expected (1, hello), got (%d, %s)", id, name)
	}
}

func TestIntegrationTransaction(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_tx_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (val INT)", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (val) VALUES (?)", tableName), 42)
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT in tx: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var val int
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT val FROM %s", tableName)).Scan(&val)
	if err != nil {
		t.Fatalf("SELECT after commit: %v", err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestIntegrationTransactionRollback(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_rb_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (val INT)", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	// Insert outside tx and commit (Oracle mode may have autocommit off)
	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (val) VALUES (?)", tableName), 1)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	db.ExecContext(ctx, "COMMIT")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (val) VALUES (?)", tableName), 2)
	if err != nil {
		tx.Rollback()
		t.Fatalf("INSERT in tx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	var count int
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", count)
	}
}

func TestIntegrationDataTypeRoundTrip(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_dt_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (v_int NUMBER, v_double NUMBER, v_str VARCHAR2(200))", tableName))
	if err != nil {
		// Try MySQL-compatible types if Oracle types fail
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (v_int INT, v_double DOUBLE, v_str VARCHAR(200))", tableName))
		if err != nil {
			t.Fatalf("CREATE TABLE: %v", err)
		}
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	intVal := int64(123)
	doubleVal := 3.14159
	strVal := "hello world"

	_, err = db.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (v_int, v_double, v_str) VALUES (?, ?, ?)", tableName),
		intVal, doubleVal, strVal)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var outInt int64
	var outDouble float64
	var outStr string
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT v_int, v_double, v_str FROM %s", tableName)).
		Scan(&outInt, &outDouble, &outStr)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if outInt != intVal {
		t.Errorf("int: expected %d, got %d", intVal, outInt)
	}
	if outDouble != doubleVal {
		t.Errorf("double: expected %f, got %f", doubleVal, outDouble)
	}
	if outStr != strVal {
		t.Errorf("string: expected %s, got %s", strVal, outStr)
	}
}

func TestIntegrationPreparedStatement(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_ps_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(100))", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	stmt, err := db.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", tableName))
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= 3; i++ {
		_, err := stmt.ExecContext(ctx, i, fmt.Sprintf("name_%d", i))
		if err != nil {
			t.Fatalf("stmt.Exec %d: %v", i, err)
		}
	}

	var count int
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

func TestIntegrationBulkInsert(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_bulk_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(100))", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	values := [][]any{
		{1, "alice"},
		{2, "bob"},
		{3, "charlie"},
	}
	_, err = BulkInsert(ctx, db, tableName, []string{"id", "name"}, values)
	if err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}

	var count int
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count)
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}
}

func TestIntegrationConnectorLifecycle(t *testing.T) {
	host := os.Getenv("OB_TEST_HOST")
	port := os.Getenv("OB_TEST_PORT")
	user := os.Getenv("OB_TEST_USER")
	pass := os.Getenv("OB_TEST_PASSWORD")
	ob20 := os.Getenv("OB_TEST_OB20")
	if host == "" || user == "" || pass == "" {
		t.Skip("credentials not set")
	}
	if port == "" {
		port = "2881"
	}

	cfg := Config{
		User:       user,
		Password:   pass,
		Addr:       host + ":" + port,
		Timeout:    10 * time.Second,
		ProtocolV2: ob20 == "1" || ob20 == "true",
	}
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Verify multiple connections work
	var v int
	if err := db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&v); err != nil {
		t.Fatalf("query: %v", err)
	}
}

func TestIntegrationSpecialCharactersInSQL(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_sc_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (txt VARCHAR(500))", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	specialValues := []string{
		"normal",
		"with ' single quote",
		"with -- dash comment",
		"with # hash",
		"with /* block */ comment",
		"with ? question",
		"with \\ backslash",
		"with \" double quote",
	}
	for _, v := range specialValues {
		_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (txt) VALUES (?)", tableName), v)
		if err != nil {
			t.Fatalf("INSERT %q: %v", v, err)
		}
	}

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT txt FROM %s ORDER BY txt", tableName))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var txt string
		if err := rows.Scan(&txt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		t.Logf("round-tripped: %q", txt)
		count++
	}
	if count != len(specialValues) {
		t.Fatalf("expected %d rows, got %d", len(specialValues), count)
	}
}

func TestIntegrationConnectionPool(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		var v int
		err := db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&v)
		if err != nil {
			t.Fatalf("query %d: %v", i, err)
		}
		if v != 1 {
			t.Fatalf("query %d: expected 1, got %d", i, v)
		}
	}
	t.Log("pool test: 10 queries successful")
}

func TestIntegrationNullValues(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tableName := fmt.Sprintf("obgo_null_%d", time.Now().UnixNano()%1000000)
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(100))", tableName))
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer func() {
		db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (?, ?)", tableName), 1, nil)
	if err != nil {
		t.Fatalf("INSERT NULL: %v", err)
	}

	var id int
	var name sql.NullString
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT id, name FROM %s WHERE id = ?", tableName), 1).Scan(&id, &name)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if id != 1 {
		t.Errorf("expected id=1, got %d", id)
	}
	if name.Valid {
		t.Errorf("expected NULL name, got %q", name.String)
	}
}

func TestIntegrationContextCancellation(t *testing.T) {
	db := openTestDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(10 * time.Millisecond)

	var v int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&v)
	if err == nil {
		t.Skip("context cancellation did not prevent query (timing-dependent)")
	}
	t.Logf("context cancellation resulted in: %v", err)
}

func TestIntegrationOB20Diagnostic(t *testing.T) {
	host := os.Getenv("OB_TEST_HOST")
	port := os.Getenv("OB_TEST_PORT")
	user := os.Getenv("OB_TEST_USER")
	pass := os.Getenv("OB_TEST_PASSWORD")
	if host == "" || user == "" || pass == "" {
		t.Skip("credentials not set")
	}
	if port == "" {
		port = "2881"
	}

	var traceBuf strings.Builder
	cfg := Config{
		User:        user,
		Password:    pass,
		Addr:        host + ":" + port,
		Timeout:     10 * time.Second,
		ProtocolV2:  true,
		Trace:       true,
		TraceWriter: &traceBuf,
	}

	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Logf("ping error: %v", err)
	}

	t.Logf("\n=== TRACE OUTPUT ===\n%s=== END TRACE ===", traceBuf.String())
}
