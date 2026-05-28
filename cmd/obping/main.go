package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/helingjun/obconnector-go"
)

const defaultQuery = "select 1 from dual"

func main() {
	var attrs repeatedFlag
	var initSQL repeatedFlag
	var (
		dsn       = flag.String("dsn", "", "DSN, for example oceanbase://user:pass@127.0.0.1:2881/db?timeout=5s")
		user      = flag.String("user", "", "OceanBase user, often user@tenant#cluster")
		pass      = flag.String("password", "", "OceanBase password")
		host      = flag.String("host", "127.0.0.1", "OceanBase host or OBProxy host")
		port      = flag.String("port", "2881", "OceanBase port or OBProxy port")
		dbName    = flag.String("database", "", "database/schema name")
		timeout   = flag.Duration("timeout", 10*time.Second, "connect and query timeout")
		trace     = flag.Bool("trace", false, "print handshake and query trace to stderr")
		capAdd    = flag.String("cap-add", "", "capability bits to force on, for example 0x200000")
		capDrop   = flag.String("cap-drop", "", "capability bits to force off, for example 0x100000")
		collation = flag.String("collation", "", "handshake collation id, for example 45")
		preset    = flag.String("preset", "", "client identity preset: default, oboracle, obclient, libobclient, connector-c, connector-j")
		ob20      = flag.Bool("ob20", false, "enable OB 2.0 protocol encapsulation")
		probe     = flag.Bool("probe-presets", false, "try all built-in client identity presets until one succeeds")
		query     = flag.String("query", defaultQuery, "query to execute")
		maxRows   = flag.Int("max-rows", 20, "maximum rows to print for non-default queries")
		txTest    = flag.Bool("tx-test", false, "run a basic begin/query/commit transaction test")
		execTest  = flag.Bool("exec-test", false, "run a DDL/DML ExecContext smoke test with a temporary table")
		execTable = flag.String("exec-table", "", "table name for -exec-test; defaults to a generated OBGO_SMOKE_* name")
		paramTest = flag.Bool("param-test", false, "run parameterized QueryContext/ExecContext smoke tests")
		poolTest  = flag.Bool("pool-test", false, "run database/sql pool lifecycle smoke tests")
		bulkTest  = flag.Bool("bulk-test", false, "run BulkInsert smoke test")
		fullTest  = flag.Bool("full-test", false, "run comprehensive integration tests (all of the above)")
		oraMode   = flag.Bool("oracle-mode", false, "force Oracle mode (equivalent to preset=oboracle)")
		mysqlMode = flag.Bool("mysql-mode", false, "force MySQL mode (equivalent to oracleMode=false)")
		tlsFlag   = flag.Bool("tls", false, "enable TLS")
		tlsCAFlag = flag.String("tls-ca", "", "path to CA certificate for TLS")
		tlsCert   = flag.String("tls-cert", "", "path to client certificate for mutual TLS")
		tlsKey    = flag.String("tls-key", "", "path to client private key for mutual TLS")
		compress  = flag.Bool("compress", false, "enable compression")
		charset   = flag.String("charset", "", "character set name (e.g. GBK, UTF8, UTF8MB4)")
		ob20Magic = flag.String("ob20-magic", "", "OB20 magic number (uint16, e.g. 0xCAFE)")
		addrs     = flag.String("addrs", "", "comma-separated list of host:port for multi-address failover")
		traceID   = flag.String("trace-id", "", "full-link trace ID")
		spanID    = flag.String("span-id", "", "full-link trace span ID")
		partID    = flag.String("partition-id", "", "full-link trace partition ID")
		check     = flag.Bool("check", false, "run protocol detection and SQL checks")
	)
	flag.Var(&attrs, "attr", "connection attribute key=value; can be repeated")
	flag.Var(&initSQL, "init", "initial SQL to run after auth; can be repeated")
	flag.Parse()

	connString := *dsn
	if connString == "" {
		if *user == "" {
			fmt.Fprintln(os.Stderr, "missing -user or -dsn")
			os.Exit(2)
		}
		connString = buildDSN(*user, *pass, *host, *port, *dbName, *timeout, *trace, *capAdd, *capDrop, *collation, *preset, *ob20, *oraMode, *compress, *charset, *ob20Magic, *addrs, attrs, initSQL)
	} else {
		var err error
		connString, err = applyExperimentParams(connString, *trace, *capAdd, *capDrop, *collation, *preset, *ob20, *oraMode, *compress, *charset, *ob20Magic, *addrs, attrs, initSQL)
		if err != nil {
			exitErr(err)
		}
	}

	// Apply TLS, MySQL mode, client cert, and full-link trace flags (applied to any DSN format)
	var extraParams url.Values
	if *tlsFlag || *tlsCAFlag != "" || *tlsCert != "" || *tlsKey != "" || *mysqlMode || *traceID != "" || *spanID != "" || *partID != "" {
		extraParams = url.Values{}
	}
	if *tlsFlag {
		extraParams.Set("tls", "true")
	}
	if *tlsCAFlag != "" {
		extraParams.Set("tls.ca", *tlsCAFlag)
	}
	if *tlsCert != "" {
		extraParams.Set("tls.cert", *tlsCert)
	}
	if *tlsKey != "" {
		extraParams.Set("tls.key", *tlsKey)
	}
	if *mysqlMode {
		extraParams.Set("oracleMode", "false")
	}
	if *traceID != "" {
		extraParams.Set("attr._ob_trace_id", *traceID)
	}
	if *spanID != "" {
		extraParams.Set("attr._ob_span_id", *spanID)
	}
	if *partID != "" {
		extraParams.Set("attr._ob_partition_id", *partID)
	}
	if extraParams != nil {
		connString = appendRawQuery(connString, extraParams)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if *check {
		if err := runCheck(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *probe {
		if err := probePresets(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *txTest {
		if err := runTxTest(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *execTest {
		if err := runExecTest(ctx, connString, *execTable); err != nil {
			exitErr(err)
		}
		return
	}

	if *paramTest {
		if err := runParamTest(ctx, connString, *execTable); err != nil {
			exitErr(err)
		}
		return
	}

	if *poolTest {
		if err := runPoolTest(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *bulkTest {
		if err := runBulkTest(ctx, connString, *execTable); err != nil {
			exitErr(err)
		}
		return
	}

	if *fullTest {
		if err := runFullTest(ctx, connString); err != nil {
			exitErr(err)
		}
		return
	}

	if *query == defaultQuery {
		value, err := runScalar(ctx, connString, *query)
		if err != nil {
			exitErr(err)
		}
		fmt.Printf("ok: %s => %v\n", *query, value)
		return
	}

	if err := runQuery(ctx, connString, *query, *maxRows); err != nil {
		exitErr(err)
	}
}

func openDB(connString string) (*sql.DB, error) {
	db, err := sql.Open("oceanbase", connString)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func runScalar(ctx context.Context, connString string, query string) (any, error) {
	db, err := openDB(connString)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var value any
	if err := db.QueryRowContext(ctx, query).Scan(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func probePresets(ctx context.Context, baseDSN string) error {
	presets := []string{"default", "oboracle", "obclient", "libobclient", "connector-c", "connector-j"}
	var lastErr error
	for _, preset := range presets {
		dsn, err := applyExperimentParams(baseDSN, false, "", "", "", preset, false, false, false, "", "", "", nil, nil)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "== probe preset %s ==\n", preset)
		value, err := runScalar(ctx, dsn, defaultQuery)
		if err == nil {
			fmt.Printf("ok: preset=%s select 1 from dual => %v\n", preset, value)
			return nil
		}
		fmt.Fprintf(os.Stderr, "preset %s failed: %v\n", preset, err)
		lastErr = err
	}
	return lastErr
}

func runQuery(ctx context.Context, connString string, query string, maxRows int) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	for i, column := range columns {
		typeName := ""
		if i < len(columnTypes) {
			typeName = columnTypes[i].DatabaseTypeName()
		}
		if i > 0 {
			fmt.Print("\t")
		}
		if typeName == "" {
			fmt.Print(column)
		} else {
			fmt.Printf("%s(%s)", column, typeName)
		}
	}
	fmt.Println()

	values := make([]any, len(columns))
	scan := make([]any, len(columns))
	for i := range values {
		scan[i] = &values[i]
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return err
		}
		if maxRows <= 0 || count < maxRows {
			printRow(values)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if maxRows > 0 && count > maxRows {
		fmt.Printf("... truncated, printed %d of %d rows\n", maxRows, count)
	} else {
		fmt.Printf("rows: %d\n", count)
	}
	return nil
}

func runTxTest(ctx context.Context, connString string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var value any
	if err := tx.QueryRowContext(ctx, defaultQuery).Scan(&value); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Printf("ok: tx %s => %v\n", defaultQuery, value)
	return nil
}

func runExecTest(ctx context.Context, connString string, tableName string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	tableName, err = smokeTableName(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("exec-test table: %s\n", tableName)

	if err := dropTableIgnoreMissing(ctx, db, tableName); err != nil {
		return err
	}
	defer func() {
		if err := dropTableIgnoreMissing(context.Background(), db, tableName); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup warning: %v\n", err)
		}
	}()

	if _, err := execStep(ctx, db, "create", fmt.Sprintf("create table %s (id number, name varchar2(40))", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "insert-1", fmt.Sprintf("insert into %s (id, name) values (1, 'alpha')", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "insert-2", fmt.Sprintf("insert into %s (id, name) values (2, 'beta')", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "update", fmt.Sprintf("update %s set name = 'gamma' where id = 2", tableName)); err != nil {
		return err
	}

	var count any
	if err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from %s", tableName)).Scan(&count); err != nil {
		return err
	}
	fmt.Printf("select-count: %v\n", formatValue(count))

	if _, err := execStep(ctx, db, "delete", fmt.Sprintf("delete from %s where id = 1", tableName)); err != nil {
		return err
	}
	if _, err := execStep(ctx, db, "drop", fmt.Sprintf("drop table %s", tableName)); err != nil {
		return err
	}
	fmt.Println("ok: exec-test completed")
	return nil
}

func runParamTest(ctx context.Context, connString string, tableName string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	var n, s, x any
	if err := db.QueryRowContext(ctx, "select ? as n, ? as s, ? as x from dual", int64(123), "O'Reilly", nil).Scan(&n, &s, &x); err != nil {
		return fmt.Errorf("param select failed: %w", err)
	}
	fmt.Printf("param-select: n=%s s=%s x=%s\n", formatValue(n), formatValue(s), formatValue(x))

	tableName, err = smokeTableName(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("param-test table: %s\n", tableName)

	if err := dropTableIgnoreMissing(ctx, db, tableName); err != nil {
		return err
	}
	defer func() {
		if err := dropTableIgnoreMissing(context.Background(), db, tableName); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup warning: %v\n", err)
		}
	}()

	if _, err := execStep(ctx, db, "create", fmt.Sprintf("create table %s (id number, name varchar2(80), created_at timestamp)", tableName)); err != nil {
		return err
	}
	when := time.Date(2026, 4, 30, 11, 56, 0, 123456000, time.UTC)
	if _, err := execStepArgs(ctx, db, "param-insert-1", fmt.Sprintf("insert into %s (id, name, created_at) values (?, ?, ?)", tableName), int64(1), "alpha", when); err != nil {
		return err
	}
	if _, err := execStepArgs(ctx, db, "param-insert-2", fmt.Sprintf("insert into %s (id, name, created_at) values (?, ?, ?)", tableName), int64(2), "beta's", nil); err != nil {
		return err
	}
	if _, err := execStepArgs(ctx, db, "param-update", fmt.Sprintf("update %s set name = ? where id = ?", tableName), "gamma", int64(2)); err != nil {
		return err
	}

	var count any
	if err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from %s where name = ?", tableName), "gamma").Scan(&count); err != nil {
		return fmt.Errorf("param count failed: %w", err)
	}
	fmt.Printf("param-count: %s\n", formatValue(count))

	if _, err := execStep(ctx, db, "drop", fmt.Sprintf("drop table %s", tableName)); err != nil {
		return err
	}
	fmt.Println("ok: param-test completed")
	return nil
}

func runPoolTest(ctx context.Context, connString string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	fmt.Println("pool-test ping: ok")

	firstID, err := queryOnDedicatedConn(ctx, db)
	if err != nil {
		return err
	}
	secondID, err := queryOnDedicatedConn(ctx, db)
	if err != nil {
		return err
	}
	fmt.Printf("pool-test idle-reuse: first=%s second=%s reused=%t\n", firstID, secondID, firstID == secondID)
	if firstID != secondID {
		return fmt.Errorf("idle connection was not reused")
	}

	if err := closeOneDriverConn(ctx, db); err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("bad connection retry ping failed: %w", err)
	}
	fmt.Println("pool-test bad-conn-retry: ok")

	if err := runConcurrentQueries(ctx, db, 8); err != nil {
		return err
	}
	stats := db.Stats()
	fmt.Printf("pool-test stats: open=%d in_use=%d idle=%d wait_count=%d\n", stats.OpenConnections, stats.InUse, stats.Idle, stats.WaitCount)
	fmt.Println("ok: pool-test completed")
	return nil
}

func runBulkTest(ctx context.Context, connString string, tableName string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	tableName, err = smokeTableName(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("bulk-test table: %s\n", tableName)

	if err := dropTableIgnoreMissing(ctx, db, tableName); err != nil {
		return err
	}
	defer func() {
		if err := dropTableIgnoreMissing(context.Background(), db, tableName); err != nil {
			fmt.Fprintf(os.Stderr, "cleanup warning: %v\n", err)
		}
	}()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("create table %s (id number, name varchar2(40))", tableName)); err != nil {
		return err
	}

	columns := []string{"id", "name"}
	values := [][]any{
		{int64(1), "alpha"},
		{int64(2), "beta"},
		{int64(3), "gamma"},
	}

	res, err := oceanbase.BulkInsert(ctx, db, tableName, columns, values)
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}
	affected, _ := res.RowsAffected()
	fmt.Printf("bulk-insert: rows_affected=%d\n", affected)

	var count int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("select count(*) from %s", tableName)).Scan(&count); err != nil {
		return err
	}
	fmt.Printf("bulk-count: %d\n", count)
	if count != 3 {
		return fmt.Errorf("expected 3 rows, got %d", count)
	}

	fmt.Println("ok: bulk-test completed")
	return nil
}

func runFullTest(ctx context.Context, connString string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	// 1. Ping
	fmt.Println("=== 1. Ping ===")
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	fmt.Println("Ping OK")

	// 2. Numeric Operations
	fmt.Println("\n=== 2. Numeric Operations ===")
	var numVal float64
	if err := db.QueryRowContext(ctx, "SELECT 1.5 + 2.3 FROM DUAL").Scan(&numVal); err != nil {
		return fmt.Errorf("numeric query failed: %w", err)
	}
	fmt.Printf("1.5 + 2.3 = %v\n", numVal)

	// 3. String Functions
	fmt.Println("\n=== 3. String Functions ===")
	var strVal string
	if err := db.QueryRowContext(ctx, "SELECT UPPER('hello world') FROM DUAL").Scan(&strVal); err != nil {
		return fmt.Errorf("string function query failed: %w", err)
	}
	fmt.Printf("UPPER('hello world') = %s\n", strVal)

	// 4. Timestamp
	fmt.Println("\n=== 4. Timestamp ===")
	var tsVal string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_TIMESTAMP FROM DUAL").Scan(&tsVal); err != nil {
		fmt.Printf("Timestamp query (non-fatal): %v\n", err)
	} else {
		fmt.Printf("CURRENT_TIMESTAMP = %s\n", tsVal)
	}

	// 5. DDL: Create Table
	fmt.Println("\n=== 5. DDL: Create Table ===")
	tableName, err := smokeTableName("")
	if err != nil {
		return err
	}
	fmt.Printf("full-test table: %s\n", tableName)

	if err := dropTableIgnoreMissing(ctx, db, tableName); err != nil {
		return err
	}
	defer func() {
		_ = dropTableIgnoreMissing(context.Background(), db, tableName)
	}()

	ddl := fmt.Sprintf("CREATE TABLE %s (id INTEGER, name VARCHAR(100), age INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)", tableName)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	fmt.Println("CREATE TABLE OK")

	// 6. INSERT + COMMIT (dedicated conn for transactional safety in Oracle mode)
	fmt.Println("\n=== 6. INSERT ===")
	insertConn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get insert conn failed: %w", err)
	}
	for i, row := range [][]any{{1, "Alice", 30}, {2, "Bob", 25}, {3, "Charlie", 35}} {
		if _, err := insertConn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", tableName), row...); err != nil {
			insertConn.Close()
			return fmt.Errorf("insert row %d failed: %w", i+1, err)
		}
	}
	if _, err := insertConn.ExecContext(ctx, "COMMIT"); err != nil {
		insertConn.Close()
		return fmt.Errorf("commit failed: %w", err)
	}
	insertConn.Close()
	fmt.Println("INSERT 3 rows OK")

	// 7. SELECT (multi-row)
	fmt.Println("\n=== 7. SELECT (multi-row) ===")
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT id, name, age FROM %s ORDER BY id", tableName))
	if err != nil {
		return fmt.Errorf("select failed: %w", err)
	}
	type person struct {
		id   int64
		name string
		age  int64
	}
	var people []person
	for rows.Next() {
		var p person
		if err := rows.Scan(&p.id, &p.name, &p.age); err != nil {
			rows.Close()
			return fmt.Errorf("row scan failed: %w", err)
		}
		people = append(people, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}
	fmt.Printf("Queried %d rows:\n", len(people))
	for _, p := range people {
		fmt.Printf("  id=%d name=%s age=%d\n", p.id, p.name, p.age)
	}

	// 8. Parameterized query with ? (text protocol — works with all servers)
	fmt.Println("\n=== 8. Parameterized Query (?) ===")
	var name string
	var age int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT name, age FROM %s WHERE id = ?", tableName), 1).Scan(&name, &age); err != nil {
		return fmt.Errorf("parameterized query failed: %w", err)
	}
	fmt.Printf("Param query: id=1 -> name=%s age=%d\n", name, age)

	// 9. Prepared Statement (?) — uses server-side prepared statement (COM_STMT_EXECUTE).
	// NOTE: Some OBProxy versions do not support COM_STMT_EXECUTE.
	// Feature test — non-fatal on failure. Uses a separate DB handle to avoid
	// connection desync (e.g. from unsupported :1 placeholders) affecting DML state.
	runPrepTest := func(label, query string) {
		fmt.Printf("\n=== %s ===\n", label)
		prepDB, err := openDB(connString)
		if err != nil {
			fmt.Printf("  get dedicated db failed: %v\n", err)
			return
		}
		defer prepDB.Close()
		conn, err := prepDB.Conn(ctx)
		if err != nil {
			fmt.Printf("  get conn failed: %v\n", err)
			return
		}
		stmtPrep, err := conn.PrepareContext(ctx, query)
		if err != nil {
			fmt.Printf("  Prepare failed: %v\n", err)
			conn.Close()
			return
		}
		var n string
		var a int64
		if err := stmtPrep.QueryRowContext(ctx, 1).Scan(&n, &a); err != nil {
			fmt.Printf("  Exec failed: %v\n", err)
			stmtPrep.Close()
			conn.Close()
			return
		}
		stmtPrep.Close()
		conn.Close()
		fmt.Printf("  id=1 -> name=%s age=%d\n", n, a)
	}

	runPrepTest("9. Prepared Statement (?)", fmt.Sprintf("SELECT name, age FROM %s WHERE id = ?", tableName))
	runPrepTest("10. Prepared Statement (:1)", fmt.Sprintf("SELECT name, age FROM %s WHERE id = :1", tableName))

	// 11. UPDATE
	fmt.Println("\n=== 11. UPDATE ===")
	res, err := db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET age = ? WHERE id = ?", tableName), 31, 1)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	affected, _ := res.RowsAffected()
	fmt.Printf("UPDATE affected %d row(s)\n", affected)

	var newAge int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT age FROM %s WHERE id = 1", tableName)).Scan(&newAge); err != nil {
		return fmt.Errorf("verify update failed: %w", err)
	}
	fmt.Printf("Alice's age is now %d (expected 31)\n", newAge)

	// 12. DELETE
	fmt.Println("\n=== 12. DELETE ===")
	res, err = db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName), 3)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}
	affected, _ = res.RowsAffected()
	fmt.Printf("DELETE affected %d row(s)\n", affected)

	var count int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
		return fmt.Errorf("count query failed: %w", err)
	}
	fmt.Printf("Remaining rows: %d (expected 2)\n", count)

	// 13. NULL Handling
	fmt.Println("\n=== 13. NULL Handling ===")
	if _, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", tableName), 4, nil, nil); err != nil {
		return fmt.Errorf("insert null failed: %w", err)
	}

	var nullName sql.NullString
	var nullAge sql.NullInt64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT name, age FROM %s WHERE id = 4", tableName)).Scan(&nullName, &nullAge); err != nil {
		return fmt.Errorf("null query failed: %w", err)
	}
	fmt.Printf("NULL row: name.Valid=%v age.Valid=%v\n", nullName.Valid, nullAge.Valid)

	// 14. Transaction Rollback
	fmt.Println("\n=== 14. Transaction Rollback ===")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET age = ? WHERE id = ?", tableName), 100, 1); err != nil {
		tx.Rollback()
		return fmt.Errorf("tx update failed: %w", err)
	}
	var txAge int64
	if err := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT age FROM %s WHERE id = 1", tableName)).Scan(&txAge); err != nil {
		tx.Rollback()
		return fmt.Errorf("tx query failed: %w", err)
	}
	fmt.Printf("In-transaction age: %d\n", txAge)
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT age FROM %s WHERE id = 1", tableName)).Scan(&newAge); err != nil {
		return fmt.Errorf("verify rollback failed: %w", err)
	}
	fmt.Printf("After rollback age: %d (expected 31, not 100)\n", newAge)

	// 15. Transaction Commit
	fmt.Println("\n=== 15. Transaction Commit ===")
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", tableName), 5, "Dave", 40); err != nil {
		tx.Rollback()
		return fmt.Errorf("tx insert failed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count); err != nil {
		return fmt.Errorf("count failed: %w", err)
	}
	fmt.Printf("After commit, rows: %d (expected 3)\n", count)

	// 16. Cleanup (table dropped by deferred function)
	fmt.Println("\n=== 16. Cleanup ===")
	fmt.Println("Dropped " + tableName)

	fmt.Println("\n=== ALL TESTS PASSED ===")
	return nil
}

func queryOnDedicatedConn(ctx context.Context, db *sql.DB) (string, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", fmt.Errorf("get dedicated conn failed: %w", err)
	}
	defer conn.Close()

	id, err := rawConnID(ctx, conn)
	if err != nil {
		return "", err
	}
	var value any
	if err := conn.QueryRowContext(ctx, defaultQuery).Scan(&value); err != nil {
		return "", fmt.Errorf("dedicated conn query failed: %w", err)
	}
	fmt.Printf("pool-test dedicated-query: conn=%s value=%s\n", id, formatValue(value))
	return id, nil
}

func rawConnID(ctx context.Context, conn *sql.Conn) (string, error) {
	var id string
	err := conn.Raw(func(driverConn any) error {
		id = fmt.Sprintf("%p", driverConn)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("raw conn id failed: %w", err)
	}
	return id, nil
}

func closeOneDriverConn(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get conn for bad-conn test failed: %w", err)
	}
	defer conn.Close()

	var id string
	if err := conn.Raw(func(driverConn any) error {
		id = fmt.Sprintf("%p", driverConn)
		closer, ok := driverConn.(interface{ Close() error })
		if !ok {
			return fmt.Errorf("driver conn %T has no Close", driverConn)
		}
		return closer.Close()
	}); err != nil {
		return fmt.Errorf("close raw driver conn failed: %w", err)
	}
	fmt.Printf("pool-test bad-conn-close: conn=%s closed\n", id)
	return nil
}

func runConcurrentQueries(ctx context.Context, db *sql.DB, workers int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var value any
			if err := db.QueryRowContext(ctx, "select ? from dual", int64(worker+1)).Scan(&value); err != nil {
				errCh <- fmt.Errorf("worker %d query failed: %w", worker, err)
				return
			}
			fmt.Printf("pool-test concurrent worker=%d value=%s\n", worker, formatValue(value))
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	fmt.Printf("pool-test concurrent: workers=%d ok\n", workers)
	return nil
}

func execStep(ctx context.Context, db *sql.DB, name string, query string) (sql.Result, error) {
	return execStepArgs(ctx, db, name, query)
}

func execStepArgs(ctx context.Context, db *sql.DB, name string, query string, args ...any) (sql.Result, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w", name, err)
	}
	affected, affectedErr := res.RowsAffected()
	insertID, insertErr := res.LastInsertId()
	fmt.Printf("%s: rows_affected=%s last_insert_id=%s\n", name, resultValue(affected, affectedErr), resultValue(insertID, insertErr))
	return res, nil
}

func resultValue(value int64, err error) string {
	if err != nil {
		return "unknown"
	}
	return fmt.Sprint(value)
}

func dropTableIgnoreMissing(ctx context.Context, db *sql.DB, tableName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("drop table %s", tableName))
	if err == nil || isMissingTableError(err) {
		return nil
	}
	return err
}

func isMissingTableError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "ORA-00942") || strings.Contains(msg, "error 942") ||
		strings.Contains(msg, "error 1051") || strings.Contains(msg, "Unknown table")
}

func smokeTableName(name string) (string, error) {
	if name == "" {
		return fmt.Sprintf("OBGO_SMOKE_%06X", time.Now().UnixNano()&0xffffff), nil
	}
	if !validIdentifier.MatchString(name) {
		return "", fmt.Errorf("invalid table name %q", name)
	}
	return strings.ToUpper(name), nil
}

var validIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,29}$`)

func printRow(values []any) {
	for i, value := range values {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(formatValue(value))
	}
	fmt.Println()
}

func formatValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

func discardRows(rows *sql.Rows) error {
	defer rows.Close()
	for rows.Next() {
	}
	if err := rows.Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}

type repeatedFlag []string

func (f *repeatedFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *repeatedFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func buildDSN(user, password, host, port, database string, timeout time.Duration, trace bool, capAdd, capDrop, collation, preset string, ob20, oracleMode, compress bool, charset, ob20Magic, addrs string, attrs, initSQL []string) string {
	hostPart := net.JoinHostPort(host, port)
	if addrs != "" {
		hostPart = addrs
	}
	u := &url.URL{
		Scheme: "oceanbase",
		User:   url.UserPassword(user, password),
		Host:   hostPart,
		Path:   database,
	}
	values, _ := experimentValues(trace, capAdd, capDrop, collation, preset, ob20, oracleMode, compress, charset, ob20Magic, attrs, initSQL)
	values.Set("timeout", timeout.String())
	u.RawQuery = values.Encode()
	return u.String()
}

func applyExperimentParams(dsn string, trace bool, capAdd, capDrop, collation, preset string, ob20, oracleMode, compress bool, charset, ob20Magic, addrs string, attrs, initSQL []string) (string, error) {
	values, changed := experimentValues(trace, capAdd, capDrop, collation, preset, ob20, oracleMode, compress, charset, ob20Magic, attrs, initSQL)
	if !strings.Contains(dsn, "://") {
		if strings.HasPrefix(dsn, "oceanbase:") || strings.HasPrefix(dsn, "oboracle:") {
			if !changed {
				return dsn, nil
			}
			return appendRawQuery(dsn, values), nil
		}
		if changed {
			return "", fmt.Errorf("experiment flags with -dsn require URL-style or oceanbase: DSN")
		}
		return dsn, nil
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	existing := u.Query()
	for key, vals := range values {
		for _, value := range vals {
			existing.Add(key, value)
		}
	}
	u.RawQuery = existing.Encode()
	return u.String(), nil
}

func experimentValues(trace bool, capAdd, capDrop, collation, preset string, ob20, oracleMode, compress bool, charset, ob20Magic string, attrs, initSQL []string) (url.Values, bool) {
	values := url.Values{}
	if oracleMode {
		values.Set("preset", "oboracle")
	}
	if trace {
		values.Set("trace", "true")
	}
	if ob20 {
		values.Set("ob20", "true")
	}
	if capAdd != "" {
		values.Set("cap.add", capAdd)
	}
	if capDrop != "" {
		values.Set("cap.drop", capDrop)
	}
	if collation != "" {
		values.Set("collation", collation)
	}
	if preset != "" {
		values.Set("preset", preset)
	}
	if compress {
		values.Set("compress", "true")
	}
	if charset != "" {
		charsetID := resolveCharset(charset)
		if charsetID != "" {
			values.Set("collation", charsetID)
		} else {
			values.Set("collation", charset)
		}
	}
	if ob20Magic != "" {
		values.Set("ob20.magic", ob20Magic)
	}
	for _, attr := range attrs {
		key, value, ok := strings.Cut(attr, "=")
		if !ok || key == "" {
			fmt.Fprintf(os.Stderr, "ignore malformed -attr %q, expected key=value\n", attr)
			continue
		}
		values.Set("attr."+key, value)
	}
	for _, query := range initSQL {
		values.Add("init", query)
	}
	return values, len(values) > 0
}

func appendRawQuery(dsn string, values url.Values) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + values.Encode()
}

func resolveCharset(name string) string {
	charsetMap := map[string]string{
		"big5":    "1",
		"latin1":  "5",
		"ascii":   "11",
		"gbk":     "28",
		"utf8":    "33",
		"utf8mb4": "45",
		"binary":  "63",
		"latin2":  "2",
		"gb2312":  "24",
		"gb18030": "248",
		"euckr":   "19",
		"sjis":    "13",
		"ujis":    "12",
		"utf16":   "54",
		"utf32":   "60",
	}
	if id, ok := charsetMap[strings.ToLower(name)]; ok {
		return id
	}
	return ""
}

func exitErr(err error) {
	fmt.Fprintf(os.Stderr, "obping: %v\n", err)
	os.Exit(1)
}

func runCheck(ctx context.Context, connString string) error {
	db, err := openDB(connString)
	if err != nil {
		return err
	}
	defer db.Close()

	// 1. Get raw connection info to check if OB2.0 was negotiated
	var isOB20 bool
	var connID uint32
	sqlConn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to establish connection: %w", err)
	}
	defer sqlConn.Close()

	err = sqlConn.Raw(func(driverConn any) error {
		if conn, ok := driverConn.(*oceanbase.Conn); ok {
			isOB20 = conn.IsOB20()
			connID = conn.ConnectionID()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to check raw connection: %w", err)
	}

	fmt.Println("=== OceanBase Protocol Detection ===")
	if isOB20 {
		fmt.Printf("Negotiated Protocol: OceanBase 2.0 (OB20)\n")
	} else {
		fmt.Printf("Negotiated Protocol: Standard MySQL Protocol\n")
	}
	if connID > 0 {
		fmt.Printf("Connection ID:       %d\n", connID)
	}
	fmt.Println()

	// 2. Perform SQL Checks
	fmt.Println("=== OceanBase SQL & Tenant Checks ===")

	tenantMode := "Unknown"
	dbVersion := "Unknown"
	currentUser := "Unknown"

	// Try checking if we are in Oracle mode
	var banner string
	isOracle := false
	if err := db.QueryRowContext(ctx, "select banner from v$version where rownum = 1").Scan(&banner); err == nil {
		isOracle = true
		tenantMode = "Oracle"
		dbVersion = banner
	} else {
		// Try checking via standard MySQL version query
		var ver string
		if err := db.QueryRowContext(ctx, "select version()").Scan(&ver); err == nil {
			isOracle = false
			tenantMode = "MySQL"
			dbVersion = ver
		}
	}

	// Get current user
	var userVal string
	userQuery := "select user from dual"
	if !isOracle {
		userQuery = "select user()"
	}
	if err := db.QueryRowContext(ctx, userQuery).Scan(&userVal); err == nil {
		currentUser = userVal
	}

	fmt.Printf("Tenant Mode:  %s\n", tenantMode)
	fmt.Printf("Current User: %s\n", currentUser)
	fmt.Printf("DB Version:   %s\n", dbVersion)

	// Check system table access
	fmt.Println("\n=== System Catalog / Dictionary Access Check ===")
	if isOracle {
		tables := []string{"all_tables", "user_tables", "all_users"}
		for _, tbl := range tables {
			var count int
			q := fmt.Sprintf("select count(*) from %s where rownum <= 1", tbl)
			if err := db.QueryRowContext(ctx, q).Scan(&count); err == nil {
				fmt.Printf("Access to %s: SUCCESS\n", tbl)
			} else {
				fmt.Printf("Access to %s: FAILED (%v)\n", tbl, err)
			}
		}
	} else {
		tables := []string{"information_schema.tables", "mysql.user"}
		for _, tbl := range tables {
			var count int
			q := fmt.Sprintf("select count(*) from %s limit 1", tbl)
			if err := db.QueryRowContext(ctx, q).Scan(&count); err == nil {
				fmt.Printf("Access to %s: SUCCESS\n", tbl)
			} else {
				fmt.Printf("Access to %s: FAILED (%v)\n", tbl, err)
			}
		}
	}

	// Transaction smoke check
	fmt.Println("\n=== Transaction Support Check ===")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Printf("Transaction Begin: FAILED (%v)\n", err)
	} else {
		var val int
		dualQuery := "select 1 from dual"
		if !isOracle {
			dualQuery = "select 1"
		}
		if err := tx.QueryRowContext(ctx, dualQuery).Scan(&val); err != nil {
			fmt.Printf("Transaction Query: FAILED (%v)\n", err)
			_ = tx.Rollback()
		} else {
			if err := tx.Commit(); err != nil {
				fmt.Printf("Transaction Commit: FAILED (%v)\n", err)
			} else {
				fmt.Printf("Transaction Smoke: SUCCESS\n")
			}
		}
	}

	fmt.Println("\nAll checks completed.")
	return nil
}
