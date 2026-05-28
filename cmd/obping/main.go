package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
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

var validIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,29}$`)

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
