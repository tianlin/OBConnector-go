package oceanbase

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

type Conn struct {
	netConn       net.Conn
	packets       *protocol.PacketConn
	cfg           *Config
	db            string // current database
	ob20Confirmed bool
	ob20Declined  bool
	ob20NewExtraInfoConfirmed bool

	mu     sync.Mutex
	closed bool
	bad    bool
	inTx   bool
}

type handshake struct {
	serverVersion string
	connectionID  uint32
	capabilities  uint32
	authPlugin    string
	authSeed      []byte
	status        uint16
}

func dialAndHandshake(ctx context.Context, cfg *Config) (*Conn, error) {
	var d net.Dialer
	if cfg.Timeout > 0 {
		d.Timeout = cfg.Timeout
	}
	netConn, err := d.DialContext(ctx, "tcp", cfg.Addr)
	if err != nil {
		return nil, err
	}

	c := &Conn{
		netConn: netConn,
		packets: protocol.NewPacketConn(netConn),
		cfg:     cfg,
	}
	if cfg.Trace && cfg.TraceWriter != nil {
		c.packets.SetTraceWriter(cfg.TraceWriter)
	}
	if err := c.withDeadline(ctx, func() error { return c.handshake() }); err != nil {
		_ = netConn.Close()
		return nil, err
	}
	for _, query := range cfg.InitSQL {
		c.tracef("init query: %s", query)
		if _, err := c.execLocked(ctx, query); err != nil {
			_ = netConn.Close()
			return nil, fmt.Errorf("init query %q failed: %w", query, err)
		}
	}
	return c, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if query == "" {
		return nil, errors.New("oceanbase: empty statement")
	}

	var stmt *Stmt
	err := c.withDeadline(ctx, func() error {
		c.packets.ResetSequence()
		c.packets.NextRequest()
		if err := c.packets.WritePacket(append([]byte{protocol.ComStmtPrepare}, query...)); err != nil {
			return err
		}

		packet, err := c.packets.ReadPacket()
		if err != nil {
			return err
		}
		if len(packet) == 0 {
			return io.ErrUnexpectedEOF
		}
		if packet[0] == protocol.ErrPacket {
			return parseServerError(packet)
		}
		if packet[0] != protocol.OKPacket {
			return fmt.Errorf("oceanbase: unexpected prepare response 0x%02x", packet[0])
		}

		if len(packet) < 12 {
			return io.ErrUnexpectedEOF
		}
		s := &Stmt{
			conn:        c,
			query:       query,
			stmtID:      binary.LittleEndian.Uint32(packet[1:5]),
			columnCount: int(binary.LittleEndian.Uint16(packet[5:7])),
			paramCount:  int(binary.LittleEndian.Uint16(packet[7:9])),
		}

		if s.paramCount > 0 {
			for i := 0; i < s.paramCount; i++ {
				if _, err := c.packets.ReadPacket(); err != nil {
					return err
				}
			}
			if err := c.readEOFOrOK(); err != nil {
				return err
			}
		}
		if s.columnCount > 0 {
			for i := 0; i < s.columnCount; i++ {
				if _, err := c.packets.ReadPacket(); err != nil {
					return err
				}
			}
			if err := c.readEOFOrOK(); err != nil {
				return err
			}
		}
		stmt = s
		return nil
	})
	if err != nil {
		return nil, c.markBadIfConnErr(err)
	}
	return stmt, nil
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.netConn == nil {
		return nil
	}
	c.packets.ResetSequence()
	c.packets.NextRequest()
	_ = c.packets.WritePacket([]byte{protocol.ComQuit})
	return c.netConn.Close()
}

func (c *Conn) closeStmt(stmtID uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.bad {
		return nil
	}
	c.packets.ResetSequence()
	c.packets.NextRequest()
	payload := make([]byte, 5)
	payload[0] = protocol.ComStmtClose
	binary.LittleEndian.PutUint32(payload[1:], stmtID)
	return c.packets.WritePacket(payload)
}

func (c *Conn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed && !c.bad
}

func (c *Conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return err
	}
	if !c.inTx {
		c.tracef("reset session: no active transaction")
		return nil
	}
	c.tracef("reset session: rollback active transaction")
	if _, err := c.execLocked(ctx, "rollback"); err != nil {
		return c.markBadIfConnErr(err)
	}
	c.inTx = false
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(0) {
		return nil, errors.New("oceanbase: custom transaction isolation is not implemented")
	}
	if opts.ReadOnly {
		return nil, errors.New("oceanbase: read-only transactions are not implemented")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTx {
		return nil, errors.New("oceanbase: transaction already active")
	}
	c.inTx = true
	return &Tx{conn: c}, nil
}

func (c *Conn) Ping(ctx context.Context) error {
	rows, err := c.QueryContext(ctx, "select 1 from dual", nil)
	if err != nil {
		return err
	}
	return rows.Close()
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	var err error
	query, err = interpolateParams(query, args)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if err := c.checkUsableLocked(); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	c.setupExtraInfo(ctx)
	rows, err := c.queryLocked(ctx, query)
	if err != nil {
		c.mu.Unlock()
		return nil, c.markBadIfConnErr(err)
	}
	if r, ok := rows.(*Rows); ok && r.streaming {
		r.release = c.mu.Unlock
	} else {
		c.mu.Unlock()
	}
	return rows, nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	var err error
	query, err = interpolateParams(query, args)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return nil, err
	}
	c.setupExtraInfo(ctx)
	res, err := c.execLocked(ctx, query)
	if err != nil {
		return nil, c.markBadIfConnErr(err)
	}
	return res, nil
}

func (c *Conn) CheckNamedValue(v *driver.NamedValue) error {
	// Let sql.Out pass through to ExecContext/QueryContext
	switch v.Value.(type) {
	case sql.Out, *sql.Out:
		return nil
	}
	return driver.ErrSkip
}

func (c *Conn) handshake() error {
	c.packets.ResetSequence()
	packet, err := c.packets.ReadPacket()
	if err != nil {
		return err
	}
	if len(packet) > 0 && packet[0] == protocol.ErrPacket {
		return parseServerError(packet)
	}

	hs, err := parseHandshake(packet)
	if err != nil {
		return err
	}
	c.tracef(
		"server handshake: version=%q connection_id=%d capability=0x%08x flags=%s status=0x%04x auth_plugin=%q seed_len=%d",
		hs.serverVersion,
		hs.connectionID,
		hs.capabilities,
		capabilityNames(hs.capabilities),
		hs.status,
		hs.authPlugin,
		len(hs.authSeed),
	)

	// TLS upgrade if requested and supported
	if c.cfg.TLSConfig != nil {
		if hs.capabilities&protocol.ClientSSL == 0 {
			return errors.New("oceanbase: server does not support SSL")
		}
		c.tracef("sending SSLRequest")
		if err := c.sendSSLRequest(); err != nil {
			return err
		}

		tlsConfig := c.cfg.TLSConfig
		if tlsConfig.ServerName == "" {
			host, _, err := net.SplitHostPort(c.cfg.Addr)
			if err == nil && net.ParseIP(host) == nil {
				tlsConfig = tlsConfig.Clone()
				tlsConfig.ServerName = host
			}
		}
		tlsConn := tls.Client(c.netConn, tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			return err
		}
		c.netConn = tlsConn
		c.packets = protocol.NewPacketConn(tlsConn)
	}

	if hs.authPlugin == "" {
		hs.authPlugin = "mysql_native_password"
	}

	authResp, err := buildAuthResponse(hs.authPlugin, c.cfg.Password, hs.authSeed)
	if err != nil {
		return err
	}

	response := c.buildHandshakeResponse(hs, authResp)
	c.tracef("client handshake response: payload_len=%d", len(response))
	if err := c.packets.WritePacket(response); err != nil {
		return err
	}

	authResult, err := c.packets.ReadPacket()
	if err != nil {
		return err
	}
	if len(authResult) == 0 {
		return io.ErrUnexpectedEOF
	}
	switch authResult[0] {
	case protocol.OKPacket:
		c.tracef("auth result: OK")
		if _, _, err := c.handleOK(authResult); err != nil {
			return err
		}
		if c.cfg.ProtocolV2 && !c.ob20Declined {
			magic := protocol.OB20MagicNum
			if c.cfg.OB20Magic != 0 {
				magic = c.cfg.OB20Magic
			}
			c.tracef("enabling OB 2.0 protocol encapsulation (ConnectionID: %d, Magic: 0x%04x, NewExtraInfo: %v)", hs.connectionID, magic, c.ob20NewExtraInfoConfirmed)
			c.packets.EnableOB20(hs.connectionID, magic, c.ob20NewExtraInfoConfirmed)
		}
		return nil
	case protocol.ErrPacket:
		c.tracef("auth result: ERR")
		return parseServerError(authResult)
	default:
		return fmt.Errorf("oceanbase: unexpected auth response 0x%02x", authResult[0])
	}
}

func (c *Conn) sendSSLRequest() error {
	caps := protocol.ClientLongPassword |
		protocol.ClientLongFlag |
		protocol.ClientProtocol41 |
		protocol.ClientTransactions |
		protocol.ClientSecureConnection |
		protocol.ClientMultiResults |
		protocol.ClientPluginAuth |
		protocol.ClientPluginAuthLenencClientData |
		protocol.ClientConnectAttrs |
		protocol.ClientSessionTrack |
		protocol.ClientSupportOracleMode |
		protocol.ClientSSL

	payload := make([]byte, 32)
	binary.LittleEndian.PutUint32(payload[0:4], caps)
	binary.LittleEndian.PutUint32(payload[4:8], protocol.DefaultMaxPacketSize)
	payload[8] = c.cfg.Collation

	c.packets.ResetSequence()
	return c.packets.WritePacket(payload)
}

func buildAuthResponse(plugin, password string, seed []byte) ([]byte, error) {
	switch plugin {
	case "mysql_native_password":
		return protocol.NativePasswordAuth(password, seed), nil
	case "caching_sha2_password":
		return protocol.CachingSha2PasswordAuth(password, seed), nil
	default:
		return nil, fmt.Errorf("oceanbase: auth plugin %q is not implemented", plugin)
	}
}

func (c *Conn) buildHandshakeResponse(hs *handshake, authResp []byte) []byte {
	baseCaps := protocol.ClientLongPassword |
		protocol.ClientLongFlag |
		protocol.ClientProtocol41 |
		protocol.ClientTransactions |
		protocol.ClientSecureConnection |
		protocol.ClientMultiResults |
		protocol.ClientPluginAuth |
		protocol.ClientPluginAuthLenencClientData |
		protocol.ClientConnectAttrs |
		protocol.ClientSessionTrack |
		protocol.ClientSupportOracleMode
	if c.cfg.TLSConfig != nil {
		baseCaps |= protocol.ClientSSL
	}
	baseCaps |= presetCapabilities(c.cfg.Preset)
	caps := baseCaps
	if hs.capabilities&protocol.ClientProtocol41 == 0 {
		caps &^= protocol.ClientProtocol41
	}
	if c.cfg.Database != "" {
		caps |= protocol.ClientConnectWithDB
	}
	caps |= c.cfg.CapabilityAdd
	caps &^= c.cfg.CapabilityDrop

	// Negotiate compression
	envOverride := os.Getenv("OB_USE_COMPRESSION")
	negotiatedCompress := NegotiateCompression(c.cfg.UseCompression, hs.capabilities, envOverride)
	if negotiatedCompress {
		caps |= protocol.ClientCompress
	}

	// Format negotiation logs
	clientWants := c.cfg.UseCompression && !isEnvClosed(envOverride)
	serverSupports := hs.capabilities&protocol.ClientCompress != 0
	c.tracef("Compression negotiation: client=%s, server=%s -> %s",
		boolToOnOff(clientWants),
		boolToOnOff(serverSupports),
		negotiationResultLabel(negotiatedCompress, clientWants, serverSupports))

	c.tracef(
		"client capabilities: base=0x%08x add=0x%08x drop=0x%08x final=0x%08x flags=%s",
		baseCaps,
		c.cfg.CapabilityAdd,
		c.cfg.CapabilityDrop,
		caps,
		capabilityNames(caps),
	)

	out := make([]byte, 0, 128)
	out = binary.LittleEndian.AppendUint32(out, caps)
	out = binary.LittleEndian.AppendUint32(out, protocol.DefaultMaxPacketSize)
	out = append(out, c.cfg.Collation)
	// Reserved (23 bytes): 19 bytes of zeros + 4 bytes of extended client capabilities
	// (lower 32-bit of ob_capability_flag) in Little-Endian.
	out = append(out, make([]byte, 19)...)
	proxyCaps := obClientCapFullLinkTrace |
		obClientCapProxyNewExtraInfo |
		obClientCapProxyFullLinkTraceShowTrace
	if c.cfg.ProtocolV2 {
		proxyCaps |= obClientCapOBProtocolV2
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(proxyCaps))
	out = append(out, c.cfg.User...)
	out = append(out, 0x00)
	out = protocol.PutLengthEncodedString(out, string(authResp))
	if caps&protocol.ClientConnectWithDB != 0 {
		out = append(out, c.cfg.Database...)
		out = append(out, 0x00)
	}
	if caps&protocol.ClientPluginAuth != 0 {
		out = append(out, hs.authPlugin...)
		out = append(out, 0x00)
	}
	if caps&protocol.ClientConnectAttrs != 0 {
		attrs := c.connectionAttributes(hs)
		attrPayload := make([]byte, 0, 128)
		for _, kv := range attrs {
			c.tracef("client attr: %s=%q", kv[0], kv[1])
			attrPayload = protocol.PutLengthEncodedString(attrPayload, kv[0])
			attrPayload = protocol.PutLengthEncodedString(attrPayload, kv[1])
		}
		out = protocol.PutLengthEncodedInt(out, uint64(len(attrPayload)))
		out = append(out, attrPayload...)
	}
	return out
}

func (c *Conn) connectionAttributes(hs *handshake) [][2]string {
	attrMap := map[string]string{
		"_client_name":      "OceanBase JDBC Driver",
		"_client_version":   "2.2.10",
		"_os":               runtime.GOOS,
		"_platform":         runtime.GOARCH,
		"program_name":      "obclient",
		"ob_server_version": hs.serverVersion,
	}
	for k, v := range presetAttributes(c.cfg.Preset) {
		attrMap[k] = v
	}
	for k, v := range c.cfg.Attributes {
		attrMap[k] = v
	}

	if !c.cfg.ProtocolV2 {
		if v, ok := attrMap["ob_capability_flag"]; ok {
			capVal, _ := strconv.ParseUint(v, 10, 64)
			capVal &^= obClientCapOBProtocolV2
			attrMap["ob_capability_flag"] = strconv.FormatUint(capVal, 10)
		}
		if v, ok := attrMap["__proxy_capability_flag"]; ok {
			capVal, _ := strconv.ParseUint(v, 10, 64)
			capVal &^= obClientCapOBProtocolV2
			attrMap["__proxy_capability_flag"] = strconv.FormatUint(capVal, 10)
		}
	}

	keys := make([]string, 0, len(attrMap))
	for k := range attrMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	attrs := make([][2]string, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, [2]string{k, attrMap[k]})
	}
	return attrs
}

func (c *Conn) stmtQueryLocked(ctx context.Context, stmtID uint32, args []driver.NamedValue) (driver.Rows, error) {
	var rows driver.Rows
	err := c.withDeadline(ctx, func() error {
		c.setupExtraInfo(ctx)
		if err := c.writeExecute(stmtID, args); err != nil {
			return err
		}
		r, err := c.readQueryResult()
		if err != nil {
			return err
		}
		if res, ok := r.(*Rows); ok {
			res.binary = true
		}
		rows = r
		return nil
	})
	return rows, err
}

func (c *Conn) stmtExecLocked(ctx context.Context, stmtID uint32, args []driver.NamedValue) (driver.Result, error) {
	var result driver.Result
	err := c.withDeadline(ctx, func() error {
		c.setupExtraInfo(ctx)
		if err := c.writeExecute(stmtID, args); err != nil {
			return err
		}
		first, err := c.packets.ReadPacket()
		if err != nil {
			return err
		}
		if len(first) == 0 {
			return io.ErrUnexpectedEOF
		}
		switch first[0] {
		case protocol.OKPacket:
			res, status, err := c.handleOK(first)
			if err != nil {
				return err
			}
			result = res
			if status&protocol.ServerPSOutParams != 0 {
				if err := c.readOutParams(args); err != nil {
					return err
				}
			}
		case protocol.ErrPacket:
			return parseServerError(first)
		default:
			// Exec against a SELECT-like statement: drain the result set
			// to keep the connection usable and avoid desync.
			res, err := c.readResultFromFirstPacket(first)
			if err != nil {
				return err
			}
			result = res
		}
		return nil
	})
	return result, err
}

func (c *Conn) readOutParams(args []driver.NamedValue) error {
	rows, err := c.readQueryResult()
	if err != nil {
		return err
	}
	r, ok := rows.(*Rows)
	if !ok {
		return fmt.Errorf("oceanbase: unexpected rows type for OUT parameters")
	}
	r.binary = true // OUT parameters are returned in binary format
	defer r.Close()

	dest := make([]driver.Value, len(r.columns))
	if err := r.Next(dest); err != nil {
		return err
	}

	outIdx := 0
	for _, arg := range args {
		var outDest any
		if out, ok := arg.Value.(sql.Out); ok {
			outDest = out.Dest
		} else if out, ok := arg.Value.(*sql.Out); ok {
			outDest = out.Dest
		} else {
			continue
		}

		if outIdx < len(dest) {
			if err := c.assignOutParam(outDest, dest[outIdx]); err != nil {
				return err
			}
			outIdx++
		}
	}

	return nil
}

func (c *Conn) assignOutParam(dest any, value driver.Value) error {
	if dest == nil {
		return nil
	}

	if scanner, ok := dest.(sql.Scanner); ok {
		return scanner.Scan(value)
	}

	if value == nil {
		// Destination is not a scanner, but value is nil.
		// If it's a pointer, we could zero it.
		dv := reflect.ValueOf(dest)
		if dv.Kind() == reflect.Ptr && !dv.IsNil() {
			dv.Elem().Set(reflect.Zero(dv.Elem().Type()))
		}
		return nil
	}

	dv := reflect.ValueOf(dest)
	if dv.Kind() != reflect.Ptr || dv.IsNil() {
		return fmt.Errorf("oceanbase: OUT parameter destination must be a non-nil pointer")
	}

	vv := reflect.ValueOf(value)
	if vv.Type().AssignableTo(dv.Elem().Type()) {
		dv.Elem().Set(vv)
		return nil
	}

	if vv.Type().ConvertibleTo(dv.Elem().Type()) {
		dv.Elem().Set(vv.Convert(dv.Elem().Type()))
		return nil
	}

	// Handle common conversions manually if needed (e.g. string to int)
	return fmt.Errorf("oceanbase: cannot assign OUT parameter of type %T to %T", value, dest)
}

func (c *Conn) stmtBulkExecLocked(ctx context.Context, stmtID uint32, argRows [][]driver.NamedValue) (driver.Result, error) {
	if len(argRows) == 0 {
		return result{}, nil
	}

	var result driver.Result
	err := c.withDeadline(ctx, func() error {
		c.setupExtraInfo(ctx)
		c.packets.ResetSequence()
		c.packets.NextRequest()

		// COM_STMT_BULK_EXECUTE (0xFA)
		// Header: 0xFA (1) + StmtID (4) + Flags (2)
		const SEND_TYPES_TO_SERVER uint16 = 0x80
		header := make([]byte, 7)
		header[0] = protocol.ComStmtBulkExecute
		binary.LittleEndian.PutUint32(header[1:5], stmtID)
		binary.LittleEndian.PutUint16(header[5:7], SEND_TYPES_TO_SERVER)

		// Param Types (2 bytes per param)
		numParams := len(argRows[0])
		paramTypes := make([]byte, numParams*2)
		for i, arg := range argRows[0] {
			val := arg.Value
			if out, ok := val.(sql.Out); ok {
				val = out.Dest
			} else if out, ok := val.(*sql.Out); ok {
				val = out.Dest
			}
			binary.LittleEndian.PutUint16(paramTypes[i*2:i*2+2], uint16(protocol.GetBinaryParamType(val)))
		}

		// Payload construction
		var payload []byte
		payload = append(payload, header...)
		payload = append(payload, paramTypes...)

		for _, row := range argRows {
			for _, arg := range row {
				val := arg.Value
				if out, ok := val.(sql.Out); ok {
					val = out.Dest
				} else if out, ok := val.(*sql.Out); ok {
					val = out.Dest
				}

				if val == nil {
					payload = append(payload, 1) // NULL
				} else {
					payload = append(payload, 0) // NOT NULL
					var err error
					payload, err = protocol.AppendBinaryParam(payload, protocol.GetBinaryParamType(val), val)
					if err != nil {
						return err
					}
				}
			}
		}

		if err := c.packets.WritePacket(payload); err != nil {
			return err
		}

		first, err := c.packets.ReadPacket()
		if err != nil {
			return err
		}
		res, _, err := c.handleOK(first)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	return result, err
}

func (c *Conn) writeExecute(stmtID uint32, args []driver.NamedValue) error {
	c.packets.ResetSequence()
	c.packets.NextRequest()

	// COM_STMT_EXECUTE
	// 0x17 (1) + StmtID (4) + Flags (1) + Iteration (4)
	payload := make([]byte, 10)
	payload[0] = protocol.ComStmtExecute
	binary.LittleEndian.PutUint32(payload[1:5], stmtID)
	payload[5] = 0 // Flags: CURSOR_TYPE_READ_ONLY = 0
	binary.LittleEndian.PutUint32(payload[6:10], 1)

	if len(args) > 0 {
		// NULL bitmap
		nullBitmap := make([]byte, (len(args)+7)/8)
		newParamsBound := byte(1)
		paramTypes := make([]byte, len(args)*2)
		var paramValues []byte

		for i, arg := range args {
			val := arg.Value
			// Handle sql.Out
			if out, ok := val.(sql.Out); ok {
				val = out.Dest
				// If it's a pointer to a pointer, or similar, we might need to dereference.
				// For now, let's just handle simple types.
			} else if out, ok := val.(*sql.Out); ok {
				val = out.Dest
			}

			if val == nil {
				nullBitmap[i/8] |= 1 << (uint(i) % 8)
			}
			typ := protocol.GetBinaryParamType(val)
			paramTypes[i*2] = typ
			paramTypes[i*2+1] = 0 // unsigned flag

			var err error
			paramValues, err = protocol.AppendBinaryParam(paramValues, typ, val)
			if err != nil {
				return err
			}
		}

		payload = append(payload, nullBitmap...)
		payload = append(payload, newParamsBound)
		payload = append(payload, paramTypes...)
		payload = append(payload, paramValues...)
	}

	return c.packets.WritePacket(payload)
}

func (c *Conn) queryLocked(ctx context.Context, query string) (driver.Rows, error) {
	var rows driver.Rows
	err := c.withDeadline(ctx, func() error {
		if err := c.writeQuery(query); err != nil {
			return err
		}
		r, err := c.readQueryResult()
		if err != nil {
			return err
		}
		rows = r
		return nil
	})
	return rows, err
}

func (c *Conn) execLocked(ctx context.Context, query string) (driver.Result, error) {
	var result driver.Result
	err := c.withDeadline(ctx, func() error {
		if err := c.writeQuery(query); err != nil {
			return err
		}
		first, err := c.packets.ReadPacket()
		if err != nil {
			return err
		}
		res, err := c.readResultFromFirstPacket(first)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	return result, err
}

func (c *Conn) writeQuery(query string) error {
	c.tracef("query: %s", query)
	c.packets.ResetSequence()
	c.packets.NextRequest()
	return c.packets.WritePacket(append([]byte{protocol.ComQuery}, query...))
}

func (c *Conn) setupExtraInfo(ctx context.Context) {
	c.packets.ClearExtraInfo()
	if id, ok := partitionIDFromContext(ctx); ok {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(id))
		c.packets.AddExtraInfo(protocol.OB20ExtraInfoTypePartitionID, buf)
	}

	traceID, okT := traceIDFromContext(ctx)
	spanID, okS := spanIDFromContext(ctx)
	if okT || okS {
		c.packets.AddExtraInfo(protocol.OB20ExtraInfoTypeFullTrace, protocol.BuildFLTExtraInfo(traceID, spanID))
	}
}

func (c *Conn) tracef(format string, args ...any) {
	if c == nil || c.cfg == nil || !c.cfg.Trace || c.cfg.TraceWriter == nil {
		return
	}
	_, _ = fmt.Fprintf(c.cfg.TraceWriter, "obconnector-go: "+format+"\n", args...)
}

func (c *Conn) checkUsableLocked() error {
	if c.closed || c.bad {
		return driver.ErrBadConn
	}
	return nil
}

func (c *Conn) markBadIfConnErr(err error) error {
	if err == nil {
		return nil
	}
	if isBadConnError(err) {
		c.bad = true
		return driver.ErrBadConn
	}
	return err
}

func (c *Conn) withDeadline(ctx context.Context, fn func() error) error {
	deadline, ok := ctx.Deadline()
	if !ok && c.cfg.Timeout > 0 {
		deadline = time.Now().Add(c.cfg.Timeout)
		ok = true
	}
	if ok {
		if err := c.netConn.SetDeadline(deadline); err != nil {
			return err
		}
		defer c.netConn.SetDeadline(time.Time{})
	}

	cancelled := make(chan struct{})
	if ctx.Done() != nil {
		defer close(cancelled)
		go func() {
			select {
			case <-ctx.Done():
				_ = c.netConn.SetDeadline(time.Now())
			case <-cancelled:
			}
		}()
	}

	err := fn()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			c.bad = true
			return ctxErr
		}
	}
	return err
}

func parseHandshake(packet []byte) (*handshake, error) {
	if len(packet) < 34 {
		return nil, io.ErrUnexpectedEOF
	}
	if packet[0] != 10 {
		return nil, fmt.Errorf("unsupported protocol version %d", packet[0])
	}

	pos := 1
	serverVersion, used, err := readNullTerminated(packet[pos:])
	if err != nil {
		return nil, err
	}
	pos += used
	if len(packet) < pos+13 {
		return nil, io.ErrUnexpectedEOF
	}

	hs := &handshake{
		serverVersion: serverVersion,
		connectionID:  binary.LittleEndian.Uint32(packet[pos : pos+4]),
	}
	pos += 4
	seed1 := append([]byte(nil), packet[pos:pos+8]...)
	pos += 9
	hs.capabilities = uint32(binary.LittleEndian.Uint16(packet[pos : pos+2]))
	pos += 2
	if len(packet) <= pos {
		hs.authSeed = seed1
		return hs, nil
	}
	pos++ // character set
	hs.status = binary.LittleEndian.Uint16(packet[pos : pos+2])
	pos += 2
	hs.capabilities |= uint32(binary.LittleEndian.Uint16(packet[pos:pos+2])) << 16
	pos += 2

	authPluginDataLen := 0
	if hs.capabilities&protocol.ClientPluginAuth != 0 {
		authPluginDataLen = int(packet[pos])
	}
	pos++
	pos += 10

	seed2Len := 12
	if authPluginDataLen > 0 {
		seed2Len = authPluginDataLen - 8
		if seed2Len < 12 {
			seed2Len = 12
		}
	}
	if len(packet) < pos+seed2Len {
		seed2Len = len(packet) - pos
	}
	if seed2Len > 0 {
		seed2 := append([]byte(nil), packet[pos:pos+seed2Len]...)
		hs.authSeed = append(seed1, bytes.TrimRight(seed2, "\x00")...)
		pos += seed2Len
	} else {
		hs.authSeed = seed1
	}
	if hs.capabilities&protocol.ClientPluginAuth != 0 && pos < len(packet) {
		plugin, _, err := readNullTerminated(packet[pos:])
		if err == nil {
			hs.authPlugin = plugin
		}
	}
	return hs, nil
}

func readNullTerminated(src []byte) (string, int, error) {
	for i, b := range src {
		if b == 0x00 {
			return string(src[:i]), i + 1, nil
		}
	}
	return "", 0, io.ErrUnexpectedEOF
}

type result struct {
	affectedRows int64
	lastInsertID int64
}

func (r result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r result) RowsAffected() (int64, error) { return r.affectedRows, nil }

func (c *Conn) handleOK(packet []byte) (res driver.Result, status uint16, err error) {
	if len(packet) == 0 || packet[0] != protocol.OKPacket {
		return nil, 0, fmt.Errorf("not an OK packet")
	}
	pos := 1
	affected, used, _, err := protocol.ReadLengthEncodedInt(packet[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += used
	lastID, used, _, err := protocol.ReadLengthEncodedInt(packet[pos:])
	if err != nil {
		return nil, 0, err
	}
	pos += used

	if pos < len(packet) {
		status = binary.LittleEndian.Uint16(packet[pos : pos+2])
		pos += 2
		if status&protocol.ServerSessionStateChanged != 0 {
			if pos < len(packet) {
				if err := c.handleStateChange(packet[pos:]); err != nil {
					c.tracef("failed to parse state change: %v", err)
				}
			}
			c.tracef("session state changed (status=0x%04x)", status)
		}
	}

	return result{affectedRows: int64(affected), lastInsertID: int64(lastID)}, status, nil
}

func (c *Conn) handleStateChange(data []byte) error {
	raw, _, _, err := protocol.ReadLengthEncodedString(data)
	if err != nil {
		return err
	}

	pos := 0
	for pos < len(raw) {
		typ := raw[pos]
		pos++
		val, used, _, err := protocol.ReadLengthEncodedString(raw[pos:])
		if err != nil {
			return err
		}
		pos += used

		switch typ {
		case 0x00, 0x26: // SESSION_TRACK_SYSTEM_VARIABLES and OceanBase Extra Info
			pos2 := 0
			for pos2 < len(val) {
				k, u, _, err := protocol.ReadLengthEncodedString(val[pos2:])
				if err != nil {
					break
				}
				pos2 += u
				v, u2, _, err := protocol.ReadLengthEncodedString(val[pos2:])
				if err != nil {
					break
				}
				pos2 += u2

				c.tracef("session track (type 0x%02x): %s = %s", typ, k, v)
				if string(k) == "ob_capability_flag" || string(k) == "__proxy_capability_flag" {
					capVal, _ := strconv.ParseUint(string(v), 10, 64)
					if capVal&protocol.OBCapOBProtocolV2 != 0 {
						c.ob20Confirmed = true
						c.tracef("OceanBase 2.0 protocol confirmed by server: %s = %d", k, capVal)
					} else {
						c.ob20Declined = true
						c.tracef("OceanBase 2.0 protocol explicitly declined by server: %s = %d", k, capVal)
					}
					if capVal&protocol.OBCapNewExtraInfo != 0 {
						c.ob20NewExtraInfoConfirmed = true
						c.tracef("OceanBase OB20 new extra info confirmed by server: %s = %d", k, capVal)
					}
				}
			}
		case 0x01: // SESSION_TRACK_SCHEMA
			c.db = string(val)
			c.tracef("database change: %s", c.db)
		}
	}
	return nil
}

func (c *Conn) IsOB20() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.packets == nil {
		return false
	}
	return c.packets.IsOB20()
}

func (c *Conn) ConnectionID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.packets == nil {
		return 0
	}
	return c.packets.ConnectionID()
}

func NegotiateCompression(
	optionUseCompression bool,
	serverCapabilityFlags uint32,
	envOverride string,
) bool {
	envClosed := false
	if envOverride != "" {
		switch strings.ToLower(envOverride) {
		case "0", "false", "off", "no":
			envClosed = true
		}
	}
	clientWants := optionUseCompression && !envClosed
	serverSupports := serverCapabilityFlags&protocol.ClientCompress != 0
	return clientWants && serverSupports
}

func isEnvClosed(env string) bool {
	if env != "" {
		switch strings.ToLower(env) {
		case "0", "false", "off", "no":
			return true
		}
	}
	return false
}

func boolToOnOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func negotiationResultLabel(negotiated, client, server bool) string {
	if negotiated {
		return "ENABLED"
	}
	if client && !server {
		return "downgraded to uncompressed"
	}
	if !client && server {
		return "uncompressed (client opted out)"
	}
	return "uncompressed"
}
