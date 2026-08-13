package oceanbase

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

type Conn struct {
	netConn         net.Conn
	packets         *protocol.PacketConn
	cfg             *Config
	db              string
	ob20Confirmed   bool
	ob20Declined    bool
	deprecateEOF    bool
	sessionTrack    bool
	tenantMode      string
	sessionLocation *time.Location

	mu     sync.Mutex
	closed bool
	bad    atomic.Bool
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

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
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
	err := c.netConn.Close()
	c.netConn = nil
	c.packets = nil
	return err
}

func (c *Conn) closeStmt(stmtID uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.bad.Load() {
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
	return !c.closed && !c.bad.Load()
}

func (c *Conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return err
	}
	if c.inTx {
		c.tracef("reset session: rollback active transaction")
		if _, err := c.execLocked(ctx, "rollback"); err != nil {
			return c.markSessionResetError(err)
		}
		c.inTx = false
	} else {
		c.tracef("reset session: no active transaction")
	}
	if err := c.configureSessionTimeZone(ctx); err != nil {
		return c.markSessionResetError(err)
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return err
	}
	err := c.withDeadline(ctx, func() error {
		c.packets.ResetSequence()
		c.packets.NextRequest()
		if err := c.packets.WritePacket([]byte{protocol.ComPing}); err != nil {
			return err
		}
		packet, err := c.packets.ReadPacket()
		if err != nil {
			return c.markProtocolError(err)
		}
		if len(packet) == 0 {
			return c.markProtocolError(io.ErrUnexpectedEOF)
		}
		if packet[0] == protocol.ErrPacket {
			return c.markProtocolError(parseServerError(packet))
		}
		if packet[0] != protocol.OKPacket {
			return c.markProtocolError(fmt.Errorf("oceanbase: unexpected ping response packet 0x%02x", packet[0]))
		}
		return nil
	})
	return c.markBadIfConnErr(err)
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
	switch v.Value.(type) {
	case sql.Out, *sql.Out:
		return nil
	}
	return driver.ErrSkip
}

func unwrapOutParam(val any) any {
	if out, ok := val.(sql.Out); ok {
		return derefValue(out.Dest)
	}
	if out, ok := val.(*sql.Out); ok {
		return derefValue(out.Dest)
	}
	return val
}

func derefValue(val any) any {
	if val == nil {
		return nil
	}
	v := reflect.ValueOf(val)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	if v.IsValid() {
		return v.Interface()
	}
	return val
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
		if err := c.updateSessionTimeZoneFromQuery(query); err != nil {
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
			return c.markProtocolError(err)
		}
		res, err := c.readResultFromFirstPacket(first)
		if err != nil {
			return err
		}
		result = res
		if err := c.updateSessionTimeZoneFromQuery(query); err != nil {
			return err
		}
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
	if c.closed || c.bad.Load() {
		return driver.ErrBadConn
	}
	return nil
}

func (c *Conn) markBadIfConnErr(err error) error {
	if err == nil {
		return nil
	}
	if isBadConnError(err) {
		c.bad.Store(true)
		return driver.ErrBadConn
	}
	return err
}

func (c *Conn) markProtocolError(err error) error {
	if err == nil {
		return nil
	}
	var serverErr *ServerError
	if errors.As(err, &serverErr) {
		return err
	}
	c.bad.Store(true)
	if errors.Is(err, driver.ErrBadConn) {
		return err
	}
	return fmt.Errorf("oceanbase: protocol error: %v: %w", err, driver.ErrBadConn)
}

func (c *Conn) markSessionResetError(err error) error {
	if err == nil {
		return nil
	}
	c.bad.Store(true)
	if errors.Is(err, driver.ErrBadConn) {
		return err
	}
	return fmt.Errorf("oceanbase: session reset failed: %w: %w", err, driver.ErrBadConn)
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

	var done atomic.Bool
	cancelled := make(chan struct{})
	if ctx.Done() != nil {
		defer close(cancelled)
		go func() {
			select {
			case <-ctx.Done():
				if !done.Load() {
					_ = c.netConn.SetDeadline(time.Now())
				}
			case <-cancelled:
			}
		}()
	}

	err := fn()
	done.Store(true)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			c.bad.Store(true)
			return ctxErr
		}
	}
	return err
}

func (c *Conn) TenantMode() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tenantMode
}

type result struct {
	affectedRows int64
	lastInsertID int64
}

func (r result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r result) RowsAffected() (int64, error) { return r.affectedRows, nil }

func (c *Conn) handleOK(packet []byte) (res driver.Result, status uint16, err error) {
	c.tracef("handleOK packet hex: %x", packet)
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
		if len(packet) < pos+4 {
			return nil, 0, io.ErrUnexpectedEOF
		}
		status = binary.LittleEndian.Uint16(packet[pos : pos+2])
		pos += 2
		pos += 2 // skip warnings
		if status&protocol.ServerSessionStateChanged != 0 {
			// Skip the human-readable info string before session state data.
			// MySQL protocol: after warnings comes a lenenc-string "info",
			// then the lenenc-string session_state (only if SESSION_STATE_CHANGED).
			if pos < len(packet) {
				_, used, _, err := protocol.ReadLengthEncodedString(packet[pos:])
				if err == nil {
					pos += used
				}
			}
			if pos < len(packet) {
				// Read the session_state_changes lenenc-string wrapper.
				// The outer lenenc-string contains: type(1) + lenenc(value) + ...
				trackData, _, _, err := protocol.ReadLengthEncodedString(packet[pos:])
				if err == nil {
					if err := c.handleStateChange(trackData); err != nil {
						c.tracef("failed to parse state change: %v", err)
					}
				}
			}
			c.tracef("session state changed (status=0x%04x)", status)
		}
	}

	return result{affectedRows: int64(affected), lastInsertID: int64(lastID)}, status, nil
}

func (c *Conn) handleStateChange(data []byte) error {
	c.tracef("handleStateChange data hex: %x", data)
	pos := 0
	for pos < len(data) {
		typ := data[pos]
		pos++
		val, used, _, err := protocol.ReadLengthEncodedString(data[pos:])
		if err != nil {
			return err
		}
		pos += used

		switch typ {
		case 0x00, 0x26:
			pos2 := 0
			for pos2 < len(val) {
				varLen, used, _, err := protocol.ReadLengthEncodedInt(val[pos2:])
				if err != nil {
					break
				}
				pos2 += used
				varEnd := pos2 + int(varLen)
				if varEnd > len(val) {
					break
				}
				k, kUsed, _, err := protocol.ReadLengthEncodedString(val[pos2:varEnd])
				if err != nil {
					break
				}
				pos2 += kUsed
				v, vUsed, _, err := protocol.ReadLengthEncodedString(val[pos2:varEnd])
				if err != nil {
					break
				}
				pos2 += vUsed

				c.tracef("session track (type 0x%02x): %s = %s", typ, string(k), string(v))
				if string(k) == "ob_capability_flag" || string(k) == "__proxy_capability_flag" {
					capVal, _ := strconv.ParseUint(string(v), 10, 64)
					if capVal&protocol.OBCapOBProtocolV2 != 0 {
						c.ob20Confirmed = true
						c.tracef("OceanBase 2.0 protocol confirmed by server: %s = %s", string(k), string(v))
					} else {
						c.ob20Declined = true
						c.tracef("OceanBase 2.0 protocol explicitly declined by server: %s = %s", string(k), string(v))
					}
				}
				if pos2 < varEnd {
					pos2 = varEnd
				}
			}
		case 0x01:
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
