package oceanbase

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

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
			return c.markProtocolError(err)
		}
		if len(packet) == 0 {
			return c.markProtocolError(io.ErrUnexpectedEOF)
		}
		if packet[0] == protocol.ErrPacket {
			return c.markProtocolError(parseServerError(packet))
		}
		if packet[0] != protocol.OKPacket {
			return c.markProtocolError(fmt.Errorf("oceanbase: unexpected prepare response 0x%02x", packet[0]))
		}

		if len(packet) < 12 {
			return c.markProtocolError(io.ErrUnexpectedEOF)
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
					return c.markProtocolError(err)
				}
			}
			if !c.deprecateEOF {
				if err := c.readEOFOrOK(); err != nil {
					return err
				}
			}
		}
		if s.columnCount > 0 {
			for i := 0; i < s.columnCount; i++ {
				if _, err := c.packets.ReadPacket(); err != nil {
					return c.markProtocolError(err)
				}
			}
			if !c.deprecateEOF {
				if err := c.readEOFOrOK(); err != nil {
					return err
				}
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
			return c.markProtocolError(err)
		}
		if len(first) == 0 {
			return c.markProtocolError(io.ErrUnexpectedEOF)
		}
		switch first[0] {
		case protocol.OKPacket:
			res, status, err := c.handleOK(first)
			if err != nil {
				return c.markProtocolError(err)
			}
			result = res
			if status&protocol.ServerPSOutParams != 0 {
				if err := c.readOutParams(args); err != nil {
					return err
				}
			}
			if status&protocol.ServerMoreResultsExists != 0 {
				if err := c.drainRemainingResults(); err != nil {
					return err
				}
			}
		case protocol.ErrPacket:
			return c.markProtocolError(parseServerError(first))
		default:
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

func (c *Conn) drainRemainingResults() error {
	first, err := c.packets.ReadPacket()
	if err != nil {
		return c.markProtocolError(err)
	}
	return c.drainResultSetsFromFirstPacket(first)
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
	r.binary = true
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

		const SEND_TYPES_TO_SERVER uint16 = 0x80
		header := make([]byte, 7)
		header[0] = protocol.ComStmtBulkExecute
		binary.LittleEndian.PutUint32(header[1:5], stmtID)
		binary.LittleEndian.PutUint16(header[5:7], SEND_TYPES_TO_SERVER)

		numParams := len(argRows[0])
		paramTypes := make([]byte, numParams*2)
		for i, arg := range argRows[0] {
			val := unwrapOutParam(arg.Value)
			binary.LittleEndian.PutUint16(paramTypes[i*2:i*2+2], uint16(protocol.GetBinaryParamType(val)))
		}

		var payload []byte
		payload = append(payload, header...)
		payload = append(payload, paramTypes...)

		for _, row := range argRows {
			for _, arg := range row {
				val := unwrapOutParam(arg.Value)

				if val == nil {
					payload = append(payload, 1)
				} else {
					payload = append(payload, 0)
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
			return c.markProtocolError(err)
		}
		if len(first) == 0 {
			return c.markProtocolError(io.ErrUnexpectedEOF)
		}
		if first[0] == protocol.ErrPacket {
			return c.markProtocolError(parseServerError(first))
		}
		if first[0] != protocol.OKPacket {
			return c.markProtocolError(fmt.Errorf("oceanbase: unexpected bulk execute response 0x%02x", first[0]))
		}
		res, _, err := c.handleOK(first)
		if err != nil {
			return c.markProtocolError(err)
		}
		result = res
		return nil
	})
	return result, err
}

func (c *Conn) writeExecute(stmtID uint32, args []driver.NamedValue) error {
	c.packets.ResetSequence()
	c.packets.NextRequest()

	payload := make([]byte, 10)
	payload[0] = protocol.ComStmtExecute
	binary.LittleEndian.PutUint32(payload[1:5], stmtID)
	payload[5] = 0
	binary.LittleEndian.PutUint32(payload[6:10], 1)

	if len(args) > 0 {
		nullBitmap := make([]byte, (len(args)+7)/8)
		newParamsBound := byte(1)
		paramTypes := make([]byte, len(args)*2)
		var paramValues []byte

		for i, arg := range args {
			val := unwrapOutParam(arg.Value)

			if val == nil {
				nullBitmap[i/8] |= 1 << (uint(i) % 8)
			}
			typ := protocol.GetBinaryParamType(val)
			paramTypes[i*2] = typ
			paramTypes[i*2+1] = 0

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

func (c *Conn) resetStmt(stmtID uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkUsableLocked(); err != nil {
		return err
	}
	c.packets.ResetSequence()
	c.packets.NextRequest()
	payload := make([]byte, 5)
	payload[0] = protocol.ComStmtReset
	binary.LittleEndian.PutUint32(payload[1:], stmtID)
	if err := c.packets.WritePacket(payload); err != nil {
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
		return c.markProtocolError(fmt.Errorf("oceanbase: unexpected statement reset response 0x%02x", packet[0]))
	}
	return nil
}
