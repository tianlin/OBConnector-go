package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
)

// SeqError is returned when a packet's sequence number doesn't match expectations.
// It implements net.Error so that the driver marks the connection as bad.
type SeqError struct {
	Got  byte
	Want byte
}

func (e *SeqError) Error() string {
	return fmt.Sprintf("unexpected packet sequence: got %d, want %d", e.Got, e.Want)
}

func (e *SeqError) Timeout() bool   { return false }
func (e *SeqError) Temporary() bool { return false }

var _ net.Error = (*SeqError)(nil)

const maxPayloadLen = 1<<24 - 1

var bufPool = sync.Pool{
	New: func() any {
		return make([]byte, 4096)
	},
}

func getBuf(size int) []byte {
	if size > 4096 {
		return make([]byte, size)
	}
	buf := bufPool.Get().([]byte)
	return buf[:size]
}

func putBuf(buf []byte) {
	if cap(buf) == 4096 {
		bufPool.Put(buf[:4096])
	}
}

type PacketConn struct {
	rw           io.ReadWriter
	seq          byte
	ob20         bool
	ob20Magic    uint16
	connectionID uint32
	requestID    uint32
	extraInfos   []OB20ExtraInfo
	mysqlBuf     []byte
}

func NewPacketConn(rw io.ReadWriter) *PacketConn {
	return &PacketConn{rw: rw}
}

func (c *PacketConn) ResetSequence() {
	c.seq = 0
}

func (c *PacketConn) EnableOB20(connectionID uint32, magic uint16) {
	c.ob20 = true
	c.ob20Magic = magic
	c.connectionID = connectionID
}

func (c *PacketConn) AddExtraInfo(typ uint16, data []byte) {
	c.extraInfos = append(c.extraInfos, OB20ExtraInfo{Type: typ, Data: data})
}

func (c *PacketConn) ClearExtraInfo() {
	c.extraInfos = nil
}

func (c *PacketConn) NextRequest() {
	c.requestID++
}

func (c *PacketConn) ReadPacket() ([]byte, error) {
	var out []byte
	for {
		if c.ob20 {
			// Step A: try to pop a MySQL packet from c.mysqlBuf first
			if len(c.mysqlBuf) >= 4 {
				mysqlLen := int(c.mysqlBuf[0]) | int(c.mysqlBuf[1])<<8 | int(c.mysqlBuf[2])<<16
				if len(c.mysqlBuf) >= 4+mysqlLen {
					payload := c.mysqlBuf[4 : 4+mysqlLen]
					out = append(out, payload...)
					c.mysqlBuf = c.mysqlBuf[4+mysqlLen:]

					if mysqlLen < maxPayloadLen {
						return out, nil
					}
					continue
				}
			}

			// Step B: read one OB20 frame from c.rw
			var obHeader [TotalHeaderLen]byte
			if _, err := io.ReadFull(c.rw, obHeader[:]); err != nil {
				return nil, err
			}
			var h OB20Header
			if !h.Decode(obHeader[:]) {
				return nil, fmt.Errorf("invalid OB 2.0 header")
			}

			obPayload := make([]byte, h.PayloadLen)
			if _, err := io.ReadFull(c.rw, obPayload); err != nil {
				return nil, err
			}

			var obTrailer [4]byte
			if _, err := io.ReadFull(c.rw, obTrailer[:]); err != nil {
				return nil, err
			}

			expectedChecksum := binary.LittleEndian.Uint32(obTrailer[:])
			if OB20PayloadChecksum(obPayload) != expectedChecksum {
				return nil, fmt.Errorf("invalid OB 2.0 payload checksum: expected 0x%08x, got 0x%08x", expectedChecksum, OB20PayloadChecksum(obPayload))
			}

			// Parse extra info if exists
			mysqlData := obPayload
			if h.Flag&OB20FlagExtraInfo != 0 {
				if len(mysqlData) < OB20ExtraLengthField {
					return nil, fmt.Errorf("truncated extra_length in OB20 payload")
				}
				extraLength := binary.LittleEndian.Uint32(mysqlData[0:4])
				extraTotal := OB20ExtraLengthField + int(extraLength)
				if len(mysqlData) < extraTotal {
					return nil, fmt.Errorf("truncated extra_info: declared %d bytes, payload has %d", extraTotal, len(mysqlData))
				}
				mysqlData = mysqlData[extraTotal:]
			}

			c.mysqlBuf = append(c.mysqlBuf, mysqlData...)

		} else {
			var header [4]byte
			if _, err := io.ReadFull(c.rw, header[:]); err != nil {
				return nil, err
			}

			payloadLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
			gotSeq := header[3]
			if gotSeq != c.seq {
				return nil, &SeqError{Got: gotSeq, Want: c.seq}
			}
			c.seq++

			payload := make([]byte, payloadLen)
			if _, err := io.ReadFull(c.rw, payload); err != nil {
				return nil, err
			}
			out = append(out, payload...)
			if payloadLen < maxPayloadLen {
				return out, nil
			}
		}
	}
}

func (c *PacketConn) WritePacket(payload []byte) error {
	for {
		chunkLen := len(payload)
		if chunkLen > maxPayloadLen {
			chunkLen = maxPayloadLen
		}

		mysqlLen := 4 + chunkLen
		extraLen := 0
		if c.ob20 {
			for _, info := range c.extraInfos {
				extraLen += info.TotalLen()
			}
		}

		var payloadBuf []byte
		if c.ob20 && extraLen > 0 {
			payloadBuf = make([]byte, 4+extraLen+mysqlLen)
			binary.LittleEndian.PutUint32(payloadBuf[0:4], uint32(extraLen))
			pos := 4
			for _, info := range c.extraInfos {
				pos += info.Encode(payloadBuf[pos:])
			}
			// MySQL packet
			mysqlPos := 4 + extraLen
			payloadBuf[mysqlPos] = byte(chunkLen)
			payloadBuf[mysqlPos+1] = byte(chunkLen >> 8)
			payloadBuf[mysqlPos+2] = byte(chunkLen >> 16)
			payloadBuf[mysqlPos+3] = c.seq
			copy(payloadBuf[mysqlPos+4:], payload[:chunkLen])
		} else {
			payloadBuf = make([]byte, mysqlLen)
			payloadBuf[0] = byte(chunkLen)
			payloadBuf[1] = byte(chunkLen >> 8)
			payloadBuf[2] = byte(chunkLen >> 16)
			payloadBuf[3] = c.seq
			copy(payloadBuf[4:], payload[:chunkLen])
		}
		c.seq++

		if c.ob20 {
			payloadLen := uint32(len(payloadBuf))
			compressLength := uint32(TotalHeaderLen - CompressHeaderLen + len(payloadBuf) + OB20TailLen) // 24 + payloadLen + 4

			flag := OB20FlagLast
			if extraLen > 0 {
				flag |= OB20FlagExtraInfo
			}
			flag |= OB20FlagNewExtraInfo

			h := OB20Header{
				CompressLength:   compressLength,
				CompressSeqNo:    c.seq - 1,
				UncompressLength: 0,
				MagicNum:         c.ob20Magic,
				Version:          OB20Version,
				ConnectionID:     c.connectionID,
				RequestID:        c.requestID & 0x00FFFFFF,
				PacketSeq:        c.seq - 1,
				PayloadLen:       payloadLen,
				Flag:             flag,
				Reserved:         0,
			}

			var obHeaderBuf [TotalHeaderLen]byte
			h.Encode(obHeaderBuf[:])

			if _, err := c.rw.Write(obHeaderBuf[:]); err != nil {
				return err
			}
			if _, err := c.rw.Write(payloadBuf); err != nil {
				return err
			}

			tail := OB20PayloadChecksum(payloadBuf)
			var obTrailer [4]byte
			binary.LittleEndian.PutUint32(obTrailer[:], tail)
			if _, err := c.rw.Write(obTrailer[:]); err != nil {
				return err
			}
		} else {
			if _, err := c.rw.Write(payloadBuf); err != nil {
				return err
			}
		}

		payload = payload[chunkLen:]
		if chunkLen < maxPayloadLen {
			return nil
		}
		if len(payload) == 0 {
			return c.writeEmptyContinuation()
		}
	}
}

func (c *PacketConn) writeEmptyContinuation() error {
	mysqlHeader := make([]byte, 4)
	mysqlHeader[0] = 0
	mysqlHeader[1] = 0
	mysqlHeader[2] = 0
	mysqlHeader[3] = c.seq
	c.seq++

	if c.ob20 {
		payloadLen := uint32(len(mysqlHeader))
		compressLength := uint32(TotalHeaderLen - CompressHeaderLen + len(mysqlHeader) + OB20TailLen) // 24 + 4 + 4 = 32

		h := OB20Header{
			CompressLength:   compressLength,
			CompressSeqNo:    mysqlHeader[3],
			UncompressLength: 0,
			MagicNum:         c.ob20Magic,
			Version:          OB20Version,
			ConnectionID:     c.connectionID,
			RequestID:        c.requestID & 0x00FFFFFF,
			PacketSeq:        mysqlHeader[3],
			PayloadLen:       payloadLen,
			Flag:             OB20FlagLast | OB20FlagNewExtraInfo,
			Reserved:         0,
		}

		var obHeaderBuf [TotalHeaderLen]byte
		h.Encode(obHeaderBuf[:])

		if _, err := c.rw.Write(obHeaderBuf[:]); err != nil {
			return err
		}
		if _, err := c.rw.Write(mysqlHeader); err != nil {
			return err
		}

		tail := OB20PayloadChecksum(mysqlHeader)
		var obTrailer [4]byte
		binary.LittleEndian.PutUint32(obTrailer[:], tail)
		_, err := c.rw.Write(obTrailer[:])
		return err
	}

	_, err := c.rw.Write(mysqlHeader)
	return err
}

func (c *PacketConn) IsOB20() bool {
	return c.ob20
}

func (c *PacketConn) ConnectionID() uint32 {
	return c.connectionID
}
