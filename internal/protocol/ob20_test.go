package protocol

import (
	"reflect"
	"testing"
)

func TestOB20Header(t *testing.T) {
	h := OB20Header{
		CompressLength: 128,
		CompressSeqNo:  1,
		MagicNum:       OB20MagicNum,
		Version:        OB20Version,
		ConnectionID:   12345,
		RequestID:      67890,
		PacketSeq:      1,
		PayloadLen:     100,
		Flag:           OB20FlagNone,
		Reserved:       0,
	}

	var buf [TotalHeaderLen]byte
	h.Encode(buf[:])

	var h2 OB20Header
	if !h2.Decode(buf[:]) {
		t.Fatal("failed to decode OB 2.0 header")
	}

	if h.MagicNum != h2.MagicNum || h.Version != h2.Version || h.ConnectionID != h2.ConnectionID ||
		h.RequestID != h2.RequestID || h.PacketSeq != h2.PacketSeq || h.PayloadLen != h2.PayloadLen ||
		h.Flag != h2.Flag || h.CompressLength != h2.CompressLength || h.CompressSeqNo != h2.CompressSeqNo {
		t.Errorf("decoded header mismatch: %+v vs %+v", h, h2)
	}
}

func TestOB20ExtraInfo(t *testing.T) {
	info := OB20ExtraInfo{
		Type: OB20ExtraInfoTypePartitionID,
		Data: []byte{0x01, 0x02, 0x03, 0x04},
	}
	buf := make([]byte, info.TotalLen())
	n := info.Encode(buf)
	if n != 10 {
		t.Errorf("expected length 10, got %d", n)
	}

	infos, err := ParseOB20ExtraInfo(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Type != info.Type || !reflect.DeepEqual(infos[0].Data, info.Data) {
		t.Errorf("decoded info mismatch: %+v", infos)
	}
}

func TestCRC16IBMMatchesCatalog(t *testing.T) {
	// 守护测试：table[1] 必须等于 0xC0C1（CRC-16/ARC catalog 标志性常量）
	if crc16IBMTable[1] != 0xC0C1 {
		t.Errorf("expected crc16IBMTable[1] == 0xC0C1, got 0x%04X", crc16IBMTable[1])
	}

	// CRC-16/ARC catalog check vector
	data := []byte("123456789")
	crc := CRC16(data)
	if crc != 0xBB3D {
		t.Errorf("expected CRC16-IBM check vector to be 0xBB3D, got 0x%04X", crc)
	}
}

func TestOBCRC32C(t *testing.T) {
	// 移植指南 §3.5 要求的 4 个验收测试向量
	cases := []struct {
		name     string
		input    []byte
		expected uint32
	}{
		{"empty", []byte{}, 0x00000000},
		{"0x01", []byte{0x01}, 0xF26B8303},
		{"0x02", []byte{0x02}, 0xE13B70F7},
		{"0x03", []byte{0x03}, 0x1350F3F4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checksum := OB20PayloadChecksum(tc.input)
			if checksum != tc.expected {
				t.Errorf("expected OB CRC32C(%s) = 0x%08X, got 0x%08X", tc.name, tc.expected, checksum)
			}
		})
	}
}
