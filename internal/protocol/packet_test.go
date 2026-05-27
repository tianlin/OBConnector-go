package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

type mockRW struct {
	*bytes.Buffer
}

func newMockRW() *mockRW {
	return &mockRW{Buffer: new(bytes.Buffer)}
}

func (m *mockRW) Read(p []byte) (int, error) {
	return m.Buffer.Read(p)
}

func (m *mockRW) Write(p []byte) (int, error) {
	return m.Buffer.Write(p)
}

// writeAndRead writes a packet (with ResetSequence before the write, and again
// before the read to simulate a fresh server response), then reads back.
func writeAndRead(pc *PacketConn, data []byte) ([]byte, error) {
	pc.ResetSequence()
	if err := pc.WritePacket(data); err != nil {
		return nil, err
	}
	pc.ResetSequence()
	return pc.ReadPacket()
}

func TestBasicReadWritePacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	data, err := writeAndRead(pc, []byte("hello"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", string(data), "hello")
	}
}

func TestPacketSequenceMismatch(t *testing.T) {
	pc := NewPacketConn(newMockRW())

	// Manually write a packet with wrong sequence number
	header := []byte{0x04, 0x00, 0x00, 0xFF} // len=4, seq=255
	pc.rw.Write(header)
	pc.rw.Write([]byte("test"))

	_, err := pc.ReadPacket()
	if err == nil {
		t.Fatal("expected sequence mismatch error")
	}
}

func TestPacketSequenceResetBeforeWrite(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	// Write seq=0, reset, write seq=0 again — buffer has two seq=0 packets
	_ = pc.WritePacket([]byte("pkt1"))
	pc.ResetSequence()
	_ = pc.WritePacket([]byte("pkt2"))

	// Reset sequence before read so c.seq matches the seq=0 in the packets
	pc.ResetSequence()

	// ReadPacket picks "pkt1" (first in buffer) since both have seq=0
	data, err := pc.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "pkt1" {
		t.Fatalf("got %q, want %q", string(data), "pkt1")
	}

	// Reading again gives "pkt2" — seq after first read is 1, but
	// pkt2 also has seq=0, so we need another reset.
	// This is expected: ResetSequence is used between requests.
	pc.ResetSequence()
	data, err = pc.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "pkt2" {
		t.Fatalf("got %q, want %q", string(data), "pkt2")
	}
}

func TestOB20ReadWritePacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	data, err := writeAndRead(pc, []byte("ob20 payload"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "ob20 payload" {
		t.Fatalf("got %q, want %q", string(data), "ob20 payload")
	}
}

func TestOB20ExtraInfoPacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	pc.AddExtraInfo(OB20ExtraInfoTypePartitionID, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01})
	data, err := writeAndRead(pc, []byte("data with partition info"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "data with partition info" {
		t.Fatalf("got %q, want %q", string(data), "data with partition info")
	}
}

func TestLargePacketSplit(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	largeData := make([]byte, maxPayloadLen+100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	data, err := writeAndRead(pc, largeData)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(data) != len(largeData) {
		t.Fatalf("len = %d, want %d", len(data), len(largeData))
	}
	for i := range largeData {
		if data[i] != largeData[i] {
			t.Fatalf("byte %d differs: got 0x%02x, want 0x%02x", i, data[i], largeData[i])
		}
	}
}

func TestOB20LargePacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	largeData := make([]byte, maxPayloadLen+100)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	data, err := writeAndRead(pc, largeData)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	if len(data) != len(largeData) {
		t.Fatalf("len = %d, want %d", len(data), len(largeData))
	}
	for i := range largeData {
		if data[i] != largeData[i] {
			t.Fatalf("byte %d differs: got 0x%02x, want 0x%02x", i, data[i], largeData[i])
		}
	}
}

func TestReadPacketTruncated(t *testing.T) {
	mock := newMockRW()
	// Write only 2 bytes of the 4-byte header
	mock.Write([]byte{0x05, 0x00})

	pc := NewPacketConn(mock)
	_, err := pc.ReadPacket()
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected io.ErrUnexpectedEOF, got %v", err)
	}
}

func TestMultipleSequentialWriteAndRead(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	for i := 0; i < 10; i++ {
		data, err := writeAndRead(pc, []byte{byte('A' + i)})
		if err != nil {
			t.Fatalf("iteration %d error: %v", i, err)
		}
		if len(data) != 1 || data[0] != byte('A'+i) {
			t.Fatalf("iteration %d: got %v, want %c", i, data, 'A'+i)
		}
	}
}

func TestReadPacketExactMaxPayload(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	data := make([]byte, maxPayloadLen)
	for i := range data {
		data[i] = byte(i)
	}

	read, err := writeAndRead(pc, data)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(read) != maxPayloadLen {
		t.Fatalf("len = %d, want %d", len(read), maxPayloadLen)
	}
}

func TestClearExtraInfo(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	pc.AddExtraInfo(OB20ExtraInfoTypePartitionID, []byte{0x01})
	pc.ClearExtraInfo()
	pc.AddExtraInfo(OB20ExtraInfoTypePartitionID, []byte{0x02})

	data, err := writeAndRead(pc, []byte("test"))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if string(data) != "test" {
		t.Fatalf("got %q, want %q", string(data), "test")
	}
}

func TestNextRequestIncrement(t *testing.T) {
	pc := NewPacketConn(newMockRW())
	pc.EnableOB20(1, OB20MagicNum)

	id1 := pc.requestID
	pc.NextRequest()
	if pc.requestID != id1+1 {
		t.Fatalf("requestID: %d -> %d, expected increment", id1, pc.requestID)
	}
}

func TestEmptyPacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	data, err := writeAndRead(pc, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("len = %d, want 0", len(data))
	}
}

func TestPacketIdentity(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)

	payloads := [][]byte{
		{},
		{0x00},
		{0xFF, 0xFE, 0xFD},
		bytes.Repeat([]byte{0xAB}, 1000),
	}

	for idx, payload := range payloads {
		data, err := writeAndRead(pc, payload)
		if err != nil {
			t.Fatalf("payload %d error: %v", idx, err)
		}
		if !bytes.Equal(data, payload) {
			t.Fatalf("payload %d: length mismatch: got %d, want %d", idx, len(data), len(payload))
			t.Fatalf("payload %d: got %v, want %v", idx, data, payload)
		}
	}
}

func TestOB20MultiPacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	mysqlPackets := []byte{}
	mysqlPackets = append(mysqlPackets, 5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o')
	mysqlPackets = append(mysqlPackets, 5, 0, 0, 1, 'w', 'o', 'r', 'l', 'd')
	mysqlPackets = append(mysqlPackets, 5, 0, 0, 2, 'o', 'c', 'e', 'a', 'n')

	payloadLen := uint32(len(mysqlPackets))
	compressLength := uint32(TotalHeaderLen - CompressHeaderLen + len(mysqlPackets) + OB20TailLen)

	h := OB20Header{
		CompressLength:   compressLength,
		CompressSeqNo:    0,
		UncompressLength: 0,
		MagicNum:         OB20MagicNum,
		Version:          OB20Version,
		ConnectionID:     1,
		RequestID:        pc.requestID,
		PacketSeq:        0,
		PayloadLen:       payloadLen,
		Flag:             OB20FlagLast | OB20FlagNewExtraInfo,
		Reserved:         0,
	}

	var obHeaderBuf [TotalHeaderLen]byte
	h.Encode(obHeaderBuf[:])

	tail := OB20PayloadChecksum(mysqlPackets)
	var obTrailer [4]byte
	binary.LittleEndian.PutUint32(obTrailer[:], tail)

	mock.Write(obHeaderBuf[:])
	mock.Write(mysqlPackets)
	mock.Write(obTrailer[:])

	p1, err := pc.ReadPacket()
	if err != nil {
		t.Fatalf("first read error: %v", err)
	}
	if string(p1) != "hello" {
		t.Errorf("got %q, want %q", string(p1), "hello")
	}

	p2, err := pc.ReadPacket()
	if err != nil {
		t.Fatalf("second read error: %v", err)
	}
	if string(p2) != "world" {
		t.Errorf("got %q, want %q", string(p2), "world")
	}

	p3, err := pc.ReadPacket()
	if err != nil {
		t.Fatalf("third read error: %v", err)
	}
	if string(p3) != "ocean" {
		t.Errorf("got %q, want %q", string(p3), "ocean")
	}
}

func TestOB20SplitPacket(t *testing.T) {
	mock := newMockRW()
	pc := NewPacketConn(mock)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	p1 := []byte{11, 0, 0, 0, 'h', 'e', 'l', 'l'}
	h1 := OB20Header{
		CompressLength: uint32(TotalHeaderLen - CompressHeaderLen + len(p1) + OB20TailLen),
		MagicNum:       OB20MagicNum,
		Version:        OB20Version,
		ConnectionID:   1,
		RequestID:      pc.requestID,
		PayloadLen:     uint32(len(p1)),
		Flag:           OB20FlagNewExtraInfo,
	}
	var header1 [TotalHeaderLen]byte
	h1.Encode(header1[:])
	tail1 := OB20PayloadChecksum(p1)
	var trailer1 [4]byte
	binary.LittleEndian.PutUint32(trailer1[:], tail1)

	mock.Write(header1[:])
	mock.Write(p1)
	mock.Write(trailer1[:])

	p2 := []byte{'o', ' ', 'w', 'o', 'r', 'l', 'd'}
	h2 := OB20Header{
		CompressLength: uint32(TotalHeaderLen - CompressHeaderLen + len(p2) + OB20TailLen),
		MagicNum:       OB20MagicNum,
		Version:        OB20Version,
		ConnectionID:   1,
		RequestID:      pc.requestID,
		PayloadLen:     uint32(len(p2)),
		Flag:           OB20FlagLast | OB20FlagNewExtraInfo,
	}
	var header2 [TotalHeaderLen]byte
	h2.Encode(header2[:])
	tail2 := OB20PayloadChecksum(p2)
	var trailer2 [4]byte
	binary.LittleEndian.PutUint32(trailer2[:], tail2)

	mock.Write(header2[:])
	mock.Write(p2)
	mock.Write(trailer2[:])

	data, err := pc.ReadPacket()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q, want %q", string(data), "hello world")
	}
}

func TestOB20FragmentedArrivalResumesWhenComplete(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	pc := NewPacketConn(client)
	pc.EnableOB20(1, OB20MagicNum)
	pc.NextRequest()

	mysqlPayload := []byte{5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'}
	payloadLen := uint32(len(mysqlPayload))
	compressLength := uint32(TotalHeaderLen - CompressHeaderLen + len(mysqlPayload) + OB20TailLen)

	h := OB20Header{
		CompressLength:   compressLength,
		CompressSeqNo:    0,
		UncompressLength: 0,
		MagicNum:         OB20MagicNum,
		Version:          OB20Version,
		ConnectionID:     1,
		RequestID:        pc.requestID,
		PacketSeq:        0,
		PayloadLen:       payloadLen,
		Flag:             OB20FlagLast | OB20FlagNewExtraInfo,
		Reserved:         0,
	}

	var obHeaderBuf [TotalHeaderLen]byte
	h.Encode(obHeaderBuf[:])
	tail := OB20PayloadChecksum(mysqlPayload)
	var obTrailer [4]byte
	binary.LittleEndian.PutUint32(obTrailer[:], tail)

	go func() {
		server.Write(obHeaderBuf[:15])
		time.Sleep(10 * time.Millisecond)

		server.Write(obHeaderBuf[15:])
		server.Write(mysqlPayload)
		time.Sleep(10 * time.Millisecond)

		server.Write(obTrailer[:])
	}()

	data, err := pc.ReadPacket()
	if err != nil {
		t.Fatalf("ReadPacket error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q, want %q", string(data), "hello")
	}
}
