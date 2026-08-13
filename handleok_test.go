package oceanbase

import (
	"encoding/binary"
	"testing"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

// buildOKPacketWithSessionTrack constructs a MySQL OK packet with session track info.
// layout: 0x00 | affected_rows(lenenc) | last_insert_id(lenenc) | status(2) | warnings(2) | info(lenenc) | session_track(lenenc)
func buildOKPacketWithSessionTrack(affectedRows, lastInsertID uint64, status, warnings uint16, info string, sessionTrack []byte) []byte {
	var pkt []byte
	pkt = append(pkt, 0x00) // OK header
	pkt = protocol.PutLengthEncodedInt(pkt, affectedRows)
	pkt = protocol.PutLengthEncodedInt(pkt, lastInsertID)
	pkt = binary.LittleEndian.AppendUint16(pkt, status)
	pkt = binary.LittleEndian.AppendUint16(pkt, warnings)
	// info field (lenenc string)
	pkt = protocol.PutLengthEncodedString(pkt, info)
	// session state change (lenenc string)
	pkt = protocol.PutLengthEncodedString(pkt, string(sessionTrack))
	return pkt
}

// buildSessionTrackUnit builds a single session track unit.
// type(1) | value(lenenc string)
// Inside value for type 0x00: var_len(lenenc) | key(lenenc) | value(lenenc)
func buildSessionTrackUnit(unitType byte, key, value string) []byte {
	var inner []byte
	inner = protocol.PutLengthEncodedInt(inner, uint64(len(key)+len(value)+2+2)) // approximate
	// Recalculate properly
	inner = nil
	keyEnc := protocol.PutLengthEncodedString(nil, key)
	valEnc := protocol.PutLengthEncodedString(nil, value)
	varLenBuf := protocol.PutLengthEncodedInt(nil, uint64(len(keyEnc)+len(valEnc)))
	inner = append(inner, varLenBuf...)
	inner = append(inner, keyEnc...)
	inner = append(inner, valEnc...)

	var unit []byte
	unit = append(unit, unitType)
	unit = protocol.PutLengthEncodedString(unit, string(inner))
	return unit
}

func TestHandleOKSkipsInfoField(t *testing.T) {
	// Build a session track with ob_capability_flag
	obCapFlagStr := "256" // OB_CAP_OB_PROTOCOL_V2 = 1<<8 = 256
	trackUnit := buildSessionTrackUnit(0x00, "ob_capability_flag", obCapFlagStr)

	// Build OK packet with non-empty info field and session track
	status := uint16(protocol.ServerSessionStateChanged)
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "some info text", trackUnit)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Timeout:    0,
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	res, parsedStatus, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}
	if parsedStatus&protocol.ServerSessionStateChanged == 0 {
		t.Error("expected ServerSessionStateChanged flag in status")
	}
	if !conn.ob20Confirmed {
		t.Error("expected ob20Confirmed to be true after parsing ob_capability_flag with OB_CAP_OB_PROTOCOL_V2")
	}
}

func TestHandleOKDeclinesOB20(t *testing.T) {
	// Build session track with ob_capability_flag that does NOT have OB_CAP_OB_PROTOCOL_V2
	obCapFlagStr := "0" // no OB20 flag
	trackUnit := buildSessionTrackUnit(0x00, "ob_capability_flag", obCapFlagStr)

	status := uint16(protocol.ServerSessionStateChanged)
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "", trackUnit)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	_, _, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if conn.ob20Confirmed {
		t.Error("expected ob20Confirmed to be false when OB_CAP_OB_PROTOCOL_V2 not set")
	}
	if !conn.ob20Declined {
		t.Error("expected ob20Declined to be true when server explicitly declines OB20")
	}
}

func TestHandleOKProxyCapabilityFlag(t *testing.T) {
	obCapFlagStr := "256" // OB_CAP_OB_PROTOCOL_V2
	trackUnit := buildSessionTrackUnit(0x26, "__proxy_capability_flag", obCapFlagStr)

	status := uint16(protocol.ServerSessionStateChanged)
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "", trackUnit)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	_, _, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if !conn.ob20Confirmed {
		t.Error("expected ob20Confirmed via __proxy_capability_flag")
	}
}

func TestHandleStateChangeMultipleVars(t *testing.T) {
	// Build session track with both ob_capability_flag and another system variable
	unit1 := buildSessionTrackUnit(0x00, "ob_capability_flag", "256")
	unit2 := buildSessionTrackUnit(0x00, "character_set_client", "utf8mb4")

	var trackData []byte
	trackData = append(trackData, unit1...)
	trackData = append(trackData, unit2...)

	status := uint16(protocol.ServerSessionStateChanged)
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "info", trackData)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	_, _, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if !conn.ob20Confirmed {
		t.Error("expected ob20Confirmed with multiple session track vars")
	}
}

func TestHandleOKNoSessionTrack(t *testing.T) {
	// OK packet without ServerSessionStateChanged - no session track data
	var pkt []byte
	pkt = append(pkt, 0x00)
	pkt = protocol.PutLengthEncodedInt(pkt, 42)
	pkt = protocol.PutLengthEncodedInt(pkt, 1)
	pkt = binary.LittleEndian.AppendUint16(pkt, 0x0002) // autocommit
	pkt = binary.LittleEndian.AppendUint16(pkt, 0)      // warnings

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	res, status, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if res.(result).affectedRows != 42 {
		t.Errorf("expected affectedRows=42, got %d", res.(result).affectedRows)
	}
	if conn.ob20Confirmed || conn.ob20Declined {
		t.Error("expected no OB20 state change when no session track")
	}
	_ = status
}

func TestHandleOKLargeOB20CapValue(t *testing.T) {
	// Test with a combined OB capability flag value including OB_PROTOCOL_V2
	obCapFlagStr := "1360128" // OB_CAP_OB_PROTOCOL_V2 + FULL_LINK_TRACE + NEW_EXTRA_INFO + SHOW_TRACE + LOCAL_FILES
	trackUnit := buildSessionTrackUnit(0x00, "ob_capability_flag", obCapFlagStr)

	status := uint16(protocol.ServerSessionStateChanged)
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "", trackUnit)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	_, _, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if !conn.ob20Confirmed {
		t.Error("expected ob20Confirmed with large cap value containing OB_PROTOCOL_V2")
	}
}

func TestHandleOKWithEmptyInfo(t *testing.T) {
	obCapFlagStr := "256"
	trackUnit := buildSessionTrackUnit(0x00, "ob_capability_flag", obCapFlagStr)

	status := uint16(protocol.ServerSessionStateChanged)
	// Empty info string
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "", trackUnit)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	_, _, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if !conn.ob20Confirmed {
		t.Error("expected ob20Confirmed with empty info field")
	}
}

func TestHandleOKRejectsTruncatedStatusWithoutPanicking(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("handleOK panicked on a truncated OK packet: %v", recovered)
		}
	}()

	// affected_rows and last_insert_id are valid, but only one byte of the
	// required status/warnings section remains.
	pkt := []byte{protocol.OKPacket, 0x00, 0x00, 0x01}
	if _, _, err := (&Conn{}).handleOK(pkt); err == nil {
		t.Fatal("handleOK() error = nil, want truncated-packet error")
	}
}

func TestHandleOKSchemaChangeTrackType(t *testing.T) {
	// Test session track type 0x01 (schema change)
	// Type 0x01 format: type(1) + lenenc-string(schema_name)
	var schemaUnit []byte
	schemaUnit = append(schemaUnit, 0x01)
	schemaUnit = protocol.PutLengthEncodedString(schemaUnit, "test_db")

	capUnit := buildSessionTrackUnit(0x00, "ob_capability_flag", "256")

	var trackData []byte
	trackData = append(trackData, schemaUnit...)
	trackData = append(trackData, capUnit...)

	status := uint16(protocol.ServerSessionStateChanged)
	pkt := buildOKPacketWithSessionTrack(0, 0, status, 0, "", trackData)

	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	_ = cfg.normalize()
	conn := &Conn{cfg: cfg}

	_, _, err := conn.handleOK(pkt)
	if err != nil {
		t.Fatalf("handleOK failed: %v", err)
	}
	if conn.db != "test_db" {
		t.Errorf("expected db=test_db, got %q", conn.db)
	}
	if !conn.ob20Confirmed {
		t.Error("expected ob20Confirmed after schema change + capability flag track")
	}
}

func TestAttributeKeyEncodingOneBytePrefix(t *testing.T) {
	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "test",
		Password:   "test",
		Timeout:    0,
		Attributes: map[string]string{},
		ProtocolV2: true,
	}
	if err := cfg.normalize(); err != nil {
		t.Fatal(err)
	}
	conn := &Conn{cfg: cfg}
	hs := &handshake{
		serverVersion: "5.6.25",
		connectionID:  42,
		capabilities: protocol.ClientLongPassword |
			protocol.ClientLongFlag |
			protocol.ClientProtocol41 |
			protocol.ClientTransactions |
			protocol.ClientSecureConnection |
			protocol.ClientMultiResults |
			protocol.ClientPluginAuth |
			protocol.ClientConnectAttrs |
			protocol.ClientSessionTrack,
		authPlugin: "mysql_native_password",
		authSeed:   []byte("12345678901234567890"),
	}

	authResp, err := buildAuthResponse(hs.authPlugin, cfg.Password, hs.authSeed)
	if err != nil {
		t.Fatal(err)
	}
	response := conn.buildHandshakeResponse(hs, authResp)

	// Verify that the attribute section uses 1-byte prefix for keys.
	// Find the "ob_capability_flag" key in the response - it should be preceded by
	// a single byte giving its length (17 bytes -> 0x11).
	obCapFlag := []byte("ob_capability_flag")
	idx := findBytes(response, obCapFlag)
	if idx < 0 {
		t.Fatal("ob_capability_flag not found in response")
	}
	// The byte before the key should be the 1-byte length prefix
	if response[idx-1] != byte(len(obCapFlag)) {
		t.Errorf("expected 1-byte key prefix = %d, got %d (byte 0x%02x)",
			len(obCapFlag), response[idx-1], response[idx-1])
	}
}

func findBytes(data, pattern []byte) int {
	for i := 0; i <= len(data)-len(pattern); i++ {
		match := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
