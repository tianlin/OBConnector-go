package oceanbase

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

func TestHandshakeResponseIncludesOceanBaseOracleExtensions(t *testing.T) {
	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "sys@tenant#cluster",
		Password:   "test-password",
		Timeout:    time.Second,
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
	caps := binary.LittleEndian.Uint32(response[:4])
	if caps&protocol.ClientSupportOracleMode == 0 {
		t.Fatalf("CLIENT_SUPPORT_ORACLE_MODE missing from %#x", caps)
	}
	if caps&protocol.ClientSessionTrack == 0 {
		t.Fatalf("CLIENT_SESSION_TRACK missing from %#x", caps)
	}

	// Verify the 23-byte reserved field layout
	// Bytes 9-28 should be all 0x00 (19 bytes)
	for i := 9; i < 28; i++ {
		if response[i] != 0 {
			t.Errorf("expected reserved byte at index %d to be 0x00, got 0x%02x", i, response[i])
		}
	}
	// Bytes 28-32 should be lower 32-bit of ob_capability in LE
	extendedCaps := binary.LittleEndian.Uint32(response[28:32])
	expectedExtended := uint32(obClientCapOBProtocolV2 |
		obClientCapFullLinkTrace |
		obClientCapProxyNewExtraInfo |
		obClientCapProxyFullLinkTraceShowTrace)
	if extendedCaps != expectedExtended {
		t.Errorf("expected extended capability in reserved bytes to be %d, got %d", expectedExtended, extendedCaps)
	}

	requiredAttrs := [][]byte{
		[]byte("__mysql_client_type"),
		[]byte("__ob_jdbc_client"),
		[]byte("__ob_client_name"),
		[]byte("OceanBase JDBC Driver"),
		[]byte("ob_capability_flag"),
		[]byte("__proxy_capability_flag"),
		[]byte("__ob_client_attribute_capability_flag"),
	}
	for _, attr := range requiredAttrs {
		if !bytes.Contains(response, attr) {
			t.Fatalf("handshake response missing attr fragment %q", attr)
		}
	}
}

func TestNegotiateCompression(t *testing.T) {
	cases := []struct {
		name        string
		clientWants bool
		serverFlags uint32
		envOverride string
		expected    bool
	}{
		{"ClientOnServerOnEnables", true, protocol.ClientCompress, "", true},
		{"ClientOnServerOffDisables", true, 0, "", false},
		{"ClientOffServerOnDisables", false, protocol.ClientCompress, "", false},
		{"ClientOffServerOffDisables", false, 0, "", false},
		{"EnvZeroDisablesEvenWhenOn", true, protocol.ClientCompress, "0", false},
		{"EnvFalseCaseInsensitive", true, protocol.ClientCompress, "FALSE", false},
		{"EnvOffDisables", true, protocol.ClientCompress, "Off", false},
		{"EnvCannotForceOnWhenServerOff", true, 0, "1", false},
		{"EnvOtherValuesIgnored", true, protocol.ClientCompress, "garbage", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := NegotiateCompression(tc.clientWants, tc.serverFlags, tc.envOverride)
			if res != tc.expected {
				t.Errorf("expected %t, got %t for params (%t, 0x%x, %q)",
					tc.expected, res, tc.clientWants, tc.serverFlags, tc.envOverride)
			}
		})
	}
}

func TestHandshakeResponseExcludesOB20WhenDisabled(t *testing.T) {
	cfg := &Config{
		Addr:       "127.0.0.1:2883",
		User:       "sys@tenant#cluster",
		Password:   "test-password",
		Timeout:    time.Second,
		Attributes: map[string]string{},
		ProtocolV2: false, // Explicitly disable OB20
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

	// Bytes 28-32 should be lower 32-bit of ob_capability in LE, and NOT contain obClientCapOBProtocolV2
	extendedCaps := binary.LittleEndian.Uint32(response[28:32])
	if extendedCaps&uint32(obClientCapOBProtocolV2) != 0 {
		t.Errorf("expected extended capability to NOT contain obClientCapOBProtocolV2 when ProtocolV2 is false")
	}

	// attributes in response should not contain ob_capability_flag value with obClientCapOBProtocolV2 bit set
	attrs := conn.connectionAttributes(hs)
	for _, attr := range attrs {
		if attr[0] == "ob_capability_flag" || attr[0] == "__proxy_capability_flag" {
			val, _ := strconv.ParseUint(attr[1], 10, 64)
			if val&obClientCapOBProtocolV2 != 0 {
				t.Errorf("attribute %s has obClientCapOBProtocolV2 set: %s", attr[0], attr[1])
			}
		}
	}
}
