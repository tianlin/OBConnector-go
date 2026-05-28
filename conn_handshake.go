package oceanbase

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/helingjun/obconnector-go/internal/protocol"
)

func dialAndHandshake(ctx context.Context, cfg *Config) (*Conn, error) {
	var d net.Dialer
	if cfg.Timeout > 0 {
		d.Timeout = cfg.Timeout
	}

	var lastErr error
	for _, addr := range cfg.Addrs {
		netConn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			lastErr = err
			continue
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
			lastErr = err
			continue
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
	return nil, lastErr
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

	if hs.status&0x0004 != 0 {
		c.tenantMode = "oracle"
	} else {
		c.tenantMode = "mysql"
	}

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

	return c.handleAuthResult(hs)
}

func (c *Conn) handleAuthResult(hs *handshake) error {
	const maxAuthRounds = 10
	for round := 0; round < maxAuthRounds; round++ {
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
			if c.cfg.ProtocolV2 && c.ob20Confirmed {
				magic := protocol.OB20MagicNum
				if c.cfg.OB20Magic != 0 {
					magic = c.cfg.OB20Magic
				}
				c.tracef("enabling OB 2.0 protocol encapsulation (ConnectionID: %d, Magic: 0x%04x, NewExtraInfo: true, Checksums: %v)", hs.connectionID, magic, !c.cfg.DisableOB20Checksum)
				c.packets.EnableOB20(hs.connectionID, magic, true, c.cfg.DisableOB20Checksum)
			}
			envOverride := os.Getenv("OB_USE_COMPRESSION")
			negotiatedCompress := NegotiateCompression(c.cfg.UseCompression, hs.capabilities, envOverride)
			if negotiatedCompress {
				c.packets.EnableCompression()
			}
			return nil
		case protocol.ErrPacket:
			c.tracef("auth result: ERR")
			return parseServerError(authResult)
		case 0x01:
			if len(authResult) < 2 {
				return fmt.Errorf("oceanbase: unexpected auth response 0x01 (too short)")
			}
			switch authResult[1] {
			case 0x03:
				c.tracef("auth result: auth-switch request (0x01 0x03)")
				plugin, seed, err := c.readAuthSwitchData()
				if err != nil {
					return err
				}
				hs.authPlugin = plugin
				hs.authSeed = seed
				authResp, err := buildAuthResponse(hs.authPlugin, c.cfg.Password, hs.authSeed)
				if err != nil {
					return err
				}
				if err := c.packets.WritePacket(authResp); err != nil {
					return err
				}
			case 0x04:
				c.tracef("auth result: caching_sha2_password full-auth required")
				if c.cfg.TLSConfig != nil {
					if err := c.packets.WritePacket([]byte(c.cfg.Password)); err != nil {
						return err
					}
				} else {
					return fmt.Errorf("oceanbase: caching_sha2_password full-auth requires TLS")
				}
			default:
				return fmt.Errorf("oceanbase: unexpected auth sub-response 0x01 0x%02x", authResult[1])
			}
		case 0xFE:
			c.tracef("auth result: classic auth-switch request (0xFE)")
			plugin, seed, err := c.readAuthSwitchData()
			if err != nil {
				return err
			}
			hs.authPlugin = plugin
			hs.authSeed = seed
			authResp, err := buildAuthResponse(hs.authPlugin, c.cfg.Password, hs.authSeed)
			if err != nil {
				return err
			}
			if err := c.packets.WritePacket(authResp); err != nil {
				return err
			}
		default:
			return fmt.Errorf("oceanbase: unexpected auth response 0x%02x", authResult[0])
		}
	}
	return fmt.Errorf("oceanbase: exceeded maximum authentication rounds (%d)", maxAuthRounds)
}

func (c *Conn) readAuthSwitchData() (string, []byte, error) {
	packet, err := c.packets.ReadPacket()
	if err != nil {
		return "", nil, err
	}
	pos := 0
	plugin, used, err := readNullTerminated(packet[pos:])
	if err != nil {
		return "", nil, err
	}
	pos += used
	seed := append([]byte(nil), packet[pos:]...)
	return plugin, seed, nil
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
		protocol.ClientCanHandleExpiredPasswords |
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
		protocol.ClientSupportOracleMode |
		protocol.ClientCanHandleExpiredPasswords
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

	envOverride := os.Getenv("OB_USE_COMPRESSION")
	negotiatedCompress := NegotiateCompression(c.cfg.UseCompression, hs.capabilities, envOverride)
	if negotiatedCompress {
		caps |= protocol.ClientCompress
	}

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
		dbName := c.cfg.Database
		if c.tenantMode == "oracle" {
			dbName = strings.ToUpper(dbName)
		}
		out = append(out, dbName...)
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
			attrPayload = append(attrPayload, byte(len(kv[0])))
			attrPayload = append(attrPayload, kv[0]...)
			attrPayload = protocol.PutLengthEncodedString(attrPayload, kv[1])
		}
		out = protocol.PutLengthEncodedInt(out, uint64(len(attrPayload)))
		out = append(out, attrPayload...)
	}
	return out
}

func (c *Conn) connectionAttributes(hs *handshake) [][2]string {
	attrMap := map[string]string{
		"_client_name":      "OceanBase Connector/Go",
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
