# Protocol Notes

This document records protocol discoveries for `obconnector-go`. It is a working notebook for clean-room implementation: document externally observable behavior, packet shapes, and experiments, but do not copy OBConnector-C, MariaDB Connector/C, or go-sql-driver/mysql source code.

## Goal

Build a pure-Go `database/sql` driver that can connect to OceanBase Oracle tenants and execute `select 1 from dual`.

Status: achieved for the first PoC against an OceanBase Oracle tenant through port `2883` with `mysql_native_password`.

Known motivation:

- `go-ora` speaks Oracle TNS and times out against OBProxy's MySQL/OB protocol port.
- `go-sql-driver/mysql` can connect to OceanBase MySQL tenants, but OceanBase Oracle tenants may reject it with `Oracle tenant for current client driver is not supported`.
- `obclient` works because it is based on MariaDB CLI plus OBConnector-C/LibOBClient and sends OceanBase-specific protocol behavior.

## Initial Implementation

The current Go PoC implements the ordinary MySQL protocol path needed for experiments:

1. Read protocol 10 initial handshake.
2. Send protocol 41 handshake response.
3. Use `mysql_native_password` authentication.
4. Send `COM_QUERY`.
5. Stream text result sets row by row and parse server error packets.
6. Provide basic `database/sql` transaction methods. Oracle mode starts transactions implicitly, so `BeginTx` reserves the connection and `Commit`/`Rollback` send `commit`/`rollback`.
7. Expose basic column type metadata for `database/sql.ColumnTypes()`.
8. Implement `Prepare`/`PrepareContext` as statement compatibility wrappers over `COM_QUERY`.
9. Support client-side interpolation for `?` parameters. This is not server-side prepared statement support.
10. Implement connection-pool lifecycle hooks: `IsValid` and `ResetSession`. Network/protocol errors mark the connection invalid; server SQL errors do not.

Client capabilities currently enabled when the server advertises them:

- `CLIENT_LONG_PASSWORD`
- `CLIENT_LONG_FLAG`
- `CLIENT_PROTOCOL_41`
- `CLIENT_TRANSACTIONS`
- `CLIENT_SECURE_CONNECTION`
- `CLIENT_MULTI_RESULTS`
- `CLIENT_PLUGIN_AUTH`
- `CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA`
- `CLIENT_CONNECT_ATTRS`
- `CLIENT_SESSION_TRACK`
- `CLIENT_SUPPORT_ORACLE_MODE` as an OceanBase/MariaDB-style extension bit; do not filter this out just because the server did not advertise it in the standard server capability bitmap.
- `CLIENT_CONNECT_WITH_DB` when a database is supplied

Default client attributes:

- `_client_name=libmariadb`
- `_client_version` matches driver `Version` constant (e.g. `0.1.0`)
- `_os=<GOOS>`
- `_platform=<GOARCH>`
- `program_name=<argv0>`
- `ob_server_version=<server-version-from-handshake>`

OceanBase-specific attributes required for the Oracle tenant PoC:

- `__mysql_client_type=__ob_libobclient`
- `__ob_client_name=OceanBase Connector/C`
- `__ob_client_version=<driver-version>`
- `__proxy_capability_flag=311552`
- `__ob_client_attribute_capability_flag=5`

DSN query parameters named `attr.<key>=<value>` override or add extra attributes for controlled experiments.

Additional experiment knobs:

- `trace=true`: print parsed handshake details, final client capabilities, client attributes, auth result, and queries.
- `cap.add=<uint32>`: force capability bits on, including candidate OceanBase-specific bits discovered by capture.
- `cap.drop=<uint32>`: force capability bits off to test server classification.
- `collation=<uint8>`: change the collation byte sent in the handshake response.
- `init=<sql>`: run post-auth initialization SQL before the PoC query.

## Open Questions

These need packet captures or official public documentation before hard-coding behavior:

- Whether larger Oracle-mode query/result workflows require actually wrapping packets with OB20 after authentication. The initial PoC succeeds with plain `COM_QUERY` after sending OBConnector-C-style handshake capabilities and attributes.
- Whether direct observer and OBProxy require exactly the same `__proxy_capability_flag` and `__ob_client_attribute_capability_flag` values.
- Whether Connector/J's public "OB2.0 protocol" support maps to capability bits, connection attributes, a changed authentication exchange, or post-auth commands.
- Whether OBConnector-C changes only the handshake response or also sends session initialization commands before the first user query.
- Exact behavior against direct observer port versus OBProxy port.
- Authentication plugins observed across OceanBase versions and tenant modes.
- TLS negotiation behavior and whether Oracle tenants require different defaults.

## Verified Oracle Tenant PoC

Environment observed from the server handshake:

- Server version: `5.6.25`
- Auth plugin: `mysql_native_password`
- Server capabilities: `0x009ff7df`
- Error before OBConnector-C-style extensions: `1235 (0A000): Oracle tenant for current client driver is not supported`

Successful client response characteristics:

- Final client capabilities: `0x08baa205`
- Required extension capability: `CLIENT_SUPPORT_ORACLE_MODE`
- Auth result: `OK`
- Query: `select 1 from dual`
- Result: `1`
- Query: `select 1 as one, sysdate as now from dual`
- Result: one row. The server reported both columns as `DECIMAL`, while `sysdate` was returned as a timestamp string. Treat Oracle-mode text rows conservatively as strings unless a later OB20/new-metadata path provides reliable type information.
- Transaction smoke test: `BeginTx` + `select 1 from dual` + `commit` succeeded.

Regression coverage:

- Opaque DSN parsing for `oceanbase:user:pass@host:port/db?...`.
- URL DSN parsing for attributes, capability overrides, collation, init SQL, and preset.
- Handshake response keeps `CLIENT_SUPPORT_ORACLE_MODE` and includes required `__ob_*` attributes.
- Text row parsing for NULL and string values. Numeric conversion is intentionally conservative because Oracle-mode text metadata can report misleading MySQL type codes.
- Basic column type metadata mapping.
- Streaming rows release the underlying connection lock on EOF or `Close`.
- Prepared statement compatibility path and `?` parameter interpolation.
- Connection validity and bad-connection mapping.

## Oracle Timestamp Result Payloads

The Oracle-mode result metadata uses OceanBase-specific timestamp type codes:

- `0xc8`: `TIMESTAMP WITH TIME ZONE`
- `0xc9`: `TIMESTAMP WITH LOCAL TIME ZONE`
- `0xca`: `TIMESTAMP`

For the observed result payload, the first 12 bytes are:

```text
century, year, month, day, hour, minute, second,
nanoseconds[4] little-endian, scale
```

`0xc8` appends signed timezone hour/minute bytes, followed by a timezone-name
length/name and an abbreviation length/abbreviation; zero lengths are valid, but
the length bytes themselves are required. `0xc9` has no offset in the payload
because its displayed wall clock is determined by the session timezone. The
driver decodes all three
types to UTC `time.Time`; Oracle sessions default to UTC and accept the
`sessionTimeZone=UTC` or `sessionTimeZone=+08:00` DSN options. IANA region
session time zones are rejected because client/server tzdata can differ and
would otherwise produce a silently wrong UTC instant.

Mode detection is automatic by default. Use the `oboracle` driver, `preset=oboracle`,
or `oracleMode=true` when the proxy/tenant handshake does not expose the Oracle
mode status bit. The configured session timezone is restored by the
`database/sql` connection reset hook; explicit timezone-changing SQL executed
through this driver also updates the local LTZ decoder state.

Malformed payloads are returned as errors instead of being exposed as strings.

## Experiment Plan

For each experiment, capture:

- OceanBase version, tenant mode, direct observer or OBProxy, and port.
- Client used: `obclient`, Connector/J, go-sql-driver/mysql, `cmd/obping`.
- Initial handshake server version, auth plugin, server capabilities, and status flags.
- Handshake response capabilities and client attributes.
- Auth result packet and first server error, if any.
- Any session initialization SQL before `select 1 from dual`.

Recommended comparisons:

1. `obclient` against Oracle tenant.
2. `obclient` against MySQL tenant.
3. Connector/J against Oracle tenant.
4. go-sql-driver/mysql against Oracle tenant.
5. `cmd/obping` against Oracle tenant with default attributes.
6. `cmd/obping` with targeted `attr.*` changes derived from public docs and captures.

## `cmd/obping` Capture Commands

Default trace:

```bash
go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -trace
```

Attribute experiment:

```bash
go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -trace \
  -attr '_client_name=<candidate>' \
  -attr '_client_version=<candidate>'
```

Capability experiment:

```bash
go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -trace \
  -cap-add 0x0 \
  -cap-drop 0x0
```

Post-auth initialization experiment:

```bash
go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -trace \
  -init '<sql observed from a known-good client>'
```

Custom query and transaction validation:

```bash
go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -query 'select 1 as one, sysdate as now from dual' \
  -max-rows 20

go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -tx-test

go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -exec-test

go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -param-test

go run ./cmd/obping \
  -host <host> \
  -port <port> \
  -user '<user>@<tenant>#<cluster>' \
  -password '<password>' \
  -pool-test \
  -trace
```

When reporting a capture result back into this document, include:

- Command used, with password redacted.
- Whether the connection target is OBProxy or direct observer.
- stderr trace from `cmd/obping`.
- Server error number, SQLSTATE, and message if authentication or query fails.
- Packet-level differences from `obclient` or Connector/J.

## Clean-Room Boundary

Allowed:

- Read official OceanBase documentation.
- Observe packet captures from our own client/server experiments.
- Record protocol fields, API behavior, error codes, and wire-level facts.
- Compare high-level behavior of open-source drivers without copying implementation code.

Not allowed:

- Copy source code from OBConnector-C, MariaDB Connector/C, or MPL/LGPL codebases.
- Port functions line by line.
- Preserve variable names, comments, or control-flow structure from incompatible source code.
