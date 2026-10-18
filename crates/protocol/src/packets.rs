//! MySQL 各类包的构造与解析。
//!
//! 移植自 Mycat3 `common` 模块：`HandshakePacket`、`AuthPacket`、`AuthSwitchRequestPacket`、
//! `ErrorPacketImpl`、`ColumnDefPacketImpl`、`MySQLPacketUtil` 等。

use crate::codec::{PacketReader, PacketWriter};
use crate::consts::{capability::*, error_code, field_type, status, PROTOCOL_VERSION};
use crate::error::{ProtocolError, ProtocolResult};

// ---------------------------------------------------------------------------
// HandshakeV10（服务端 → 客户端）
// ---------------------------------------------------------------------------

/// 构造 HandshakeV10 载荷。字段顺序与 `HandshakePacket.writePayload` 一致：
/// `0x0a | serverVersion\0 | connId(4) | salt1(8) | 0x00 | caps_low(2) | charset(1)
/// | status(2) | caps_high(2) | authPluginDataLen(1) | reserved(10) | salt2(12)+0x00 | plugin\0`
pub fn build_handshake_v10(
    server_version: &str,
    connection_id: u32,
    seed: &[u8; 20],
    capabilities: u32,
    charset: u8,
    status_flags: u16,
    auth_plugin: &str,
) -> Vec<u8> {
    let mut w = PacketWriter::with_capacity(128);
    w.write_u8(PROTOCOL_VERSION);
    w.write_null_str(server_version);
    w.write_int4(connection_id);
    w.write_fixed(&seed[..8]);
    w.write_u8(0); // filler
    w.write_int2((capabilities & 0xFFFF) as u16);
    w.write_u8(charset);
    w.write_int2(status_flags);
    w.write_int2(((capabilities >> 16) & 0xFFFF) as u16);
    if capabilities & CLIENT_PLUGIN_AUTH != 0 {
        w.write_u8(21); // authPluginDataLen = 8 + 12 + 1
    } else {
        w.write_u8(0);
    }
    w.write_zeros(10); // reserved
    if capabilities & CLIENT_SECURE_CONNECTION != 0 {
        w.write_fixed(&seed[8..20]);
        w.write_u8(0);
    }
    if capabilities & CLIENT_PLUGIN_AUTH != 0 {
        w.write_null_str(auth_plugin);
    }
    w.into_bytes()
}

// ---------------------------------------------------------------------------
// HandshakeResponse41（客户端 → 服务端）
// ---------------------------------------------------------------------------

/// 解析后的客户端认证响应包。对应 `AuthPacket`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthResponse {
    pub capabilities: u32,
    pub max_packet_size: u32,
    pub charset: u8,
    pub username: String,
    /// scramble 响应（auth-plugin-data）
    pub auth_response: Vec<u8>,
    pub database: Option<String>,
    pub auth_plugin_name: Option<String>,
}

pub fn parse_auth_response(payload: &[u8]) -> ProtocolResult<AuthResponse> {
    let mut r = PacketReader::new(payload);
    let capabilities = r.read_int4()?;
    let max_packet_size = r.read_int4()?;
    let charset = r.read_u8()?;
    r.skip(23)?; // reserved
    let username = r.read_null_str()?.to_string();

    let auth_response = if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        let len = r.read_lenenc_int()? as usize;
        r.read_fixed(len)?.to_vec()
    } else if capabilities & CLIENT_SECURE_CONNECTION != 0 {
        let len = r.read_u8()? as usize;
        r.read_fixed(len)?.to_vec()
    } else {
        // NUL 结尾
        let s = r.read_null_str()?;
        s.as_bytes().to_vec()
    };

    let database = if capabilities & CLIENT_CONNECT_WITH_DB != 0 && !r.is_empty() {
        let db = r.read_null_str()?.to_string();
        if db.is_empty() {
            None
        } else {
            Some(db)
        }
    } else {
        None
    };

    let auth_plugin_name = if capabilities & CLIENT_PLUGIN_AUTH != 0 && !r.is_empty() {
        Some(r.read_null_str()?.to_string())
    } else {
        None
    };

    Ok(AuthResponse {
        capabilities,
        max_packet_size,
        charset,
        username,
        auth_response,
        database,
        auth_plugin_name,
    })
}

/// 构造 AuthSwitchRequest (0xFE | plugin\0 | seed\0)。对应 `AuthSwitchRequestPacket`。
pub fn build_auth_switch_request(plugin: &str, seed: &[u8]) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(0xfe);
    w.write_null_str(plugin);
    w.write_fixed(seed);
    w.write_u8(0);
    w.into_bytes()
}

/// caching_sha2 快速认证成功通知：0x01 0x03。
pub fn build_caching_sha2_fast_ok() -> Vec<u8> {
    vec![0x01, 0x03]
}

/// caching_sha2 需要完整认证（明文密码）：0x01 0x04。
pub fn build_caching_sha2_full_auth() -> Vec<u8> {
    vec![0x01, 0x04]
}

// ---------------------------------------------------------------------------
// OK / ERR / EOF
// ---------------------------------------------------------------------------

/// 构造 OK 包。对应 `MySQLPacketUtil.generateOk`。
/// `header` 为 0x00；CLIENT_DEPRECATE_EOF 时 EOF 位置用 0xfe。
pub fn build_ok(
    header: u8,
    affected_rows: u64,
    last_insert_id: u64,
    server_status: u16,
    warning_count: u16,
    message: &str,
) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(header);
    w.write_lenenc_int(affected_rows);
    w.write_lenenc_int(last_insert_id);
    w.write_int2(server_status);
    w.write_int2(warning_count);
    if !message.is_empty() {
        w.write_eof_str(message);
    }
    w.into_bytes()
}

pub fn build_ok_packet(
    affected_rows: u64,
    last_insert_id: u64,
    server_status: u16,
    message: &str,
) -> Vec<u8> {
    build_ok(
        0x00,
        affected_rows,
        last_insert_id,
        server_status,
        0,
        message,
    )
}

/// CLIENT_DEPRECATE_EOF 时行结束包：0xfe 头的 OK 包。
pub fn build_eof_as_ok(server_status: u16, warning_count: u16) -> Vec<u8> {
    build_ok(0xfe, 0, 0, server_status, warning_count, "")
}

/// 构造 EOF 包 (0xfe | warnings(2) | status(2))。对应 `generateEof`。
pub fn build_eof(warning_count: u16, status_flags: u16) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(0xfe);
    w.write_int2(warning_count);
    w.write_int2(status_flags);
    w.into_bytes()
}

/// 构造 ERR 包：`0xff | errno(2) | '#' | sqlstate(5) | message`。
///
/// 对应 `ErrorPacketImpl.writePayload`；Java 版省略了 sqlstate（源码中被注释），
/// Pro 版恢复标准格式以兼容严格的客户端。
pub fn build_error(errno: u16, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(0xff);
    w.write_int2(errno);
    w.write_u8(b'#');
    let state = format!("{:<5}", sqlstate);
    w.write_fixed(state[..5].as_bytes());
    w.write_eof_str(message);
    w.into_bytes()
}

pub fn access_denied(user: &str, host: &str, using_password: bool) -> Vec<u8> {
    build_error(
        error_code::ER_ACCESS_DENIED_ERROR,
        "28000",
        &format!(
            "Access denied for user '{}'@'{}' (using password: {})",
            user,
            host,
            if using_password { "YES" } else { "NO" }
        ),
    )
}

// ---------------------------------------------------------------------------
// 结果集：列数 / 列定义 / 行
// ---------------------------------------------------------------------------

/// 列定义。对应 `ColumnDefPacketImpl`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub schema: String,
    pub table: String,
    pub org_table: String,
    pub name: String,
    pub org_name: String,
    /// 排序规则索引，默认 33 (utf8_general_ci)；二进制列用 63
    pub charset: u16,
    /// 最大显示长度，默认 255
    pub column_length: u32,
    pub field_type: u8,
    pub flags: u16,
    pub decimals: u8,
}

impl Default for ColumnDef {
    fn default() -> Self {
        ColumnDef {
            schema: String::new(),
            table: String::new(),
            org_table: String::new(),
            name: String::new(),
            org_name: String::new(),
            charset: crate::consts::charset::UTF8_GENERAL_CI as u16,
            column_length: 255,
            field_type: field_type::VAR_STRING,
            flags: 0,
            decimals: 0,
        }
    }
}

impl ColumnDef {
    pub fn new(name: &str, field_type: u8) -> Self {
        ColumnDef {
            name: name.to_string(),
            org_name: name.to_string(),
            field_type,
            ..Default::default()
        }
    }

    pub fn is_binary(&self) -> bool {
        self.charset == crate::consts::charset::BINARY as u16
    }

    /// 序列化为列定义包载荷。字段顺序与 Java `writePayload` 完全一致。
    pub fn encode(&self) -> Vec<u8> {
        let mut w = PacketWriter::with_capacity(64 + self.name.len());
        w.write_lenenc_bytes(b"def");
        w.write_lenenc_bytes(self.schema.as_bytes());
        w.write_lenenc_bytes(self.table.as_bytes());
        w.write_lenenc_bytes(self.org_table.as_bytes());
        w.write_lenenc_bytes(self.name.as_bytes());
        w.write_lenenc_bytes(self.org_name.as_bytes());
        w.write_lenenc_int(0x0c);
        w.write_int2(self.charset);
        w.write_int4(self.column_length);
        w.write_u8(self.field_type);
        w.write_int2(self.flags);
        w.write_u8(self.decimals);
        w.write_int2(0); // filler
        w.into_bytes()
    }

    /// 从载荷解析列定义（后端响应处理用）。
    pub fn decode(payload: &[u8]) -> ProtocolResult<ColumnDef> {
        let mut r = PacketReader::new(payload);
        let _catalog = r.read_lenenc_bytes()?;
        let schema = read_lenenc_str(&mut r)?;
        let table = read_lenenc_str(&mut r)?;
        let org_table = read_lenenc_str(&mut r)?;
        let name = read_lenenc_str(&mut r)?;
        let org_name = read_lenenc_str(&mut r)?;
        let _len = r.read_lenenc_int()?; // 0x0c
        let charset = r.read_int2()?;
        let column_length = r.read_int4()?;
        let field_type = r.read_u8()?;
        let flags = r.read_int2()?;
        let decimals = r.read_u8()?;
        Ok(ColumnDef {
            schema,
            table,
            org_table,
            name,
            org_name,
            charset,
            column_length,
            field_type,
            flags,
            decimals,
        })
    }
}

fn read_lenenc_str(r: &mut PacketReader) -> ProtocolResult<String> {
    match r.read_lenenc_bytes()? {
        None => Ok(String::new()),
        Some(b) => String::from_utf8(b.to_vec()).map_err(|_| ProtocolError::InvalidUtf8),
    }
}

/// 列数包载荷（lenenc int）。
pub fn build_column_count(count: usize) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_lenenc_int(count as u64);
    w.into_bytes()
}

/// 构造文本协议行：NULL → 0xfb，其余 → lenenc 字节。对应 `generateTextRow`。
pub fn build_text_row(values: &[Option<&[u8]>]) -> Vec<u8> {
    let mut w = PacketWriter::new();
    for v in values {
        match v {
            None => w.write_u8(crate::consts::NULL_MARK),
            Some(bytes) => w.write_lenenc_bytes(bytes),
        }
    }
    w.into_bytes()
}

/// 构造二进制协议行。对应 `generateBinaryRow`：
/// `0x00 | null-bitmap((n+7+2)/8, 位偏移 i+2) | 每列已编码值`
pub fn build_binary_row(values: &[Option<Vec<u8>>]) -> Vec<u8> {
    let n = values.len();
    let bitmap_len = (n + 7 + 2) / 8;
    let mut bitmap = vec![0u8; bitmap_len];
    let mut w = PacketWriter::new();
    w.write_u8(0x00);
    for (i, v) in values.iter().enumerate() {
        if v.is_none() {
            let bit = i + 2;
            bitmap[bit / 8] |= 1 << (bit % 8);
        }
    }
    w.write_fixed(&bitmap);
    for v in values {
        if let Some(bytes) = v {
            w.write_fixed(bytes);
        }
    }
    w.into_bytes()
}

/// COM_STMT_PREPARE 响应 OK 包。对应 `generatePrepareOk`：
/// `0x00 | stmtId(4) | numColumns(2) | numParams(2) | 0x00 | warnings(2)`
pub fn build_prepare_ok(stmt_id: u32, num_columns: u16, num_params: u16, warnings: u16) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(0x00);
    w.write_int4(stmt_id);
    w.write_int2(num_columns);
    w.write_int2(num_params);
    w.write_u8(0x00);
    w.write_int2(warnings);
    w.into_bytes()
}

// ---------------------------------------------------------------------------
// 响应包分类（后端/前端通用）
// ---------------------------------------------------------------------------

/// 响应包类型。对应 `MySQLPacketResolver.resolvePayloadType` 的分类规则。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    /// 0xff
    Error,
    /// 0xfe 且载荷 < 9：EOF（也可能是 auth-switch，载荷首字节 0xfe 且更长）
    Eof,
    /// 0xfe 且更长：AuthSwitchRequest / AuthMoreData
    AuthSwitch,
    /// 0x00
    Ok,
    /// 0x01：AuthMoreData (caching_sha2)
    AuthMoreData,
    /// 0xfb：LOAD DATA LOCAL 请求（文本协议下行）或二进制行 NULL
    LocalInfile,
    /// 其他：lenenc 列数 / 行数据
    ResultSetRow,
}

pub fn classify_payload(payload: &[u8]) -> PayloadKind {
    match payload.first() {
        None => PayloadKind::ResultSetRow,
        Some(0xff) => PayloadKind::Error,
        Some(0xfe) => {
            if payload.len() < 9 {
                PayloadKind::Eof
            } else {
                PayloadKind::AuthSwitch
            }
        }
        Some(0x00) => PayloadKind::Ok,
        Some(0x01) => PayloadKind::AuthMoreData,
        Some(0xfb) => PayloadKind::LocalInfile,
        Some(_) => PayloadKind::ResultSetRow,
    }
}

/// 解析 OK 包。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OkPacket {
    pub affected_rows: u64,
    pub last_insert_id: u64,
    pub status_flags: u16,
    pub warnings: u16,
    pub message: String,
}

pub fn parse_ok(payload: &[u8]) -> ProtocolResult<OkPacket> {
    let mut r = PacketReader::new(payload);
    let _header = r.read_u8()?; // 0x00 或 0xfe
    let affected_rows = r.read_lenenc_int()?;
    let last_insert_id = r.read_lenenc_int()?;
    let status_flags = if r.remaining() >= 2 {
        r.read_int2()?
    } else {
        0
    };
    let warnings = if r.remaining() >= 2 {
        r.read_int2()?
    } else {
        0
    };
    let message = r.read_eof_str().unwrap_or("").to_string();
    Ok(OkPacket {
        affected_rows,
        last_insert_id,
        status_flags,
        warnings,
        message,
    })
}

/// 解析 ERR 包。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrPacket {
    pub errno: u16,
    pub sqlstate: String,
    pub message: String,
}

pub fn parse_err(payload: &[u8]) -> ProtocolResult<ErrPacket> {
    let mut r = PacketReader::new(payload);
    let _header = r.read_u8()?; // 0xff
    let errno = r.read_int2()?;
    let mut sqlstate = String::new();
    if r.remaining() > 0 {
        // 可选 '#' + 5 字节 sqlstate
        if r.peek_u8()? == b'#' {
            r.skip(1)?;
            let bytes = r.read_fixed(5)?;
            sqlstate = String::from_utf8_lossy(bytes).to_string();
        }
    }
    let message = r.read_eof_str().unwrap_or("").to_string();
    Ok(ErrPacket {
        errno,
        sqlstate,
        message,
    })
}

// ---------------------------------------------------------------------------
// 命令构造（后端客户端使用）
// ---------------------------------------------------------------------------

pub fn build_com_query(sql: &str) -> Vec<u8> {
    let mut w = PacketWriter::with_capacity(sql.len() + 1);
    w.write_u8(crate::consts::command::COM_QUERY);
    w.write_eof_str(sql);
    w.into_bytes()
}

pub fn build_com_init_db(schema: &str) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(crate::consts::command::COM_INIT_DB);
    w.write_eof_str(schema);
    w.into_bytes()
}

pub fn build_com_ping() -> Vec<u8> {
    vec![crate::consts::command::COM_PING]
}

pub fn build_com_quit() -> Vec<u8> {
    vec![crate::consts::command::COM_QUIT]
}

pub fn build_com_stmt_prepare(sql: &str) -> Vec<u8> {
    let mut w = PacketWriter::new();
    w.write_u8(crate::consts::command::COM_STMT_PREPARE);
    w.write_eof_str(sql);
    w.into_bytes()
}

/// 客户端握手响应（HandshakeResponse41）构造，后端连接使用。
pub fn build_auth_response(
    capabilities: u32,
    max_packet_size: u32,
    charset: u8,
    username: &str,
    auth_response: &[u8],
    database: Option<&str>,
    auth_plugin: &str,
) -> Vec<u8> {
    let mut w = PacketWriter::with_capacity(128);
    w.write_int4(capabilities);
    w.write_int4(max_packet_size);
    w.write_u8(charset);
    w.write_zeros(23);
    w.write_null_str(username);
    if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
        w.write_lenenc_bytes(auth_response);
    } else if capabilities & CLIENT_SECURE_CONNECTION != 0 {
        w.write_u8(auth_response.len() as u8);
        w.write_fixed(auth_response);
    } else {
        w.write_fixed(auth_response);
        w.write_u8(0);
    }
    if capabilities & CLIENT_CONNECT_WITH_DB != 0 {
        if let Some(db) = database {
            w.write_null_str(db);
        } else {
            w.write_u8(0);
        }
    }
    if capabilities & CLIENT_PLUGIN_AUTH != 0 {
        w.write_null_str(auth_plugin);
    }
    w.into_bytes()
}

// ---------------------------------------------------------------------------
// 二进制协议参数值（COM_STMT_EXECUTE）
// ---------------------------------------------------------------------------

/// 从 COM_STMT_EXECUTE 载荷读取绑定参数，返回 (文本表示, 字段类型)。
/// 携带类型使替换层按类型渲染字面量（字符串类加引号、BLOB 类 X'hex'），
/// 避免"看起来像数字的字符串"被裸写导致隐式类型转换/索引失效（Bug 修复）。
/// 移植自 `BindValueUtil.read` 的核心分支（数值/字符串类型）。
pub fn read_binary_params(
    payload: &[u8],
    num_params: usize,
) -> ProtocolResult<Vec<Option<(String, u8)>>> {
    let mut r = PacketReader::new(payload);
    r.skip(4)?; // stmtId
    r.skip(1)?; // flags
    r.skip(4)?; // iteration count
    if num_params == 0 {
        return Ok(Vec::new());
    }
    let bitmap_len = (num_params + 7) / 8;
    let bitmap = r.read_fixed(bitmap_len)?.to_vec();
    let new_params_bound = r.read_u8()?;
    let mut types = vec![field_type::VAR_STRING as u16; num_params];
    if new_params_bound == 1 {
        for t in types.iter_mut() {
            *t = r.read_int2()?;
        }
    }
    let mut values = Vec::with_capacity(num_params);
    for i in 0..num_params {
        let is_null = bitmap[i / 8] & (1 << (i % 8)) != 0;
        if is_null {
            values.push(None);
            continue;
        }
        let t = (types[i] & 0xff) as u8;
        let unsigned = types[i] & 0x8000 != 0;
        let v = match t {
            field_type::TINY => {
                let b = r.read_u8()?;
                if unsigned {
                    (b as u16).to_string()
                } else {
                    (b as i8).to_string()
                }
            }
            field_type::SHORT | field_type::YEAR => {
                let v = r.read_int2()?;
                if unsigned {
                    v.to_string()
                } else {
                    (v as i16).to_string()
                }
            }
            field_type::LONG | field_type::INT24 => {
                let v = r.read_int4()?;
                if unsigned {
                    v.to_string()
                } else {
                    (v as i32).to_string()
                }
            }
            field_type::LONGLONG => {
                let v = r.read_int8()?;
                if unsigned {
                    v.to_string()
                } else {
                    (v as i64).to_string()
                }
            }
            field_type::FLOAT => {
                let v = f32::from_bits(r.read_int4()?);
                v.to_string()
            }
            field_type::DOUBLE => {
                let v = f64::from_bits(r.read_int8()?);
                v.to_string()
            }
            field_type::DATE | field_type::DATETIME | field_type::TIMESTAMP => {
                let len = r.read_u8()?;
                let bytes = r.read_fixed(len as usize)?.to_vec();
                format_datetime(&bytes)
            }
            field_type::TIME => {
                let len = r.read_u8()?;
                let bytes = r.read_fixed(len as usize)?.to_vec();
                format_time(&bytes)
            }
            _ => {
                // DECIMAL/NEWDECIMAL/VARCHAR/VAR_STRING/STRING/BLOB/JSON/ENUM/SET/GEOMETRY/BIT
                let bytes = match r.read_lenenc_bytes()? {
                    None => {
                        values.push(None);
                        continue;
                    }
                    Some(b) => b,
                };
                if is_blob_field_type(t) {
                    // BLOB 类参数：无损十六进制文本（替换层包 X'...'），
                    // 杜绝 String::from_utf8_lossy 对二进制数据的破坏
                    hex_text(bytes)
                } else {
                    String::from_utf8_lossy(bytes).to_string()
                }
            }
        };
        values.push(Some((v, t)));
    }
    Ok(values)
}

/// BLOB 类字段类型（参数按二进制处理）。
pub fn is_blob_field_type(t: u8) -> bool {
    matches!(
        t,
        field_type::BLOB
            | field_type::TINY_BLOB
            | field_type::MEDIUM_BLOB
            | field_type::LONG_BLOB
            | field_type::GEOMETRY
            | field_type::BIT
    )
}

/// 字节 → 小写十六进制文本（BLOB 参数/长数据的无损表示）。
pub fn hex_text(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

fn format_datetime(b: &[u8]) -> String {
    match b.len() {
        0 => "0000-00-00 00:00:00".to_string(),
        4 => format!(
            "{:04}-{:02}-{:02}",
            u16::from_le_bytes([b[0], b[1]]),
            b[2],
            b[3]
        ),
        7 => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            u16::from_le_bytes([b[0], b[1]]),
            b[2],
            b[3],
            b[4],
            b[5],
            b[6]
        ),
        11 => {
            let micros = u32::from_le_bytes([b[7], b[8], b[9], b[10]]);
            format!(
                "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                u16::from_le_bytes([b[0], b[1]]),
                b[2],
                b[3],
                b[4],
                b[5],
                b[6],
                micros
            )
        }
        _ => String::from_utf8_lossy(b).to_string(),
    }
}

fn format_time(b: &[u8]) -> String {
    if b.is_empty() {
        return "00:00:00".to_string();
    }
    let negative = b[0] == 1;
    let days = u32::from_le_bytes([b[1], b[2], b[3], b[4]]);
    let hours = b[5] as u32 + days * 24;
    let mut s = format!("{:02}:", hours);
    if b.len() >= 8 {
        s.push_str(&format!("{:02}:{:02}", b[6], b[7]));
    }
    if b.len() >= 12 {
        let micros = u32::from_le_bytes([b[8], b[9], b[10], b[11]]);
        s.push_str(&format!(".{:06}", micros));
    }
    if negative {
        s.insert(0, '-');
    }
    s
}

// ---------------------------------------------------------------------------
// 前端会话状态位计算，对应 MycatDataContext.serverStatus()
// ---------------------------------------------------------------------------

pub fn compute_server_status(autocommit: bool, in_transaction: bool, more_results: bool) -> u16 {
    let mut s = 0u16;
    if autocommit {
        s |= status::SERVER_STATUS_AUTOCOMMIT;
    }
    if in_transaction {
        s |= status::SERVER_STATUS_IN_TRANS;
    }
    if more_results {
        s |= status::SERVER_MORE_RESULTS_EXISTS;
    }
    s
}

/// 常用 ERR 快捷构造。
pub fn err_unknown(msg: &str) -> Vec<u8> {
    build_error(error_code::ER_UNKNOWN_ERROR, "HY000", msg)
}

pub fn err_parse(msg: &str) -> Vec<u8> {
    build_error(error_code::ER_PARSE_ERROR, "42000", msg)
}

pub fn err_no_such_table(schema: &str, table: &str) -> Vec<u8> {
    build_error(
        error_code::ER_NO_SUCH_TABLE,
        "42S02",
        &format!("Table '{}.{}' doesn't exist", schema, table),
    )
}

pub fn err_not_supported(msg: &str) -> Vec<u8> {
    build_error(error_code::ER_NOT_SUPPORTED_YET, "42000", msg)
}

pub fn err_bad_db(db: &str) -> Vec<u8> {
    build_error(
        error_code::ER_BAD_DB_ERROR,
        "42000",
        &format!("Unknown database '{}'", db),
    )
}

pub fn err_syntax_near(sql: &str) -> Vec<u8> {
    build_error(
        error_code::ER_PARSE_ERROR,
        "42000",
        &format!(
            "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '{}'",
            sql.chars().take(80).collect::<String>()
        ),
    )
}

pub fn err_empty_query() -> Vec<u8> {
    build_error(error_code::ER_EMPTY_QUERY, "42000", "Query was empty")
}

pub fn default_handshake_capabilities() -> u32 {
    DEFAULT_SERVER_CAPABILITIES
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::AUTH_PLUGIN_NATIVE;

    #[test]
    fn test_handshake_v10_layout() {
        let seed = [b'a'; 20];
        let p = build_handshake_v10(
            "5.7.33-mycat-3.0-pro",
            42,
            &seed,
            DEFAULT_SERVER_CAPABILITIES,
            8,
            2,
            AUTH_PLUGIN_NATIVE,
        );
        let mut r = PacketReader::new(&p);
        assert_eq!(r.read_u8().unwrap(), 10);
        assert_eq!(r.read_null_str().unwrap(), "5.7.33-mycat-3.0-pro");
        assert_eq!(r.read_int4().unwrap(), 42);
        assert_eq!(r.read_fixed(8).unwrap(), &seed[..8]);
        assert_eq!(r.read_u8().unwrap(), 0);
        let caps_low = r.read_int2().unwrap() as u32;
        assert_eq!(r.read_u8().unwrap(), 8);
        assert_eq!(r.read_int2().unwrap(), 2);
        let caps_high = r.read_int2().unwrap() as u32;
        assert_eq!(caps_low | (caps_high << 16), DEFAULT_SERVER_CAPABILITIES);
        assert_eq!(r.read_u8().unwrap(), 21);
        r.skip(10).unwrap();
        assert_eq!(r.read_fixed(12).unwrap(), &seed[8..20]);
        assert_eq!(r.read_u8().unwrap(), 0);
        assert_eq!(r.read_null_str().unwrap(), AUTH_PLUGIN_NATIVE);
        assert!(r.is_empty());
    }

    #[test]
    fn test_auth_response_roundtrip() {
        let caps = DEFAULT_CLIENT_CAPABILITIES | CLIENT_CONNECT_WITH_DB | CLIENT_SECURE_CONNECTION;
        let payload = build_auth_response(
            caps,
            1 << 24,
            33,
            "root",
            b"12345678901234567890",
            Some("db1"),
            AUTH_PLUGIN_NATIVE,
        );
        let resp = parse_auth_response(&payload).unwrap();
        assert_eq!(resp.username, "root");
        assert_eq!(resp.auth_response, b"12345678901234567890");
        assert_eq!(resp.database.as_deref(), Some("db1"));
        assert_eq!(resp.auth_plugin_name.as_deref(), Some(AUTH_PLUGIN_NATIVE));
        assert_eq!(resp.charset, 33);
    }

    #[test]
    fn test_ok_err_eof() {
        let ok = parse_ok(&build_ok_packet(3, 7, 2, "")).unwrap();
        assert_eq!(ok.affected_rows, 3);
        assert_eq!(ok.last_insert_id, 7);
        assert_eq!(ok.status_flags, 2);

        let err = parse_err(&build_error(1045, "28000", "Access denied")).unwrap();
        assert_eq!(err.errno, 1045);
        assert_eq!(err.sqlstate, "28000");
        assert_eq!(err.message, "Access denied");

        let eof = build_eof(0, 2);
        assert_eq!(eof[0], 0xfe);
        assert_eq!(classify_payload(&eof), PayloadKind::Eof);
    }

    #[test]
    fn test_classify() {
        assert_eq!(classify_payload(&[0xff, 1, 0]), PayloadKind::Error);
        assert_eq!(classify_payload(&[0x00, 0]), PayloadKind::Ok);
        assert_eq!(classify_payload(&[0xfe, 0, 0, 2, 0]), PayloadKind::Eof);
        assert_eq!(classify_payload(&[0xfe; 12]), PayloadKind::AuthSwitch);
        assert_eq!(classify_payload(&[0x01, 0x03]), PayloadKind::AuthMoreData);
        assert_eq!(classify_payload(&[0x03, b'a']), PayloadKind::ResultSetRow);
    }

    #[test]
    fn test_column_def_roundtrip() {
        let def = ColumnDef {
            schema: "db1".into(),
            table: "t".into(),
            org_table: "t".into(),
            name: "id".into(),
            org_name: "id".into(),
            charset: 33,
            column_length: 11,
            field_type: field_type::LONG,
            flags: column_flag_def(),
            decimals: 0,
        };
        fn column_flag_def() -> u16 {
            crate::consts::column_flag::NOT_NULL
        }
        let decoded = ColumnDef::decode(&def.encode()).unwrap();
        assert_eq!(decoded, def);
    }

    #[test]
    fn test_text_row() {
        let row = build_text_row(&[Some(b"hello"), None, Some(b"")]);
        let mut r = PacketReader::new(&row);
        assert_eq!(r.read_lenenc_bytes().unwrap(), Some(&b"hello"[..]));
        assert_eq!(r.read_lenenc_bytes().unwrap(), None);
        assert_eq!(r.read_lenenc_bytes().unwrap(), Some(&b""[..]));
    }

    #[test]
    fn test_binary_row_bitmap() {
        // 3 列，第 0、2 列为 NULL
        let row = build_binary_row(&[None, Some(vec![0x01]), None]);
        assert_eq!(row[0], 0x00);
        // bitmap 长度 (3+7+2)/8 = 1，位偏移 i+2：bit2 (col0), bit3(col1 非空), bit4 (col2)
        assert_eq!(row[1], (1 << 2) | (1 << 4));
    }

    #[test]
    fn test_prepare_ok() {
        let p = build_prepare_ok(1, 2, 3, 0);
        let mut r = PacketReader::new(&p);
        assert_eq!(r.read_u8().unwrap(), 0);
        assert_eq!(r.read_int4().unwrap(), 1);
        assert_eq!(r.read_int2().unwrap(), 2);
        assert_eq!(r.read_int2().unwrap(), 3);
        assert_eq!(r.read_u8().unwrap(), 0);
        assert_eq!(r.read_int2().unwrap(), 0);
    }

    #[test]
    fn test_binary_params() {
        // stmtId=1, flags=0, iter=1, bitmap(1 param)=0, bound=1, type=LONG, value=42
        let mut w = PacketWriter::new();
        w.write_int4(1);
        w.write_u8(0);
        w.write_int4(1);
        w.write_u8(0); // null bitmap
        w.write_u8(1); // new params bound
        w.write_int2(field_type::LONG as u16);
        w.write_int4(42);
        let params = read_binary_params(&w.into_bytes(), 1).unwrap();
        assert_eq!(params, vec![Some(("42".to_string(), field_type::LONG))]);

        // BLOB 参数：无损十六进制文本
        let mut w = PacketWriter::new();
        w.write_int4(1);
        w.write_u8(0);
        w.write_int4(1);
        w.write_u8(0); // null bitmap
        w.write_u8(1); // new params bound
        w.write_int2(field_type::BLOB as u16);
        w.write_lenenc_bytes(&[0x00, 0xff, 0x10]);
        let params = read_binary_params(&w.into_bytes(), 1).unwrap();
        assert_eq!(params, vec![Some(("00ff10".to_string(), field_type::BLOB))]);
    }

    #[test]
    fn test_hex_text() {
        assert_eq!(hex_text(&[0x00, 0xff, 0x10]), "00ff10");
        assert_eq!(hex_text(&[]), "");
    }

    #[test]
    fn test_datetime_format() {
        let mut b = vec![0u8; 7];
        b[..2].copy_from_slice(&2024u16.to_le_bytes());
        b[2] = 5;
        b[3] = 6;
        b[4] = 7;
        b[5] = 8;
        b[6] = 9;
        assert_eq!(format_datetime(&b), "2024-05-06 07:08:09");
    }
}
