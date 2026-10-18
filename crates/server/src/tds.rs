//! TDS (SQL Server) 线协议前端（TDS 7.2 – 7.4）。
//!
//! 对应 Java 版 `io.mycat.protocol.tds.TDSProtocolHandler` + `TDSResponse`，
//! 并修复/补齐 Java 版的缺陷：
//! - Java 版 RPC(0x03) 直接把载荷当 UTF-16 SQL 读 → JDBC 预处理语句全部失败；
//!   Pro 版实现 `sp_executesql` 参数流解析（mssql-jdbc / jTDS 预处理语句的默认路径），
//!   `?` 占位符按参数值替换后进入共享执行引擎；`sp_prepexec` 等游标类 RPC 返回明确错误
//! - Java 版行数据 NULL → 空字符串（丢失 NULL 语义）；Pro 版按规范用 0xFFFF 长度标记 NULL
//! - 登录流程与 Java 一致：PreLogin(ENCRYPT_NOT_SUP，客户端需 encrypt=false)、
//!   Login7 密码去混淆（XOR 0xA5 + nibble swap）、TDS < 7.2 拒绝、
//!   ENVCHANGE(database/collation/packet size) + LOGINACK + DONE 单包应答
//! - SQL Batch(0x01)：跳过 ALL_HEADERS 后按 UTF-16LE 解码，进入 `engine::execute_sql`
//!
//! 结果集列类型与 Java 一致统一为 NVARCHAR(4000)（值文本化）；
//! TLS/加密不支持（PreLogin 明确声明 ENCRYPT_NOT_SUP）。

use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, info, warn};

use crate::engine::{execute_sql, EngineError, EngineOutcome, EngineSession};
use crate::session::SessionContext;
use crate::state::ServerState;

// ---------------------------------------------------------------------------
// 协议常量（与 Java TDSProtocolHandler 一致）
// ---------------------------------------------------------------------------
const TDS_PRELOGIN: u8 = 0x12;
const TDS_LOGIN7: u8 = 0x10;
const TDS_SQL_BATCH: u8 = 0x01;
const TDS_RPC_REQUEST: u8 = 0x03;
const TDS_TABULAR_RESULT: u8 = 0x04;
const TDS_ATTENTION: u8 = 0x06;

const TOKEN_LOGINACK: u8 = 0xAD;
const TOKEN_ENVCHANGE: u8 = 0xE3;
const TOKEN_INFO: u8 = 0xAB;
const TOKEN_ERROR: u8 = 0xAA;
const TOKEN_DONE: u8 = 0xFD;
const TOKEN_COLMETADATA: u8 = 0x81;
const TOKEN_ROW: u8 = 0xD1;

const EOM: u8 = 0x01;
const PACKET_SIZE: usize = 4096;
/// SQL Server 2017 CU 级别版本号（LOGINACK/PreLogin 展示用）
const SERVER_VERSION: [u8; 4] = [14, 0, 0x03, 0xE8]; // 14.0.1000
const SUB_BUILD: [u8; 2] = [0x00, 0xA9]; // .169
/// Latin1_General_CI_AS_KS_WS 排序规则（Java 注释：缺失时 sqljdbc4 会在后续 RPC 上 NPE）
const COLLATION: [u8; 5] = [0x09, 0x04, 0xD0, 0x00, 0x34];

// ---------------------------------------------------------------------------
// UTF-16LE 工具
// ---------------------------------------------------------------------------

pub fn decode_utf16le(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

pub fn encode_utf16le(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() * 2);
    for u in s.encode_utf16() {
        out.extend_from_slice(&u.to_le_bytes());
    }
    out
}

/// Login7 密码去混淆：每字节先 XOR 0xA5，再交换高低 4 位。
/// （客户端混淆为：nibble swap 后 XOR 0xA5，见 MS-TDS 2.2.6.3）
pub fn unmangle_password(bytes: &[u8]) -> String {
    let plain: Vec<u8> = bytes
        .iter()
        .map(|b| {
            let x = b ^ 0xA5;
            ((x << 4) | (x >> 4)) & 0xFF
        })
        .collect();
    decode_utf16le(&plain)
}

/// 客户端方向的密码混淆（测试用）。
pub fn mangle_password(s: &str) -> Vec<u8> {
    encode_utf16le(s)
        .iter()
        .map(|b| {
            let x = ((b << 4) | (b >> 4)) & 0xFF;
            x ^ 0xA5
        })
        .collect()
}

// ---------------------------------------------------------------------------
// TDS 包分帧
// ---------------------------------------------------------------------------

/// 组一个（或多个）TDS 包。长度字段含 8 字节头。
pub fn build_packets(packet_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 16);
    let mut offset = 0usize;
    let mut packet_id = 1u8;
    loop {
        let chunk = std::cmp::min(PACKET_SIZE - 8, payload.len() - offset);
        let len = (chunk + 8) as u16;
        out.push(packet_type);
        let is_last = offset + chunk >= payload.len();
        out.push(if is_last { EOM } else { 0 });
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&[0, 0]); // SPID
        out.push(packet_id);
        out.push(0); // window
        out.extend_from_slice(&payload[offset..offset + chunk]);
        offset += chunk;
        packet_id = packet_id.wrapping_add(1);
        if is_last {
            break;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Token 构造
// ---------------------------------------------------------------------------

fn token_done(status: u16, curcmd: u16, rowcount: u64, out: &mut Vec<u8>) {
    out.push(TOKEN_DONE);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&curcmd.to_le_bytes());
    out.extend_from_slice(&rowcount.to_le_bytes()); // TDS 7.2+ 为 8 字节
}

fn token_msg(
    token: u8,
    number: u32,
    state: u8,
    class: u8,
    message: &str,
    server: &str,
    out: &mut Vec<u8>,
) {
    let msg = encode_utf16le(message);
    let srv = encode_utf16le(server);
    // 长度 = number(4)+state(1)+class(1)+msglen(2)+msg+srvlen(1)+srv+proclen(1)+line(4)
    let len = 4 + 1 + 1 + 2 + msg.len() + 1 + srv.len() + 1 + 4;
    out.push(token);
    out.extend_from_slice(&(len as u16).to_le_bytes());
    out.extend_from_slice(&number.to_le_bytes());
    out.push(state);
    out.push(class);
    out.extend_from_slice(&((msg.len() / 2) as u16).to_le_bytes());
    out.extend_from_slice(&msg);
    out.push(server.len() as u8); // 字符数近似（ASCII 服务名）
    out.extend_from_slice(&srv);
    out.push(0); // procname len
    out.extend_from_slice(&0u32.to_le_bytes()); // line
}

pub fn token_error(number: u32, message: &str, out: &mut Vec<u8>) {
    token_msg(TOKEN_ERROR, number, 1, 16, message, "Mycat3-Pro-TDS", out);
}

fn token_info(number: u32, message: &str, out: &mut Vec<u8>) {
    token_msg(TOKEN_INFO, number, 1, 10, message, "Mycat3-Pro-TDS", out);
}

fn token_envchange_string(kind: u8, new: &str, old: &str, out: &mut Vec<u8>) {
    let n = encode_utf16le(new);
    let o = encode_utf16le(old);
    // ENVCHANGE Length 为 u16 LE（MS-TDS 2.2.7.3；Java buildEnvChangeBVarcharPair 同）
    let len = (1 + 1 + n.len() + 1 + o.len()) as u16;
    out.push(TOKEN_ENVCHANGE);
    out.extend_from_slice(&len.to_le_bytes());
    out.push(kind);
    out.push(new.chars().count() as u8);
    out.extend_from_slice(&n);
    out.push(old.chars().count() as u8);
    out.extend_from_slice(&o);
}

fn token_envchange_collation(out: &mut Vec<u8>) {
    // 注意：真实 SQL Server 登录应答中 collation 的 OldValue 长度为 0。
    // Java 版在 OldValue 重复写 5 字节 collation，ODBC Driver 13 会报
    // "Protocol error in TDS stream"（实测二分定位）；Pro 版按真实服务器行为修正。
    out.push(TOKEN_ENVCHANGE);
    out.extend_from_slice(&(8u16).to_le_bytes()); // 1+1+5+1，u16 LE
    out.push(7); // SQL_COLLATION
    out.push(5);
    out.extend_from_slice(&COLLATION);
    out.push(0); // OldValue 长度为 0
}

fn token_loginack(client_tds_version: u32, out: &mut Vec<u8>) {
    let prog = encode_utf16le("Microsoft SQL Server");
    let len = 1 + 4 + 1 + prog.len() + 4;
    out.push(TOKEN_LOGINACK);
    out.extend_from_slice(&(len as u16).to_le_bytes());
    out.push(1); // Interface: SQL
                 // 注意：MS-TDS 的字节序陷阱 —— Login7 请求中的 TDSVersion 是小端，
                 // 而 LOGINACK 应答中的 TDSVersion 是大端（实测 ODBC Driver 13 校验此字节序）
    out.extend_from_slice(&client_tds_version.to_be_bytes());
    out.push((prog.len() / 2) as u8);
    out.extend_from_slice(&prog);
    out.extend_from_slice(&SERVER_VERSION);
    let _ = SUB_BUILD;
}

/// PreLogin 响应：VERSION + ENCRYPTION(NOT_SUP) + 终止符。
pub fn build_prelogin_response() -> Vec<u8> {
    // 选项表：VERSION(off=11,len=6) ENCRYPTION(off=17,len=1) 0xFF；数据紧随其后
    let mut payload = Vec::new();
    payload.push(0x00); // VERSION token
    payload.extend_from_slice(&11u16.to_be_bytes());
    payload.extend_from_slice(&6u16.to_be_bytes());
    payload.push(0x01); // ENCRYPTION token
    payload.extend_from_slice(&17u16.to_be_bytes());
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.push(0xFF); // terminator
                        // VERSION: UL_VERSION(4) + US_SUBBUILD(2)
    payload.extend_from_slice(&SERVER_VERSION);
    payload.extend_from_slice(&SUB_BUILD);
    // ENCRYPTION: 0x02 = ENCRYPT_NOT_SUP
    payload.push(0x02);
    payload
}

/// COLMETADATA：count + 每列 NVARCHAR(4000)。
fn token_colmetadata(columns: &[String], out: &mut Vec<u8>) {
    out.push(TOKEN_COLMETADATA);
    out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for name in columns {
        out.extend_from_slice(&0u32.to_le_bytes()); // usertype (7.2+)
        out.extend_from_slice(&0x0008u16.to_le_bytes()); // flags: nullable
        out.push(0xE7); // NVARCHAR
        out.extend_from_slice(&8000u16.to_le_bytes()); // maxlen 4000 字符
        out.extend_from_slice(&COLLATION);
        let n = encode_utf16le(name);
        out.push(name.chars().count().min(255) as u8);
        out.extend_from_slice(&n);
    }
}

/// ROW：每列 u16LE 字节长度 + UTF-16LE；NULL = 0xFFFF（修复 Java 版丢失 NULL 的问题）。
fn token_row(values: &[Option<String>], out: &mut Vec<u8>) {
    out.push(TOKEN_ROW);
    for v in values {
        match v {
            None => out.extend_from_slice(&0xFFFFu16.to_le_bytes()),
            Some(s) => {
                let bytes = encode_utf16le(s);
                out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Login7 解析
// ---------------------------------------------------------------------------

pub struct Login7 {
    pub tds_version: u32,
    pub username: String,
    pub password: String,
    pub database: String,
    pub app_name: String,
    pub hostname: String,
}

/// 解析 Login7 载荷（MS-TDS 2.2.6.3）。
/// offset/length 对自 body 偏移 36 起：HostName(36) UserName(40) Password(44)
/// AppName(48) ServerName(52) …… Database(68)。
pub fn parse_login7(body: &[u8]) -> Result<Login7, EngineError> {
    if body.len() < 94 {
        return Err(EngineError::new(18456, "28000", "Login7 packet too short"));
    }
    // TDSVersion 为小端存储（MS-TDS 2.2.6.3；实测 ODBC/JDBC 驱动均如此。
    // Java 版按 BE 读取会把 7.4(0x74000004 LE 字节 04 00 00 74) 误判为低版本拒绝）
    let tds_version = u32::from_le_bytes([body[4], body[5], body[6], body[7]]);
    let rd = |off: usize| -> (usize, usize) {
        let o = u16::from_le_bytes([body[off], body[off + 1]]) as usize;
        let l = u16::from_le_bytes([body[off + 2], body[off + 3]]) as usize;
        (o, l)
    };
    let read_chars = |off: usize, chars: usize, password: bool| -> String {
        if chars == 0 || off == 0 || off >= body.len() {
            return String::new();
        }
        let end = (off + chars * 2).min(body.len());
        let bytes = &body[off..end];
        if password {
            unmangle_password(bytes)
        } else {
            decode_utf16le(bytes)
        }
    };
    let (ho, hl) = rd(36);
    let (uo, ul) = rd(40);
    let (po, pl) = rd(44);
    let (ao, al) = rd(48);
    let (dbo, dbl) = rd(68);
    let mut login = Login7 {
        tds_version,
        username: read_chars(uo, ul, false),
        password: read_chars(po, pl, true),
        database: read_chars(dbo, dbl, false),
        app_name: read_chars(ao, al, false),
        hostname: read_chars(ho, hl, false),
    };
    login.username = login.username.trim_end_matches('\0').to_string();
    login.password = login.password.trim_end_matches('\0').to_string();
    Ok(login)
}

// ---------------------------------------------------------------------------
// SQL Batch / RPC 解析
// ---------------------------------------------------------------------------

/// SQL Batch：跳过 ALL_HEADERS 后为 UTF-16LE SQL。
pub fn parse_sql_batch(payload: &[u8]) -> String {
    if payload.len() < 4 {
        return decode_utf16le(payload);
    }
    let total = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
    if total >= 4 && total <= payload.len() {
        decode_utf16le(&payload[total..])
    } else {
        decode_utf16le(payload)
    }
}

/// RPC 解析结果。
pub enum RpcRequest {
    /// sp_executesql：SQL + 参数值（已按声明类型解码为字符串）
    ExecSql {
        sql: String,
        values: Vec<RpcParamValue>,
    },
    /// 其他（sp_prepexec/sp_cursor* 等）：返回不支持错误
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RpcParamValue {
    Null,
    Int(i64),
    Float(f64),
    Str(String),
}

impl RpcParamValue {
    /// SQL 字面量形式（用于 ? 替换）。
    pub fn to_literal(&self) -> String {
        match self {
            RpcParamValue::Null => "NULL".to_string(),
            RpcParamValue::Int(i) => i.to_string(),
            RpcParamValue::Float(f) => f.to_string(),
            RpcParamValue::Str(s) => quote_tsql(s),
        }
    }
}

fn quote_tsql(v: &str) -> String {
    format!("'{}'", v.replace('\'', "''"))
}

/// 解析 RPC Request（MS-TDS 2.2.6.5），基础支持 sp_executesql。
pub fn parse_rpc(payload: &[u8]) -> Result<RpcRequest, EngineError> {
    let mut pos = 0usize;
    // ALL_HEADERS
    if payload.len() >= 4 {
        let total = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        if total >= 4 && total <= payload.len() {
            pos = total;
        }
    }
    if pos + 4 > payload.len() {
        return Err(EngineError::general("RPC packet truncated"));
    }
    // ProcNameSwitch
    let switch = u16::from_le_bytes([payload[pos], payload[pos + 1]]);
    pos += 2;
    let (proc_id, proc_name) = if switch == 0xFFFF {
        if pos + 2 > payload.len() {
            return Err(EngineError::general("RPC name truncated"));
        }
        let chars = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;
        let end = (pos + chars * 2).min(payload.len());
        let name = decode_utf16le(&payload[pos..end]);
        pos = end;
        (0u16, name)
    } else {
        (switch, String::new())
    };
    // OptionFlags
    pos += 2;
    let is_execsql = proc_id == 10 || proc_name.to_lowercase().contains("sp_executesql");
    let is_prepexec = proc_id == 13 || proc_name.to_lowercase().contains("sp_prepexec");
    if !is_execsql {
        let what = if is_prepexec {
            "sp_prepexec/sp_execute（服务端预处理句柄）"
        } else {
            "该存储过程 RPC"
        };
        return Ok(RpcRequest::Unsupported(format!(
            "{} 暂不支持，请配置驱动使用 sp_executesql（mssql-jdbc: prepareMethod=executesql）",
            what
        )));
    }
    // 参数流
    let mut params: Vec<(String, RpcParamValue)> = Vec::new();
    while pos < payload.len() {
        // ParamName
        if pos >= payload.len() {
            break;
        }
        let name_chars = payload[pos] as usize;
        pos += 1;
        let name_end = (pos + name_chars * 2).min(payload.len());
        let name = decode_utf16le(&payload[pos..name_end]);
        pos = name_end;
        if pos >= payload.len() {
            break;
        }
        // StatusFlag (0=INPUT, 1=OUTPUT)
        let _status = payload[pos];
        pos += 1;
        // TYPE_INFO + 值
        match parse_rpc_param(payload, &mut pos) {
            Ok(v) => params.push((name, v)),
            Err(e) => return Err(e),
        }
    }
    // sp_executesql(@stmt, @params?, 值...)
    let mut iter = params.into_iter();
    let sql = match iter.next() {
        Some((_, RpcParamValue::Str(s))) => s,
        _ => {
            return Err(EngineError::general(
                "sp_executesql: missing statement parameter",
            ))
        }
    };
    let mut values = Vec::new();
    for (_, v) in iter {
        // 第二个字符串参数若是参数声明（@P1 int,...）则跳过
        if values.is_empty() {
            if let RpcParamValue::Str(s) = &v {
                if s.contains('@')
                    && (s.contains("int")
                        || s.contains("nvarchar")
                        || s.contains("varchar")
                        || s.contains("bigint")
                        || s.contains("datetime")
                        || s.contains("float")
                        || s.contains("bit")
                        || s.contains("decimal"))
                {
                    continue;
                }
            }
        }
        values.push(v);
    }
    Ok(RpcRequest::ExecSql { sql, values })
}

/// 解析单个 RPC 参数的 TYPE_INFO 与值。所有读取带边界检查：
/// 截断载荷返回明确错误而非索引越界 panic（恶意/异常客户端防护）。
fn parse_rpc_param(payload: &[u8], pos: &mut usize) -> Result<RpcParamValue, EngineError> {
    if *pos >= payload.len() {
        return Ok(RpcParamValue::Null);
    }
    let truncated = || EngineError::unsupported("RPC 参数载荷截断".to_string());
    let type_id = payload[*pos];
    *pos += 1;
    match type_id {
        // 变长字符串：NVARCHAR(0xE7)/VARCHAR(0xA7)/NCHAR(0xEF)/CHAR(0xAF)
        0xE7 | 0xA7 | 0xEF | 0xAF => {
            if *pos + 7 > payload.len() {
                return Err(truncated());
            }
            *pos += 2; // maxlen
            *pos += 5; // collation
            read_var_value(payload, pos, type_id == 0xE7 || type_id == 0xEF)
        }
        // 变长数值：INTN(0x26)/FLOATN(0x6D)/BITN(0x68)/NUMERICN(0x6C)/DECIMALN(0x6A)
        //          /DATETIMEN(0x6F)/DATEN(0x28)/MONEYN(0x6E)
        0x26 | 0x6D | 0x68 | 0x6C | 0x6A | 0x6F | 0x28 | 0x6E => {
            if *pos + 1 > payload.len() {
                return Err(truncated());
            }
            let max_len = payload[*pos] as usize;
            *pos += 1;
            if type_id == 0x6C || type_id == 0x6A {
                if *pos + 1 > payload.len() {
                    return Err(truncated());
                }
                *pos += 1; // scale
            }
            if *pos + 1 > payload.len() {
                return Err(truncated());
            }
            let len = payload[*pos] as usize;
            *pos += 1;
            if len == 0 {
                return Ok(RpcParamValue::Null);
            }
            if *pos + len > payload.len() {
                return Err(truncated());
            }
            let bytes = &payload[*pos..*pos + len];
            *pos += len;
            match type_id {
                0x26 => Ok(RpcParamValue::Int(match bytes.len() {
                    1 => bytes[0] as i8 as i64,
                    2 => i16::from_le_bytes([bytes[0], bytes[1]]) as i64,
                    4 => i32::from_le_bytes(bytes.try_into().map_err(|_| truncated())?) as i64,
                    8 => i64::from_le_bytes(bytes.try_into().map_err(|_| truncated())?),
                    _ => 0,
                })),
                0x6D => Ok(RpcParamValue::Float(if bytes.len() == 4 {
                    f32::from_le_bytes(bytes.try_into().map_err(|_| truncated())?) as f64
                } else {
                    f64::from_le_bytes(bytes.try_into().map_err(|_| truncated())?)
                })),
                0x68 => Ok(RpcParamValue::Int(if bytes.is_empty() {
                    0
                } else if bytes[0] != 0 {
                    1
                } else {
                    0
                })),
                _ => {
                    let _ = max_len;
                    Ok(RpcParamValue::Str(decode_utf16le(bytes)))
                }
            }
        }
        other => Err(EngineError::unsupported(format!(
            "RPC 参数类型 0x{:02X} 暂不支持",
            other
        ))),
    }
}

fn read_var_value(
    payload: &[u8],
    pos: &mut usize,
    wide: bool,
) -> Result<RpcParamValue, EngineError> {
    if *pos + 2 > payload.len() {
        return Err(EngineError::unsupported("RPC 参数载荷截断".to_string()));
    }
    let len = u16::from_le_bytes([payload[*pos], payload[*pos + 1]]);
    *pos += 2;
    if len == 0xFFFF {
        return Ok(RpcParamValue::Null);
    }
    let len = len as usize;
    if *pos + len > payload.len() {
        return Err(EngineError::unsupported("RPC 参数载荷截断".to_string()));
    }
    let bytes = &payload[*pos..*pos + len];
    *pos += len;
    Ok(RpcParamValue::Str(if wide {
        decode_utf16le(bytes)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }))
}

/// T-SQL 会话级 SET 选项（驱动初始化语句），本地吸收不下推。
const TSQL_SET_OPTIONS: &[&str] = &[
    "QUOTED_IDENTIFIER",
    "ANSI_NULLS",
    "ANSI_PADDING",
    "ANSI_WARNINGS",
    "ANSI_NULL_DFLT_ON",
    "TEXTSIZE",
    "LANGUAGE",
    "NOCOUNT",
    "CONCAT_NULL_YIELDS_NULL",
    "DATEFORMAT",
    "DATEFIRST",
    "IDENTITY_INSERT",
    "XACT_ABORT",
    "LOCK_TIMEOUT",
    "DEADLOCK_PRIORITY",
    "SHOWPLAN",
    "STATISTICS",
    "FIPS_FLAGGER",
    "ARITHABORT",
    "NUMERIC_ROUNDABORT",
    "CURSOR_CLOSE_ON_COMMIT",
    "IMPLICIT_TRANSACTIONS",
    "NOEXEC",
    "PARSEONLY",
    "FORCEPLAN",
    "ROWCOUNT",
    "RESULT_SET_CACHING",
    "CONTEXT_INFO",
    "TRANSACTION",
    "ANSI_DEFAULTS",
    "OFFSETS",
    "REMOTE_PROC_TRANSACTIONS",
];

/// 判断是否为 T-SQL 会话 SET 语句（SET <option> ON/OFF/值，且不是 MySQL 风格 set names/autocommit 等）。
pub fn is_tsql_session_set(sql: &str) -> bool {
    let norm = sql.trim().to_uppercase();
    if !norm.starts_with("SET ") {
        return false;
    }
    let rest = norm[4..].trim_start();
    // MySQL 风格会话变量保留给引擎（set autocommit=0 / set names x / set session ...）
    if rest.starts_with("AUTOCOMMIT")
        || rest.starts_with("NAMES")
        || rest.starts_with("SESSION ")
        || rest.starts_with("GLOBAL ")
    {
        return false;
    }
    TSQL_SET_OPTIONS.iter().any(|opt| rest.starts_with(opt))
}

/// 将 `?` 占位符按序替换为参数字面量（引号感知）。
pub fn substitute_tsql_params(sql: &str, values: &[RpcParamValue]) -> String {
    let mut out = String::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut seg = 0;
    let mut p = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'[' => {
                let close = if bytes[i] == b'[' { b']' } else { bytes[i] };
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && close != b']' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == close {
                        break;
                    }
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            }
            b'?' => {
                out.push_str(&sql[seg..i]);
                out.push_str(
                    &values
                        .get(p)
                        .map(|v| v.to_literal())
                        .unwrap_or_else(|| "NULL".into()),
                );
                p += 1;
                i += 1;
                seg = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&sql[seg..]);
    out
}

// ---------------------------------------------------------------------------
// T-SQL SELECT TOP 重写
// ---------------------------------------------------------------------------

/// 连续词法单元（字母/数字/下划线）的结束下标；`pos` 必须位于词首。
fn tsql_word_end(s: &str, pos: usize) -> Option<usize> {
    let b = s.as_bytes();
    let mut i = pos;
    while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
        i += 1;
    }
    (i > pos).then_some(i)
}

fn tsql_skip_ws(s: &str, mut pos: usize) -> usize {
    let b = s.as_bytes();
    while pos < b.len() && b[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

/// 第一个顶层分号（单引号字符串内的分号跳过；T-SQL 以 `''` 转义，
/// 朴素开关在 `''` 上翻转两次，恰好等价）。
fn tsql_top_level_semicolon(s: &str) -> Option<usize> {
    let mut in_str = false;
    for (i, c) in s.char_indices() {
        match c {
            '\'' => in_str = !in_str,
            ';' if !in_str => return Some(i),
            _ => {}
        }
    }
    None
}

/// 重写 `SELECT [ALL|DISTINCT] TOP (n) …` → `SELECT … LIMIT n`（MySQL/Oracle 方言）。
/// - 仅处理语句最外层首 SELECT；`TOP n PERCENT` / `WITH TIES` 不重写（交由后端报错）；
/// - LIMIT 插在第一个顶层分号前，多语句批中仍归属首语句。
pub fn rewrite_tsql_top(sql: &str) -> String {
    let rest = sql.trim_start();
    let lead = &sql[..sql.len() - rest.len()];
    let Some(sel_end) = tsql_word_end(rest, 0) else {
        return sql.to_string();
    };
    if !rest[..sel_end].eq_ignore_ascii_case("SELECT") {
        return sql.to_string();
    }
    // SELECT [ALL|DISTINCT] TOP …
    let mut pos = tsql_skip_ws(rest, sel_end);
    let mut saw_modifier = false;
    let (top_start, after_num, n_str) = loop {
        let Some(end) = tsql_word_end(rest, pos) else {
            return sql.to_string();
        };
        let tok = &rest[pos..end];
        if !saw_modifier
            && (tok.eq_ignore_ascii_case("ALL") || tok.eq_ignore_ascii_case("DISTINCT"))
        {
            saw_modifier = true;
            pos = tsql_skip_ws(rest, end);
            continue;
        }
        if !tok.eq_ignore_ascii_case("TOP") {
            return sql.to_string();
        }
        // TOP 后必须是空白或 '('（"top10" 等单 token 已由词法切分排除）
        let b = rest.as_bytes();
        let mut npos = tsql_skip_ws(rest, end);
        let mut paren = false;
        if npos < b.len() && b[npos] == b'(' {
            paren = true;
            npos = tsql_skip_ws(rest, npos + 1);
        }
        let Some(nend) = tsql_word_end(rest, npos) else {
            return sql.to_string();
        };
        if !rest[npos..nend].chars().all(|c| c.is_ascii_digit()) {
            return sql.to_string();
        }
        let n_str = rest[npos..nend].to_string();
        let mut after_num = nend;
        if paren {
            after_num = tsql_skip_ws(rest, after_num);
            if after_num >= b.len() || b[after_num] != b')' {
                return sql.to_string();
            }
            after_num += 1;
        }
        // PERCENT / WITH TIES 语义无法用 LIMIT 表达 → 不重写
        let probe = tsql_skip_ws(rest, after_num);
        if let Some(pe) = tsql_word_end(rest, probe) {
            let t = &rest[probe..pe];
            if t.eq_ignore_ascii_case("PERCENT") || t.eq_ignore_ascii_case("WITH") {
                return sql.to_string();
            }
        }
        break (pos, after_num, n_str);
    };
    // 剔除 "TOP n"，再插入 LIMIT
    let mut body = String::with_capacity(rest.len() + 16);
    body.push_str(&rest[..top_start]);
    body.push_str(&rest[after_num..]);
    let limit = format!(" LIMIT {}", n_str);
    let insert_at = tsql_top_level_semicolon(&body).unwrap_or(body.len());
    body.insert_str(insert_at, &limit);
    let mut out = String::with_capacity(sql.len() + 16);
    out.push_str(lead);
    out.push_str(&body);
    out
}

// ---------------------------------------------------------------------------
// 连接处理
// ---------------------------------------------------------------------------

pub struct TdsFrontend {
    socket: TcpStream,
    state: Arc<ServerState>,
    sess: EngineSession,
    packet_id: u8,
    proc: Option<Arc<crate::state::ProcessInfo>>,
}

#[derive(Debug, thiserror::Error)]
enum TdsError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("closed")]
    Closed,
}

pub async fn serve_tds_connection(
    state: Arc<ServerState>,
    socket: TcpStream,
    prefeed: Vec<u8>,
    kill_handle: Option<std::net::TcpStream>,
) {
    let peer = socket
        .peer_addr()
        .map(|a| a.ip().to_string())
        .unwrap_or_default();
    let conn_id = state.next_connection_id();
    state.stats.incr_conn();
    let ctx = SessionContext::new(conn_id, peer.clone());
    let mut fe = TdsFrontend {
        socket,
        state: state.clone(),
        sess: EngineSession::new(ctx),
        packet_id: 1,
        proc: None,
    };
    info!(conn_id, peer = %peer, "sqlserver(TDS) client connected");
    let result = fe.run(prefeed, kill_handle).await;
    if let Err(e) = result {
        debug!("tds connection closed: {}", e);
    }
    state.processes.unregister(conn_id);
    if !fe.sess.ctx.user.is_empty() {
        state.decr_user_conn(&fe.sess.ctx.user);
    }
    fe.sess.close().await;
    state.stats.decr_conn();
}

impl TdsFrontend {
    async fn run(
        &mut self,
        mut prefeed: Vec<u8>,
        mut kill_handle: Option<std::net::TcpStream>,
    ) -> Result<(), TdsError> {
        // ---- PreLogin ----
        let (ptype, payload) = self.read_packet(&mut prefeed).await?;
        if ptype != TDS_PRELOGIN {
            // 客户端直接发 Login7（部分老驱动跳过 PreLogin）
            if ptype == TDS_LOGIN7 {
                self.handle_login(&payload, &mut kill_handle).await?;
            } else {
                return Ok(());
            }
        } else {
            let resp = build_prelogin_response();
            self.send(TDS_TABULAR_RESULT, &resp).await?;
            let (_, payload) = self.read_packet(&mut Vec::new()).await?;
            self.handle_login(&payload, &mut kill_handle).await?;
        }

        // ---- 命令循环 ----
        loop {
            let (ptype, payload) = self.read_packet(&mut Vec::new()).await?;
            // pauseServer 挂起：排队等待 resumeServer
            crate::state::wait_if_paused(self.proc.as_deref()).await;
            let started = Instant::now();
            match ptype {
                TDS_SQL_BATCH => {
                    let sql = parse_sql_batch(&payload);
                    if let Some(proc) = &self.proc {
                        proc.set_command("Query");
                        proc.set_sql(Some(sql.chars().take(1024).collect()));
                        proc.set_db(self.sess.ctx.schema.clone());
                    }
                    self.execute(&sql).await?;
                }
                TDS_RPC_REQUEST => match parse_rpc(&payload) {
                    Ok(RpcRequest::ExecSql { sql, values }) => {
                        let final_sql = substitute_tsql_params(&sql, &values);
                        self.execute(&final_sql).await?;
                    }
                    Ok(RpcRequest::Unsupported(msg)) => {
                        let mut out = Vec::new();
                        token_error(1, &msg, &mut out);
                        token_done(0, 0, 0, &mut out);
                        self.send(TDS_TABULAR_RESULT, &out).await?;
                    }
                    Err(e) => {
                        let mut out = Vec::new();
                        token_error(1, &e.message, &mut out);
                        token_done(0, 0, 0, &mut out);
                        self.send(TDS_TABULAR_RESULT, &out).await?;
                    }
                },
                TDS_ATTENTION => {
                    warn!("tds attention received (cancel not supported)");
                    let mut out = Vec::new();
                    token_done(0x0020, 0, 0, &mut out); // DONE 带 attention ack
                    self.send(TDS_TABULAR_RESULT, &out).await?;
                }
                other => {
                    warn!("tds: unsupported packet type 0x{:02X}", other);
                    let mut out = Vec::new();
                    token_error(
                        1,
                        &format!("unsupported TDS packet type 0x{:02X}", other),
                        &mut out,
                    );
                    token_done(0, 0, 0, &mut out);
                    self.send(TDS_TABULAR_RESULT, &out).await?;
                }
            }
            if started.elapsed().as_millis() > 1000 {
                self.state.stats.incr_slow();
            }
        }
    }

    /// Login7：认证 + ENVCHANGE/LOGINACK/DONE 应答。
    async fn handle_login(
        &mut self,
        payload: &[u8],
        kill_handle: &mut Option<std::net::TcpStream>,
    ) -> Result<(), TdsError> {
        let login = match parse_login7(payload) {
            Ok(l) => l,
            Err(e) => {
                let mut out = Vec::new();
                token_error(e.errno as u32, &e.message, &mut out);
                token_done(0, 0, 0, &mut out);
                self.send(TDS_TABULAR_RESULT, &out).await?;
                return Err(TdsError::Closed);
            }
        };
        // TDS < 7.2 拒绝（与 Java 一致）
        if login.tds_version != 0 && login.tds_version < 0x7209_0002 {
            let mut out = Vec::new();
            token_error(
                18456,
                "TDS version below 7.2 is not supported (use a modern JDBC/ODBC driver)",
                &mut out,
            );
            token_done(0, 0, 0, &mut out);
            self.send(TDS_TABULAR_RESULT, &out).await?;
            return Err(TdsError::Closed);
        }
        let tds_version = if login.tds_version == 0 {
            0x7400_0004
        } else {
            login.tds_version.min(0x7400_0004)
        };

        // 认证：users 配置明文密码比对（与 Java MycatProtocolBackend.authenticate 一致）
        let cfg = self.state.config();
        let user_cfg = cfg.users.get(&login.username).cloned();
        drop(cfg);
        let authorized = match &user_cfg {
            Some(u) => {
                u.ip_allowed(&self.sess.ctx.client_ip) && u.decrypted_password() == login.password
            }
            None => false,
        };
        if !authorized {
            warn!(user = %login.username, "tds login failed");
            let mut out = Vec::new();
            token_error(
                18456,
                &format!("Login failed for user '{}'.", login.username),
                &mut out,
            );
            token_done(0, 0, 0, &mut out);
            self.send(TDS_TABULAR_RESULT, &out).await?;
            return Err(TdsError::Closed);
        }
        self.sess.ctx.user = login.username.clone();
        if let Some(u) = &user_cfg {
            if u.login_limit > 0
                && self.state.user_conn_count(&login.username) >= u.login_limit as u32
            {
                let mut out = Vec::new();
                token_error(18456, "user exceeds login limit", &mut out);
                token_done(0, 0, 0, &mut out);
                self.send(TDS_TABULAR_RESULT, &out).await?;
                return Err(TdsError::Closed);
            }
        }
        self.state.incr_user_conn(&login.username);
        if let Some(u) = &user_cfg {
            self.sess.ctx.transaction_policy = u.transaction_type.clone();
        }
        // 初始库：Login7 database 字段优先，其次用户配置默认 schema
        if !login.database.is_empty() {
            self.sess.ctx.schema = Some(login.database.clone());
        } else if let Some(u) = &user_cfg {
            self.sess.ctx.schema = u
                .schema
                .clone()
                .or_else(|| u.schemas.first().cloned())
                .or(self.sess.ctx.schema.take());
        }
        let schema = self.sess.ctx.schema.clone().unwrap_or_default();

        // 应答：ENVCHANGE(db) + INFO(5701) + ENVCHANGE(language) + INFO(5703)
        //       + LOGINACK + ENVCHANGE(collation) + ENVCHANGE(packet size) + DONE
        let mut out = Vec::new();
        token_envchange_string(1, &schema, "master", &mut out);
        token_info(
            5701,
            &format!("Changed database context to '{}'.", schema),
            &mut out,
        );
        token_envchange_string(2, "us_english", "us_english", &mut out);
        token_info(5703, "Changed language setting to us_english.", &mut out);
        token_loginack(tds_version, &mut out);
        token_envchange_collation(&mut out);
        token_envchange_string(4, "4096", "4096", &mut out);
        token_done(0, 0, 0, &mut out);
        self.send(TDS_TABULAR_RESULT, &out).await?;
        info!(user = %login.username, database = %schema, "sqlserver(TDS) client authenticated");
        if self.proc.is_none() {
            let proc = self.state.processes.register(
                self.sess.ctx.connection_id,
                "TDS",
                self.sess.ctx.client_ip.clone(),
                kill_handle.take(),
            );
            proc.set_user(login.username.clone());
            proc.set_db(self.sess.ctx.schema.clone());
            self.proc = Some(proc);
        }
        Ok(())
    }

    /// 执行 SQL 并回写 tabular 结果。
    async fn execute(&mut self, sql: &str) -> Result<(), TdsError> {
        // T-SQL 会话选项（ODBC/JDBC 驱动连接后自动发送的初始化语句）：
        // 这些设置在 MySQL 后端无对应语义，直接本地吸收并回 DONE（Babelfish 同类做法）。
        // 否则 sqlparser 解析失败会导致 ODBC 认为登录阶段失败（Login timeout）。
        if is_tsql_session_set(sql) {
            let mut out = Vec::new();
            token_done(0x0010, 0xC1, 0, &mut out);
            self.send(TDS_TABULAR_RESULT, &out).await?;
            return Ok(());
        }
        let sql = rewrite_tsql_top(sql);
        match execute_sql(&self.state, &mut self.sess, &sql).await {
            Ok(outcomes) => {
                let mut out = Vec::new();
                let last = outcomes.len().saturating_sub(1);
                for (i, outcome) in outcomes.iter().enumerate() {
                    let more = i < last;
                    match outcome {
                        EngineOutcome::Rows { columns, rows } => {
                            let names: Vec<String> =
                                columns.iter().map(|c| c.name.clone()).collect();
                            token_colmetadata(&names, &mut out);
                            for row in rows {
                                token_row(row, &mut out);
                            }
                            token_done(
                                if more { 0x0001 } else { 0x0010 },
                                0xC1,
                                rows.len() as u64,
                                &mut out,
                            );
                        }
                        EngineOutcome::Ok { affected_rows, .. } => {
                            token_done(
                                if more { 0x0011 } else { 0x0010 },
                                0xC1,
                                *affected_rows,
                                &mut out,
                            );
                        }
                    }
                }
                self.send(TDS_TABULAR_RESULT, &out).await?;
            }
            Err(e) => {
                self.state.stats.incr_error();
                let mut out = Vec::new();
                token_error(
                    e.errno as u32,
                    &format!("[{}] {}", e.sqlstate, e.message),
                    &mut out,
                );
                token_done(0, 0, 0, &mut out);
                self.send(TDS_TABULAR_RESULT, &out).await?;
            }
        }
        Ok(())
    }

    // ---- IO ----
    async fn read_packet(&mut self, prefeed: &mut Vec<u8>) -> Result<(u8, Vec<u8>), TdsError> {
        // 头 8 字节
        let mut head = [0u8; 8];
        let mut filled = 0usize;
        if !prefeed.is_empty() {
            let take = std::cmp::min(8, prefeed.len());
            head[..take].copy_from_slice(&prefeed[..take]);
            if prefeed.len() > take {
                let rest = prefeed.split_off(take);
                *prefeed = rest;
            } else {
                prefeed.clear();
            }
            filled = take;
        }
        while filled < 8 {
            let n = self.socket.read(&mut head[filled..]).await?;
            if n == 0 {
                return Err(TdsError::Closed);
            }
            filled += n;
        }
        let ptype = head[0];
        let len = u16::from_be_bytes([head[2], head[3]]) as usize;
        if len < 8 || len > 64 * 1024 * 1024 {
            return Err(TdsError::Closed);
        }
        let mut payload = vec![0u8; len - 8];
        let mut filled = 0usize;
        if !prefeed.is_empty() {
            let take = std::cmp::min(payload.len(), prefeed.len());
            payload[..take].copy_from_slice(&prefeed[..take]);
            if prefeed.len() > take {
                let rest = prefeed.split_off(take);
                *prefeed = rest;
            } else {
                prefeed.clear();
            }
            filled = take;
        }
        while filled < payload.len() {
            let n = self.socket.read(&mut payload[filled..]).await?;
            if n == 0 {
                return Err(TdsError::Closed);
            }
            filled += n;
        }
        self.state.stats.add_in((len) as u64);
        // 多包消息累积（status 无 EOM 时继续读）
        if head[1] & EOM == 0 {
            let (next_type, mut rest) = Box::pin(self.read_packet(&mut Vec::new())).await?;
            let _ = next_type;
            payload.append(&mut rest);
        }
        Ok((ptype, payload))
    }

    async fn send(&mut self, ptype: u8, payload: &[u8]) -> Result<(), TdsError> {
        let wire = build_packets(ptype, payload);
        self.socket.write_all(&wire).await?;
        self.socket.flush().await?;
        self.state.stats.add_out(wire.len() as u64);
        self.packet_id = self.packet_id.wrapping_add(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_password_mangle_roundtrip() {
        for pw in ["", "Pass123", "p@ss'w\"ord", "密码abc"] {
            let mangled = mangle_password(pw);
            assert_eq!(unmangle_password(&mangled), pw);
        }
        // 已知向量：TDS 规范示例 "P" (0x50,0x00) → 混淆后 0xF3,0x5A? 验证算法自洽即可
        let m = mangle_password("P");
        assert_eq!(m.len(), 2);
        assert_eq!(unmangle_password(&m), "P");
    }

    #[test]
    fn test_utf16_codec() {
        assert_eq!(decode_utf16le(&encode_utf16le("select 1")), "select 1");
        assert_eq!(decode_utf16le(&[0x60, 0x4F]), "你"); // U+4F60 LE
    }

    #[test]
    fn test_build_packets_single_and_split() {
        let p = build_packets(TDS_TABULAR_RESULT, b"hello");
        assert_eq!(p[0], TDS_TABULAR_RESULT);
        assert_eq!(p[1], EOM);
        assert_eq!(u16::from_be_bytes([p[2], p[3]]), 13); // 8 + 5
                                                          // 超 4088 字节载荷 → 多包，仅最后一包 EOM
        let big = vec![7u8; PACKET_SIZE - 8 + 10]; // 4098 → 两包（4088 + 10）
        let p = build_packets(TDS_TABULAR_RESULT, &big);
        assert_eq!(p[1], 0); // 首包非 EOM
        let first_len = u16::from_be_bytes([p[2], p[3]]) as usize;
        assert_eq!(first_len, PACKET_SIZE);
        let second_start = first_len;
        assert_eq!(p[second_start], TDS_TABULAR_RESULT);
        assert_eq!(p[second_start + 1], EOM); // 尾包 EOM
        assert_eq!(
            u16::from_be_bytes([p[second_start + 2], p[second_start + 3]]),
            18
        );
    }

    #[test]
    fn test_prelogin_response_layout() {
        let r = build_prelogin_response();
        // VERSION 选项：token 0，offset 11，len 6
        assert_eq!(r[0], 0x00);
        assert_eq!(u16::from_be_bytes([r[1], r[2]]), 11);
        assert_eq!(u16::from_be_bytes([r[3], r[4]]), 6);
        // ENCRYPTION 选项：token 1，offset 17，len 1
        assert_eq!(r[5], 0x01);
        assert_eq!(u16::from_be_bytes([r[6], r[7]]), 17);
        // 终止符
        assert_eq!(r[10], 0xFF);
        // 版本 14.0.1000.169
        assert_eq!(&r[11..15], &SERVER_VERSION);
        assert_eq!(&r[15..17], &SUB_BUILD);
        // ENCRYPT_NOT_SUP
        assert_eq!(r[17], 0x02);
    }

    fn build_login7(user: &str, password: &str, tds_version: u32) -> Vec<u8> {
        // 固定部分 94 字节，数据区依次：HostName、UserName、Password
        let mut body = vec![0u8; 94];
        body[4..8].copy_from_slice(&tds_version.to_le_bytes());
        let host = encode_utf16le("testhost");
        let uname = encode_utf16le(user);
        let pwd = mangle_password(password);
        let base = 94usize;
        let host_off = base;
        let user_off = host_off + host.len();
        let pwd_off = user_off + uname.len();
        let put = |b: &mut [u8], off: usize, o: usize, l: usize| {
            b[off..off + 2].copy_from_slice(&(o as u16).to_le_bytes());
            b[off + 2..off + 4].copy_from_slice(&(l as u16).to_le_bytes());
        };
        put(&mut body, 36, host_off, 8);
        put(&mut body, 40, user_off, user.chars().count());
        put(&mut body, 44, pwd_off, pwd.len() / 2);
        body.extend_from_slice(&host);
        body.extend_from_slice(&uname);
        body.extend_from_slice(&pwd);
        body
    }

    #[test]
    fn test_parse_login7_roundtrip() {
        let body = build_login7("sa", "Secret1!", 0x7400_0004);
        let l = parse_login7(&body).unwrap();
        assert_eq!(l.username, "sa");
        assert_eq!(l.password, "Secret1!");
        assert_eq!(l.tds_version, 0x7400_0004);
    }

    #[test]
    fn test_parse_sql_batch() {
        // ALL_HEADERS: total=8（仅头）后接 UTF-16 SQL
        let mut payload = Vec::new();
        payload.extend_from_slice(&8u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes()); // 头内容占位
        payload.extend_from_slice(&encode_utf16le("select * from t"));
        assert_eq!(parse_sql_batch(&payload), "select * from t");
        // 无头兼容
        assert_eq!(parse_sql_batch(&encode_utf16le("select 1")), "select 1");
    }

    #[test]
    fn test_rewrite_tsql_top() {
        assert_eq!(
            rewrite_tsql_top("select top 5 id, name from t order by id"),
            "select  id, name from t order by id LIMIT 5"
        );
        assert_eq!(
            rewrite_tsql_top("SELECT DISTINCT TOP (10) a FROM t"),
            "SELECT DISTINCT  a FROM t LIMIT 10"
        );
        // 多语句批：LIMIT 归属首语句
        assert_eq!(
            rewrite_tsql_top("select top 3 * from t; select 1"),
            "select  * from t LIMIT 3; select 1"
        );
        // 字符串内分号不截断
        assert_eq!(
            rewrite_tsql_top("select top 2 * from t where name = 'a;b'"),
            "select  * from t where name = 'a;b' LIMIT 2"
        );
        // 尾随分号
        assert_eq!(
            rewrite_tsql_top("select top 7 a from t;"),
            "select  a from t LIMIT 7;"
        );
        // 不支持/无需重写的结构原样返回
        assert_eq!(rewrite_tsql_top("select * from t"), "select * from t");
        assert_eq!(
            rewrite_tsql_top("select top 5 percent * from t"),
            "select top 5 percent * from t"
        );
        assert_eq!(rewrite_tsql_top("select topx from t"), "select topx from t");
        assert_eq!(
            rewrite_tsql_top("select top @n from t"),
            "select top @n from t"
        );
        assert_eq!(
            rewrite_tsql_top("insert into t select top 1 * from s"),
            "insert into t select top 1 * from s"
        );
        // 前导空白保留
        assert_eq!(
            rewrite_tsql_top("  select top 1 a from t"),
            "  select  a from t LIMIT 1"
        );
    }

    /// 构造 sp_executesql RPC 载荷：@stmt NVARCHAR + @params NVARCHAR + 值
    fn build_rpc_execsql(sql: &str, decl: Option<&str>, values: &[RpcParamValue]) -> Vec<u8> {
        let mut payload = Vec::new();
        // ALL_HEADERS: total=8
        payload.extend_from_slice(&8u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        // ProcNameSwitch = 10 (sp_executesql)
        payload.extend_from_slice(&10u16.to_le_bytes());
        // OptionFlags
        payload.extend_from_slice(&0u16.to_le_bytes());
        let push_nvarchar = |name: &str, v: &str, out: &mut Vec<u8>| {
            out.push(name.chars().count() as u8);
            out.extend_from_slice(&encode_utf16le(name));
            out.push(0); // status: input
            out.push(0xE7); // NVARCHAR
            out.extend_from_slice(&8000u16.to_le_bytes());
            out.extend_from_slice(&COLLATION);
            let bytes = encode_utf16le(v);
            out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            out.extend_from_slice(&bytes);
        };
        push_nvarchar("@stmt", sql, &mut payload);
        if let Some(d) = decl {
            push_nvarchar("@params", d, &mut payload);
        }
        for (i, v) in values.iter().enumerate() {
            let name = format!("@P{}", i + 1);
            match v {
                RpcParamValue::Int(n) => {
                    payload.push(name.chars().count() as u8);
                    payload.extend_from_slice(&encode_utf16le(&name));
                    payload.push(0);
                    payload.push(0x26); // INTN
                    payload.push(8); // maxlen
                    if n == &i64::MIN {
                        payload.push(0); // NULL
                    } else {
                        payload.push(8);
                        payload.extend_from_slice(&n.to_le_bytes());
                    }
                }
                RpcParamValue::Str(s) => push_nvarchar(&name, s, &mut payload),
                RpcParamValue::Null => {
                    payload.push(name.chars().count() as u8);
                    payload.extend_from_slice(&encode_utf16le(&name));
                    payload.push(0);
                    payload.push(0xE7);
                    payload.extend_from_slice(&8000u16.to_le_bytes());
                    payload.extend_from_slice(&COLLATION);
                    payload.extend_from_slice(&0xFFFFu16.to_le_bytes());
                }
                RpcParamValue::Float(_) => unimplemented!("test"),
            }
        }
        payload
    }

    #[test]
    fn test_parse_rpc_execsql() {
        let payload = build_rpc_execsql(
            "select * from orders where id=? and name=?",
            Some("@P1 bigint,@P2 nvarchar(4000)"),
            &[RpcParamValue::Int(5), RpcParamValue::Str("O'Brien".into())],
        );
        match parse_rpc(&payload).unwrap() {
            RpcRequest::ExecSql { sql, values } => {
                assert_eq!(sql, "select * from orders where id=? and name=?");
                assert_eq!(
                    values,
                    vec![RpcParamValue::Int(5), RpcParamValue::Str("O'Brien".into())]
                );
                let final_sql = substitute_tsql_params(&sql, &values);
                assert_eq!(
                    final_sql,
                    "select * from orders where id=5 and name='O''Brien'"
                );
            }
            other => panic!("expected ExecSql, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn test_parse_rpc_no_decl() {
        let payload = build_rpc_execsql("select ?", None, &[RpcParamValue::Int(9)]);
        match parse_rpc(&payload).unwrap() {
            RpcRequest::ExecSql { values, .. } => assert_eq!(values, vec![RpcParamValue::Int(9)]),
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_rpc_null_param() {
        let payload = build_rpc_execsql("select ?", None, &[RpcParamValue::Null]);
        match parse_rpc(&payload).unwrap() {
            RpcRequest::ExecSql { values, .. } => {
                assert_eq!(values, vec![RpcParamValue::Null]);
                assert_eq!(substitute_tsql_params("select ?", &values), "select NULL");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_parse_rpc_prepexec_unsupported() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&8u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&13u16.to_le_bytes()); // sp_prepexec
        payload.extend_from_slice(&0u16.to_le_bytes());
        match parse_rpc(&payload).unwrap() {
            RpcRequest::Unsupported(msg) => assert!(msg.contains("sp_executesql")),
            _ => panic!(),
        }
    }

    #[test]
    fn test_substitute_quoted() {
        // 引号内 ? 不替换
        assert_eq!(
            substitute_tsql_params("select '?' , ?", &[RpcParamValue::Int(1)]),
            "select '?' , 1"
        );
        // 方括号标识符内不替换
        assert_eq!(
            substitute_tsql_params("select [a?b] where c=?", &[RpcParamValue::Int(2)]),
            "select [a?b] where c=2"
        );
    }

    #[test]
    fn test_tsql_session_set_detection() {
        assert!(is_tsql_session_set("SET QUOTED_IDENTIFIER OFF"));
        assert!(is_tsql_session_set("set ansi_nulls on"));
        assert!(is_tsql_session_set("SET TEXTSIZE 2147483647"));
        assert!(is_tsql_session_set("SET NOCOUNT ON"));
        // MySQL 风格不吸收
        assert!(!is_tsql_session_set("set autocommit=0"));
        assert!(!is_tsql_session_set("SET NAMES utf8mb4"));
        assert!(!is_tsql_session_set("set session sql_mode=''"));
        assert!(!is_tsql_session_set("select 1"));
    }

    #[test]
    fn test_row_null_encoding() {
        let mut out = Vec::new();
        token_row(&[None, Some("a".into())], &mut out);
        assert_eq!(out[0], TOKEN_ROW);
        assert_eq!(&out[1..3], &0xFFFFu16.to_le_bytes()); // NULL
        assert_eq!(u16::from_le_bytes([out[3], out[4]]), 2); // "a" UTF-16 = 2 字节
        assert_eq!(out[5], b'a');
    }

    #[test]
    fn test_collation_envchange_old_zero() {
        // ODBC Driver 13 要求 collation ENVCHANGE 的 OldValue 长度为 0（真实 SQL Server 行为）
        let mut out = Vec::new();
        token_envchange_collation(&mut out);
        assert_eq!(out[0], TOKEN_ENVCHANGE);
        let len = u16::from_le_bytes([out[1], out[2]]);
        assert_eq!(len as usize, out.len() - 3); // 长度字段自洽
        assert_eq!(out[3], 7); // type = SQL_COLLATION
        assert_eq!(out[4], 5); // new len
        assert_eq!(&out[5..10], &COLLATION);
        assert_eq!(out[10], 0); // old len = 0（关键）
        assert_eq!(out.len(), 11);
    }

    #[test]
    fn test_error_token() {
        let mut out = Vec::new();
        token_error(18456, "Login failed", &mut out);
        assert_eq!(out[0], TOKEN_ERROR);
        let len = u16::from_le_bytes([out[1], out[2]]) as usize;
        assert_eq!(len + 3, out.len());
        let number = u32::from_le_bytes([out[3], out[4], out[5], out[6]]);
        assert_eq!(number, 18456);
    }
}
