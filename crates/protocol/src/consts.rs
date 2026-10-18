//! MySQL 协议常量定义。
//!
//! 移植自 Mycat3 `common` 模块：
//! - `io.mycat.config.MySQLServerCapabilityFlags`
//! - `io.mycat.beans.mysql.MySQLServerStatusFlags`
//! - `io.mycat.beans.mysql.MySQLCommandType`
//! - `io.mycat.beans.mysql.MySQLErrorCode`
//! - `io.mycat.beans.mysql.MySQLFieldsType`

/// 客户端/服务端能力标志 (Capability Flags)。
pub mod capability {
    pub const CLIENT_LONG_PASSWORD: u32 = 0x0000_0001;
    pub const CLIENT_FOUND_ROWS: u32 = 0x0000_0002;
    pub const CLIENT_LONG_FLAG: u32 = 0x0000_0004;
    pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
    pub const CLIENT_NO_SCHEMA: u32 = 0x0000_0010;
    pub const CLIENT_COMPRESS: u32 = 0x0000_0020;
    pub const CLIENT_ODBC: u32 = 0x0000_0040;
    pub const CLIENT_LOCAL_FILES: u32 = 0x0000_0080;
    pub const CLIENT_IGNORE_SPACE: u32 = 0x0000_0100;
    pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
    pub const CLIENT_INTERACTIVE: u32 = 0x0000_0400;
    pub const CLIENT_SSL: u32 = 0x0000_0800;
    pub const CLIENT_IGNORE_SIGPIPE: u32 = 0x0000_1000;
    pub const CLIENT_TRANSACTIONS: u32 = 0x0000_2000;
    pub const CLIENT_RESERVED: u32 = 0x0000_4000;
    pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
    pub const CLIENT_MULTI_STATEMENTS: u32 = 0x0001_0000;
    pub const CLIENT_MULTI_RESULTS: u32 = 0x0002_0000;
    pub const CLIENT_PS_MULTI_RESULTS: u32 = 0x0004_0000;
    pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
    pub const CLIENT_CONNECT_ATTRS: u32 = 0x0010_0000;
    pub const CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA: u32 = 0x0020_0000;
    pub const CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS: u32 = 0x0040_0000;
    pub const CLIENT_SESSION_TRACK: u32 = 0x0080_0000;
    pub const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;

    /// 默认服务端能力，等价于 Mycat3 `MySQLServerCapabilityFlags.getDefaultServerCapabilities()`：
    /// LONG_PASSWORD | FOUND_ROWS | LONG_FLAG | CONNECT_WITH_DB | LOCAL_FILES | IGNORE_SPACE
    /// | PROTOCOL_41 | INTERACTIVE | IGNORE_SIGPIPE | TRANSACTIONS | SECURE_CONNECTION
    /// | MULTI_STATEMENTS | MULTI_RESULTS | PLUGIN_AUTH
    pub const DEFAULT_SERVER_CAPABILITIES: u32 = CLIENT_LONG_PASSWORD
        | CLIENT_FOUND_ROWS
        | CLIENT_LONG_FLAG
        | CLIENT_CONNECT_WITH_DB
        | CLIENT_LOCAL_FILES
        | CLIENT_IGNORE_SPACE
        | CLIENT_PROTOCOL_41
        | CLIENT_INTERACTIVE
        | CLIENT_IGNORE_SIGPIPE
        | CLIENT_TRANSACTIONS
        | CLIENT_SECURE_CONNECTION
        | CLIENT_MULTI_STATEMENTS
        | CLIENT_MULTI_RESULTS
        | CLIENT_PLUGIN_AUTH;

    /// 后端客户端连接使用的能力（附加 CONNECT_WITH_DB / PLUGIN_AUTH_LENENC / DEPRECATE_EOF 可选）。
    pub const DEFAULT_CLIENT_CAPABILITIES: u32 = DEFAULT_SERVER_CAPABILITIES
        | CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
        | CLIENT_PS_MULTI_RESULTS;
}

/// 服务端状态标志 (Status Flags)。移植自 `MySQLServerStatusFlags`。
pub mod status {
    pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
    pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
    pub const SERVER_STATUS_NO_GOOD_INDEX_USED: u16 = 0x0010;
    pub const SERVER_STATUS_NO_INDEX_USED: u16 = 0x0020;
    pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
    pub const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;
    pub const SERVER_STATUS_DB_DROPPED: u16 = 0x0100;
    pub const SERVER_STATUS_NO_BACKSLASH_ESCAPES: u16 = 0x0200;
    pub const SERVER_STATUS_METADATA_CHANGED: u16 = 0x0400;
    pub const SERVER_QUERY_WAS_SLOW: u16 = 0x0800;
    pub const SERVER_PS_OUT_PARAMS: u16 = 0x1000;
}

/// 命令类型。移植自 `MySQLCommandType`。
pub mod command {
    pub const COM_SLEEP: u8 = 0x00;
    pub const COM_QUIT: u8 = 0x01;
    pub const COM_INIT_DB: u8 = 0x02;
    pub const COM_QUERY: u8 = 0x03;
    pub const COM_FIELD_LIST: u8 = 0x04;
    pub const COM_CREATE_DB: u8 = 0x05;
    pub const COM_DROP_DB: u8 = 0x06;
    pub const COM_REFRESH: u8 = 0x07;
    pub const COM_SHUTDOWN: u8 = 0x08;
    pub const COM_STATISTICS: u8 = 0x09;
    pub const COM_PROCESS_INFO: u8 = 0x0a;
    pub const COM_CONNECT: u8 = 0x0b;
    pub const COM_PROCESS_KILL: u8 = 0x0c;
    pub const COM_DEBUG: u8 = 0x0d;
    pub const COM_PING: u8 = 0x0e;
    pub const COM_TIME: u8 = 0x0f;
    pub const COM_DELAYED_INSERT: u8 = 0x10;
    pub const COM_CHANGE_USER: u8 = 0x11;
    pub const COM_BINLOG_DUMP: u8 = 0x12;
    pub const COM_TABLE_DUMP: u8 = 0x13;
    pub const COM_CONNECT_OUT: u8 = 0x14;
    pub const COM_REGISTER_SLAVE: u8 = 0x15;
    pub const COM_STMT_PREPARE: u8 = 0x16;
    pub const COM_STMT_EXECUTE: u8 = 0x17;
    pub const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
    pub const COM_STMT_CLOSE: u8 = 0x19;
    pub const COM_STMT_RESET: u8 = 0x1a;
    pub const COM_SET_OPTION: u8 = 0x1b;
    pub const COM_STMT_FETCH: u8 = 0x1c;
    pub const COM_DAEMON: u8 = 0x1d;
    pub const COM_BINLOG_DUMP_GTID: u8 = 0x1e;
    pub const COM_RESET_CONNECTION: u8 = 0x1f;
}

/// 列（字段）类型。移植自 `MySQLFieldsType`。
pub mod field_type {
    pub const DECIMAL: u8 = 0x00;
    pub const TINY: u8 = 0x01;
    pub const SHORT: u8 = 0x02;
    pub const LONG: u8 = 0x03;
    pub const FLOAT: u8 = 0x04;
    pub const DOUBLE: u8 = 0x05;
    pub const NULL: u8 = 0x06;
    pub const TIMESTAMP: u8 = 0x07;
    pub const LONGLONG: u8 = 0x08;
    pub const INT24: u8 = 0x09;
    pub const DATE: u8 = 0x0a;
    pub const TIME: u8 = 0x0b;
    pub const DATETIME: u8 = 0x0c;
    pub const YEAR: u8 = 0x0d;
    pub const NEWDATE: u8 = 0x0e;
    pub const VARCHAR: u8 = 0x0f;
    pub const BIT: u8 = 0x10;
    pub const JSON: u8 = 0xf5;
    pub const NEWDECIMAL: u8 = 0xf6;
    pub const ENUM: u8 = 0xf7;
    pub const SET: u8 = 0xf8;
    pub const TINY_BLOB: u8 = 0xf9;
    pub const MEDIUM_BLOB: u8 = 0xfa;
    pub const LONG_BLOB: u8 = 0xfb;
    pub const BLOB: u8 = 0xfc;
    pub const VAR_STRING: u8 = 0xfd;
    pub const STRING: u8 = 0xfe;
    pub const GEOMETRY: u8 = 0xff;
}

/// 列标志 (Column Flags)。
pub mod column_flag {
    pub const NOT_NULL: u16 = 0x0001;
    pub const PRI_KEY: u16 = 0x0002;
    pub const UNIQUE_KEY: u16 = 0x0004;
    pub const MULTIPLE_KEY: u16 = 0x0008;
    pub const BLOB: u16 = 0x0010;
    pub const UNSIGNED: u16 = 0x0020;
    pub const ZEROFILL: u16 = 0x0040;
    pub const BINARY: u16 = 0x0080;
    pub const ENUM: u16 = 0x0100;
    pub const AUTO_INCREMENT: u16 = 0x0200;
    pub const TIMESTAMP: u16 = 0x0400;
    pub const SET: u16 = 0x0800;
    pub const NO_DEFAULT_VALUE: u16 = 0x1000;
}

/// 常用错误码。移植自 `MySQLErrorCode`（节选）。
pub mod error_code {
    pub const ER_HANDSHAKE_ERROR: u16 = 1043;
    /// 行值与表列数不匹配（1136，MySQL ER_WRONG_VALUE_COUNT_ON_ROW）
    pub const ER_WRONG_VALUE_COUNT_ON_ROW: u16 = 1136;
    pub const ER_ACCESS_DENIED_ERROR: u16 = 1045;
    pub const ER_NO_DB_ERROR: u16 = 1046;
    pub const ER_UNKNOWN_COM_ERROR: u16 = 1047;
    pub const ER_BAD_DB_ERROR: u16 = 1049;
    pub const ER_TABLE_EXISTS_ERROR: u16 = 1050;
    pub const ER_BAD_TABLE_ERROR: u16 = 1051;
    pub const ER_SERVER_SHUTDOWN: u16 = 1053;
    pub const ER_DUP_ENTRY: u16 = 1062;
    pub const ER_PARSE_ERROR: u16 = 1064;
    pub const ER_EMPTY_QUERY: u16 = 1065;
    pub const ER_NO_SUCH_TABLE: u16 = 1146;
    pub const ER_UNKNOWN_ERROR: u16 = 1105;
    pub const ER_NOT_SUPPORTED_YET: u16 = 1235;
    pub const ER_UNKNOWN_SYSTEM_VARIABLE: u16 = 1193;
    pub const ER_QUERY_TIMEOUT: u16 = 3024;
}

/// 字符集/排序规则索引（节选）。
pub mod charset {
    pub const UTF8_GENERAL_CI: u8 = 33;
    pub const UTF8MB4_GENERAL_CI: u8 = 45;
    pub const UTF8MB4_0900_AI_CI: u8 = 255;
    pub const BINARY: u8 = 63;
    pub const LATIN1_SWEDISH_CI: u8 = 8;
}

/// 协议版本。
pub const PROTOCOL_VERSION: u8 = 10;
/// 默认服务端版本串，等价 Mycat3 `ServerConfig.serverVersion` 默认值。
pub const DEFAULT_SERVER_VERSION: &str = "5.7.33-mycat-3.0-pro";
/// 单个 MySQL 包最大载荷（超过则分包）。
pub const MAX_PACKET_SIZE: usize = 0x00FF_FFFF;
/// 文本协议 NULL 标记。
pub const NULL_MARK: u8 = 0xfb;
/// mysql_native_password 认证插件名。
pub const AUTH_PLUGIN_NATIVE: &str = "mysql_native_password";
/// caching_sha2_password 认证插件名。
pub const AUTH_PLUGIN_CACHING_SHA2: &str = "caching_sha2_password";
