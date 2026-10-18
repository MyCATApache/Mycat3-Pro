//! information_schema 本地视图、系统变量表与 SHOW 扩展。
//!
//! 对应 Java 版 `io.mycat.prototypeserver.mysql.InformationSchema`（MySQL8 视图
//! 定义 + VisualTableHandler 内存表）与 `MysqlVariableServiceImpl`（硬编码变量
//! fallback 表）的常用子集：
//! - `information_schema.SCHEMATA / TABLES / COLUMNS`：由逻辑元数据 +
//!   createTableSQL 列解析生成（JDBC/BI 工具元数据查询兼容）
//! - `select @@var` 多变量/带 session|global 前缀的完整 fallback
//! - `SHOW CREATE TABLE / SHOW FULL TABLES / SHOW COLUMNS / DESC / SHOW VARIABLES`
//!
//! Java 版 information_schema 走 Calcite 引擎全量生成后谓词下推；Pro 版
//! 生成全量行后做等值/LIKE 过滤（覆盖驱动元数据查询的常见谓词）。

use std::sync::Arc;

use mycat3_protocol::consts::field_type;
use mycat3_sql::TableMeta;

use crate::engine::{EngineColumn, EngineError, EngineOutcome};
use crate::session::SessionContext;
use crate::state::ServerState;

fn col(name: &str) -> EngineColumn {
    EngineColumn {
        name: name.to_string(),
        mysql_type: field_type::VAR_STRING,
        charset: 33,
    }
}

fn col_i64(name: &str) -> EngineColumn {
    EngineColumn {
        name: name.to_string(),
        mysql_type: field_type::LONGLONG,
        charset: 63,
    }
}

// ---------------------------------------------------------------------------
// information_schema 视图生成
// ---------------------------------------------------------------------------

/// 若 SQL 是 information_schema 单表查询则本地应答（含 WHERE 等值/LIKE 过滤）。
pub fn try_information_schema(
    state: &Arc<ServerState>,
    sess: &SessionContext,
    sql: &str,
) -> Option<Result<EngineOutcome, EngineError>> {
    let lower = sql.to_lowercase();
    if !lower.contains("information_schema.") {
        return None;
    }
    // 识别目标表
    let table = ["schemata", "tables", "columns"]
        .into_iter()
        .find(|t| lower.contains(&format!("information_schema.{}", t)))?;
    let (columns, rows) = match table {
        "schemata" => gen_schemata(state),
        "tables" => gen_tables(state),
        "columns" => gen_columns(state),
        _ => return None,
    };
    // WHERE 过滤（等值 + LIKE，AND 连接）
    let filters = parse_where_filters(sql, sess);
    let rows = apply_filters(&columns, rows, &filters);
    // 投影裁剪：SELECT 列表中的纯列引用按序输出（* → 全列）
    let projection = parse_projection(sql);
    let (columns, rows) = match projection {
        Some(names) if !names.is_empty() => project_columns(columns, rows, &names),
        _ => (columns, rows),
    };
    Some(Ok(EngineOutcome::Rows { columns, rows }))
}

/// 解析 SELECT 投影列表；含表达式/函数等非纯列引用时返回 None（回退全列）。
fn parse_projection(sql: &str) -> Option<Vec<String>> {
    let stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql).ok()?;
    let sqlparser::ast::Statement::Query(q) = stmts.first()? else {
        return None;
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        return None;
    };
    let mut names = Vec::new();
    for item in &sel.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Identifier(i)) => {
                names.push(i.value.clone());
            }
            sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::CompoundIdentifier(
                parts,
            )) => {
                names.push(parts.last()?.value.clone());
            }
            sqlparser::ast::SelectItem::ExprWithAlias { alias, .. } => {
                names.push(alias.value.clone())
            }
            sqlparser::ast::SelectItem::Wildcard(_) => return None, // 全列
            _ => return None,                                       // 表达式 → 全列回退
        }
    }
    Some(names)
}

/// 按投影列名（大小写不敏感）重排/裁剪列与行；未知列名以 NULL 填充（宽容）。
fn project_columns(
    columns: Vec<EngineColumn>,
    rows: Vec<Row>,
    names: &[String],
) -> (Vec<EngineColumn>, Vec<Row>) {
    let idx: Vec<Option<usize>> = names
        .iter()
        .map(|n| columns.iter().position(|c| c.name.eq_ignore_ascii_case(n)))
        .collect();
    let new_cols: Vec<EngineColumn> = idx
        .iter()
        .zip(names)
        .map(|(i, n)| match i {
            Some(k) => columns[*k].clone(),
            None => col(n),
        })
        .collect();
    let new_rows: Vec<Row> = rows
        .into_iter()
        .map(|row| {
            idx.iter()
                .map(|i| i.and_then(|k| row.get(k).cloned()).flatten())
                .collect()
        })
        .collect();
    (new_cols, new_rows)
}

type Row = Vec<Option<String>>;

fn gen_schemata(state: &Arc<ServerState>) -> (Vec<EngineColumn>, Vec<Row>) {
    let meta = state.metadata();
    let columns = vec![
        col("CATALOG_NAME"),
        col("SCHEMA_NAME"),
        col("DEFAULT_CHARACTER_SET_NAME"),
        col("DEFAULT_COLLATION_NAME"),
        col("SQL_PATH"),
    ];
    let mut rows = Vec::new();
    for name in meta.schema_names() {
        rows.push(vec![
            Some("def".into()),
            Some(name.to_string()),
            Some("utf8mb4".into()),
            Some("utf8mb4_general_ci".into()),
            None,
        ]);
    }
    (columns, rows)
}

fn gen_tables(state: &Arc<ServerState>) -> (Vec<EngineColumn>, Vec<Row>) {
    let meta = state.metadata();
    let columns = vec![
        col("TABLE_CATALOG"),
        col("TABLE_SCHEMA"),
        col("TABLE_NAME"),
        col("TABLE_TYPE"),
        col("ENGINE"),
        col_i64("VERSION"),
        col("ROW_FORMAT"),
        col_i64("TABLE_ROWS"),
        col("TABLE_COLLATION"),
        col_i64("CREATE_TIME"),
    ];
    let mut rows = Vec::new();
    for schema in meta.schemas.values() {
        for (tname, tmeta) in &schema.tables {
            let ttype = match tmeta {
                TableMeta::View { .. } => "VIEW",
                _ => "BASE TABLE",
            };
            rows.push(vec![
                Some("def".into()),
                Some(schema.name.clone()),
                Some(tname.clone()),
                Some(ttype.into()),
                Some("InnoDB".into()),
                Some("10".into()),
                Some("Compact".into()),
                Some("0".into()),
                Some("utf8mb4_general_ci".into()),
                None,
            ]);
        }
    }
    (columns, rows)
}

fn gen_columns(state: &Arc<ServerState>) -> (Vec<EngineColumn>, Vec<Row>) {
    let meta = state.metadata();
    let columns = vec![
        col("TABLE_CATALOG"),
        col("TABLE_SCHEMA"),
        col("TABLE_NAME"),
        col("COLUMN_NAME"),
        col_i64("ORDINAL_POSITION"),
        col("COLUMN_DEFAULT"),
        col("IS_NULLABLE"),
        col("DATA_TYPE"),
        col_i64("CHARACTER_MAXIMUM_LENGTH"),
        col("COLUMN_TYPE"),
        col("COLUMN_KEY"),
        col("EXTRA"),
        col("PRIVILEGES"),
    ];
    let mut rows = Vec::new();
    for schema in meta.schemas.values() {
        for (tname, tmeta) in &schema.tables {
            let ddl = match tmeta {
                TableMeta::Normal { create_sql, .. }
                | TableMeta::Global { create_sql, .. }
                | TableMeta::Sharding { create_sql, .. } => create_sql.as_deref(),
                TableMeta::View { .. } => None,
            };
            let Some(ddl) = ddl else { continue };
            for (i, c) in parse_create_table_columns(ddl).into_iter().enumerate() {
                rows.push(vec![
                    Some("def".into()),
                    Some(schema.name.clone()),
                    Some(tname.clone()),
                    Some(c.name),
                    Some((i + 1).to_string()), // ORDINAL_POSITION 1 基（修复 Java 0 基 quirk）
                    c.default,
                    Some(if c.nullable { "YES" } else { "NO" }.into()),
                    Some(base_type(&c.column_type)),
                    char_max_len(&c.column_type).map(|n| n.to_string()),
                    Some(c.column_type),
                    Some(
                        if c.primary {
                            "PRI"
                        } else if c.unique {
                            "UNI"
                        } else {
                            ""
                        }
                        .to_string(),
                    ),
                    Some(
                        if c.auto_increment {
                            "auto_increment"
                        } else {
                            ""
                        }
                        .to_string(),
                    ),
                    Some("select,insert,update,references".into()),
                ]);
            }
        }
    }
    (columns, rows)
}

/// 解析出的列定义。对应 Java `SimpleColumnInfo` 子集。
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedColumn {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub default: Option<String>,
    pub auto_increment: bool,
    pub primary: bool,
    pub unique: bool,
}

/// 从 CREATE TABLE DDL 解析列定义（顶层逗号切分，跳过约束行）。
/// 对应 `PrototypeService.getColumnInfo`（Java 用 druid；Pro 用轻量解析）。
pub fn parse_create_table_columns(ddl: &str) -> Vec<ParsedColumn> {
    let Some(open) = ddl.find('(') else {
        return Vec::new();
    };
    // 找匹配的右括号
    let bytes = ddl.as_bytes();
    let mut depth = 0i32;
    let mut close = None;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(i);
                    break;
                }
            }
            b'\'' | b'"' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() && bytes[i] != q {
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    let Some(close) = close else {
        return Vec::new();
    };
    let body = &ddl[open + 1..close];
    let mut cols = Vec::new();
    let mut pk_cols: Vec<String> = Vec::new();
    for item in split_top_commas(body) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let upper = item.to_uppercase();
        if upper.starts_with("PRIMARY KEY") {
            // 表级主键
            pk_cols.extend(extract_paren_names(item));
            continue;
        }
        if upper.starts_with("KEY ")
            || upper.starts_with("INDEX ")
            || upper.starts_with("UNIQUE KEY")
            || upper.starts_with("UNIQUE INDEX")
            || upper.starts_with("CONSTRAINT")
            || upper.starts_with("FOREIGN KEY")
            || upper.starts_with("FULLTEXT")
            || upper.starts_with("CHECK")
        {
            continue;
        }
        if let Some(c) = parse_column_item(item) {
            cols.push(c);
        }
    }
    for c in cols.iter_mut() {
        if pk_cols.iter().any(|p| p.eq_ignore_ascii_case(&c.name)) {
            c.primary = true;
            c.nullable = false;
        }
    }
    cols
}

fn extract_paren_names(s: &str) -> Vec<String> {
    let Some(o) = s.find('(') else { return vec![] };
    let Some(c) = s.rfind(')') else { return vec![] };
    s[o + 1..c]
        .split(',')
        .map(|x| {
            x.trim()
                .trim_matches('`')
                .split('(')
                .next()
                .unwrap_or("")
                .trim()
                .to_string()
        })
        .filter(|x| !x.is_empty())
        .collect()
}

fn split_top_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut in_quote: Option<u8> = None;
    for b in s.bytes() {
        match in_quote {
            Some(q) => {
                cur.push(b as char);
                if b == q {
                    in_quote = None;
                }
                continue;
            }
            None => match b {
                b'\'' | b'"' | b'`' => {
                    in_quote = Some(b);
                    cur.push(b as char);
                }
                b'(' => {
                    depth += 1;
                    cur.push(b as char);
                }
                b')' => {
                    depth -= 1;
                    cur.push(b as char);
                }
                b',' if depth == 0 => {
                    out.push(std::mem::take(&mut cur));
                }
                _ => cur.push(b as char),
            },
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

fn parse_column_item(item: &str) -> Option<ParsedColumn> {
    let mut it = item.trim();
    // 列名（可能带反引号）
    let name = if it.starts_with('`') {
        let end = it[1..].find('`')? + 1;
        let n = it[1..end].to_string();
        it = it[end + 1..].trim();
        n
    } else {
        let end = it.find(|c: char| c.is_whitespace() || c == '(')?;
        let n = it[..end].to_string();
        it = it[end..].trim();
        n
    };
    if name.is_empty() {
        return None;
    }
    // 类型：读到下一个顶层空白（含括号内容，如 decimal(10,2) / varchar(64)）
    let (column_type, rest) = split_type(it);
    let upper = rest.to_uppercase();
    let nullable = !upper.contains("NOT NULL");
    let auto_increment = upper.contains("AUTO_INCREMENT");
    let primary = upper.contains("PRIMARY KEY");
    let unique = upper.contains("UNIQUE");
    // DEFAULT 值
    let default = extract_default(rest);
    Some(ParsedColumn {
        name,
        column_type: column_type.to_lowercase(),
        nullable: nullable && !primary,
        default,
        auto_increment,
        primary,
        unique,
    })
}

fn split_type(s: &str) -> (&str, &str) {
    let bytes = s.as_bytes();
    let mut i = 0;
    // 类型名（字母/数字/下划线）
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    // 可能的 (n) / (n,m)
    if i < bytes.len() && bytes[i] == b'(' {
        let mut depth = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        i += 1;
                        break;
                    }
                }
                _ => {}
            }
            i += 1;
        }
    }
    // unsigned / zerofill 后缀
    loop {
        let rest = &s[i..];
        let trimmed = rest.trim_start();
        let consumed = rest.len() - trimmed.len();
        let lower = trimmed.to_lowercase();
        if lower.starts_with("unsigned") {
            i += consumed + "unsigned".len();
        } else if lower.starts_with("zerofill") {
            i += consumed + "zerofill".len();
        } else {
            break;
        }
    }
    (&s[..i], s[i..].trim())
}

fn extract_default(rest: &str) -> Option<String> {
    let lower = rest.to_lowercase();
    let pos = lower.find("default")?;
    let after = rest[pos + "default".len()..].trim();
    if after.is_empty() {
        return None;
    }
    if after.starts_with('\'') {
        let end = after[1..]
            .find('\'')
            .map(|i| i + 1)
            .unwrap_or(after.len() - 1);
        Some(after[..=end].to_string())
    } else {
        let tok = after
            .split(|c: char| c.is_whitespace() || c == ',')
            .next()
            .unwrap_or("");
        if tok.eq_ignore_ascii_case("NULL") {
            None
        } else {
            Some(tok.to_string())
        }
    }
}

fn base_type(t: &str) -> String {
    t.split('(').next().unwrap_or(t).trim().to_string()
}

fn char_max_len(t: &str) -> Option<u32> {
    let base = base_type(t);
    if !matches!(
        base.as_str(),
        "varchar"
            | "char"
            | "varbinary"
            | "binary"
            | "text"
            | "tinytext"
            | "mediumtext"
            | "longtext"
    ) {
        return None;
    }
    let o = t.find('(')?;
    let c = t.find(')')?;
    t[o + 1..c].parse().ok()
}

// ---------------------------------------------------------------------------
// WHERE 过滤（等值 + LIKE）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Filter {
    Eq(String, String),   // 列名(大写), 值
    Like(String, String), // 列名(大写), 模式
    NotEq(String, String),
}

/// 用 sqlparser 提取 WHERE 中列 = '值' / LIKE '模式' / 列 = DATABASE() 条件。
fn parse_where_filters(sql: &str, sess: &SessionContext) -> Vec<Filter> {
    let mut out = Vec::new();
    let Ok(stmts) = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql)
    else {
        return out;
    };
    let Some(sqlparser::ast::Statement::Query(q)) = stmts.first() else {
        return out;
    };
    let sqlparser::ast::SetExpr::Select(sel) = q.body.as_ref() else {
        return out;
    };
    let Some(expr) = &sel.selection else {
        return out;
    };
    collect_filters(expr, sess, &mut out);
    out
}

fn collect_filters(expr: &sqlparser::ast::Expr, sess: &SessionContext, out: &mut Vec<Filter>) {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            collect_filters(left, sess, out);
            collect_filters(right, sess, out);
        }
        Expr::Nested(inner) => collect_filters(inner, sess, out),
        Expr::BinaryOp {
            op: BinaryOperator::Eq,
            left,
            right,
        } => {
            if let Some(f) = eq_filter(left, right, sess).or_else(|| eq_filter(right, left, sess)) {
                out.push(f);
            }
        }
        Expr::BinaryOp {
            op: BinaryOperator::NotEq,
            left,
            right,
        } => {
            if let Some(Filter::Eq(c, v)) =
                eq_filter(left, right, sess).or_else(|| eq_filter(right, left, sess))
            {
                out.push(Filter::NotEq(c, v));
            }
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            if !negated {
                if let (Some(c), Some(v)) = (column_ident(expr), literal_str(pattern)) {
                    out.push(Filter::Like(c.to_uppercase(), v));
                }
            }
        }
        _ => {}
    }
}

fn eq_filter(
    col_side: &sqlparser::ast::Expr,
    val_side: &sqlparser::ast::Expr,
    sess: &SessionContext,
) -> Option<Filter> {
    let c = column_ident(col_side)?;
    // 值：字面量 或 DATABASE()/SCHEMA() 函数
    let v = literal_str(val_side).or_else(|| {
        if let sqlparser::ast::Expr::Function(f) = val_side {
            let name = f.name.to_string().to_lowercase();
            if name == "database" || name == "schema" {
                return sess.schema.clone();
            }
        }
        None
    })?;
    Some(Filter::Eq(c.to_uppercase(), v))
}

fn column_ident(e: &sqlparser::ast::Expr) -> Option<&str> {
    match e {
        sqlparser::ast::Expr::Identifier(i) => Some(&i.value),
        sqlparser::ast::Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.as_str()),
        _ => None,
    }
}

fn literal_str(e: &sqlparser::ast::Expr) -> Option<String> {
    match e {
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) => {
            Some(s.clone())
        }
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::DoubleQuotedString(s)) => {
            Some(s.clone())
        }
        sqlparser::ast::Expr::Value(sqlparser::ast::Value::Number(n, _)) => Some(n.to_string()),
        _ => None,
    }
}

fn apply_filters(columns: &[EngineColumn], rows: Vec<Row>, filters: &[Filter]) -> Vec<Row> {
    if filters.is_empty() {
        return rows;
    }
    rows.into_iter()
        .filter(|row| {
            filters.iter().all(|f| {
                let cname = match f {
                    Filter::Eq(c, _) => c,
                    Filter::NotEq(c, _) => c,
                    Filter::Like(c, _) => c,
                };
                let Some(idx) = columns
                    .iter()
                    .position(|c| c.name.eq_ignore_ascii_case(cname))
                else {
                    return true; // 未知列不参与过滤
                };
                let actual = row.get(idx).and_then(|v| v.as_deref()).unwrap_or("");
                match f {
                    Filter::Eq(_, expect) => actual.eq_ignore_ascii_case(expect),
                    Filter::NotEq(_, expect) => !actual.eq_ignore_ascii_case(expect),
                    Filter::Like(_, pattern) => sql_like(actual, pattern),
                }
            })
        })
        .collect()
}

/// SQL LIKE（% 与 _ 通配，大小写不敏感）。
pub fn sql_like(value: &str, pattern: &str) -> bool {
    let v: Vec<char> = value.to_lowercase().chars().collect();
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    like_match(&v, &p)
}

fn like_match(v: &[char], p: &[char]) -> bool {
    // 经典双指针回溯
    let (mut vi, mut pi) = (0usize, 0usize);
    let (mut star_p, mut star_v) = (usize::MAX, 0usize);
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '%') {
            star_p = pi;
            star_v = vi;
            pi += 1;
        } else if pi < p.len() && (p[pi] == '_' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if star_p != usize::MAX {
            pi = star_p + 1;
            star_v += 1;
            vi = star_v;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

// ---------------------------------------------------------------------------
// 系统变量表（对应 MysqlVariableServiceImpl 硬编码 fallback）
// ---------------------------------------------------------------------------

/// 查询变量值：会话动态值优先，其余用内置表。
pub fn lookup_variable(
    state: &Arc<ServerState>,
    sess: &SessionContext,
    raw_name: &str,
) -> Option<String> {
    // 去掉 @@ / session. / global. / local. 前缀
    let mut name = raw_name.trim_start_matches('@').to_lowercase();
    for p in ["session.", "global.", "local.", "@@"] {
        if let Some(rest) = name.strip_prefix(p) {
            name = rest.to_string();
            break;
        }
    }
    // 会话动态值
    let dynamic = match name.as_str() {
        "autocommit" => Some(if sess.autocommit { "1" } else { "0" }),
        "transaction_isolation" | "tx_isolation" => Some(match sess.isolation {
            0 => "READ-UNCOMMITTED",
            1 => "READ-COMMITTED",
            2 => "SERIALIZABLE",
            _ => "REPEATABLE-READ",
        }),
        "transaction_read_only" | "tx_read_only" => Some("0"),
        "character_set_client" | "character_set_connection" | "character_set_results" => {
            Some(charset_name(sess.charset))
        }
        _ => None,
    };
    if let Some(v) = dynamic {
        return Some(v.to_string());
    }
    let cfg = state.config();
    let builtins = builtin_variables(&cfg.server.server.server_version);
    builtins.get(name.as_str()).cloned()
}

fn charset_name(c: u8) -> &'static str {
    match c {
        33 => "utf8",
        45 | 46 | 47 => "utf8mb4",
        8 => "latin1",
        28 => "gbk",
        63 => "binary",
        _ => "utf8mb4",
    }
}

/// 内置变量表（对应 Java hardcoded fallback；节选驱动/BI 工具最常探测的变量）。
pub fn builtin_variables(server_version: &str) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    let mut put = |k: &str, v: &str| {
        m.insert(k.to_string(), v.to_string());
    };
    put("version", server_version);
    put("version_comment", "Mycat3 Pro (Rust)");
    put("version_compile_machine", std::env::consts::ARCH);
    put("version_compile_os", std::env::consts::OS);
    put("license", "GPL-3.0");
    put("system_time_zone", "UTC");
    put("time_zone", "SYSTEM");
    put("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION");
    put("lower_case_table_names", "1");
    put("max_allowed_packet", "16777216");
    put("net_buffer_length", "8192");
    put("net_write_timeout", "60");
    put("wait_timeout", "172800");
    put("interactive_timeout", "172800");
    put("character_set_client", "utf8mb4");
    put("character_set_connection", "utf8mb4");
    put("character_set_results", "utf8mb4");
    put("character_set_server", "utf8mb4");
    put("character_set_database", "utf8mb4");
    put("collation_server", "utf8mb4_general_ci");
    put("collation_connection", "utf8mb4_general_ci");
    put("init_connect", "");
    put("query_cache_size", "0");
    put("query_cache_type", "OFF");
    put("have_query_cache", "NO");
    put("performance_schema", "0");
    put("protocol_version", "10");
    put("sql_select_limit", "18446744073709551615");
    put("max_execution_time", "0");
    put("innodb_lock_wait_timeout", "50");
    put("foreign_key_checks", "1");
    put("auto_increment_offset", "1");
    put("auto_increment_increment", "1");
    put("tx_isolation", "REPEATABLE-READ");
    put("transaction_isolation", "REPEATABLE-READ");
    put("autocommit", "1");
    put("storage_engine", "InnoDB");
    put("default_storage_engine", "InnoDB");
    put("transaction_read_only", "0");
    put("tx_read_only", "0");
    put("event_scheduler", "OFF");
    put("profiling", "0");
    put("have_ssl", "NO");
    put("have_openssl", "NO");
    put("ssl", "");
    put("hostname", "mycat3-pro");
    put("pid_file", "");
    put("report_port", "0");
    put("big_tables", "0");
    put("old_alter_table", "0");
    put("automatic_sp_privileges", "1");
    put("back_log", "80");
    m
}

/// `SHOW VARIABLES [LIKE 'pattern']` 本地应答。
pub fn show_variables(state: &Arc<ServerState>, sess: &SessionContext, sql: &str) -> EngineOutcome {
    let mut vars = builtin_variables(&state.config().server.server.server_version);
    // 会话动态覆盖
    for name in vars.keys().cloned().collect::<Vec<_>>() {
        if let Some(v) = lookup_variable(state, sess, &name) {
            vars.insert(name, v);
        }
    }
    let pattern = extract_like_pattern(sql);
    let mut names: Vec<&String> = vars.keys().collect();
    names.sort();
    let rows: Vec<Row> = names
        .into_iter()
        .filter(|n| pattern.as_ref().map(|p| sql_like(n, p)).unwrap_or(true))
        .map(|n| {
            vec![
                Some(n.clone()),
                Some(vars.get(n.as_str()).cloned().unwrap_or_default()),
            ]
        })
        .collect();
    EngineOutcome::Rows {
        columns: vec![col("Variable_name"), col("Value")],
        rows,
    }
}

fn extract_like_pattern(sql: &str) -> Option<String> {
    let lower = sql.to_lowercase();
    let pos = lower.find(" like ")?;
    let rest = sql[pos + 6..].trim().trim_end_matches(';').trim();
    let rest = rest.trim_matches('\'').trim_matches('"');
    if rest.is_empty() {
        None
    } else {
        Some(rest.to_string())
    }
}

/// SHOW [FULL] PROCESSLIST 本地应答（连接注册表生成）。
/// 对应 Java ShowProcessListSQLHandler；修复其 Time 列毫秒 quirk（Pro 用秒，MySQL 标准）。
pub fn try_processlist(
    state: &Arc<ServerState>,
    sess: &SessionContext,
    sql: &str,
) -> Option<EngineOutcome> {
    let norm = sql.trim().trim_end_matches(';').to_lowercase();
    if !(norm.starts_with("show processlist") || norm.starts_with("show full processlist")) {
        return None;
    }
    let full = norm.contains("full");
    let mut rows: Vec<Row> = Vec::new();
    for p in state.processes.list() {
        let user = p.user.read().clone();
        if !full && !user.is_empty() && user != sess.user {
            continue; // 非 FULL 只看自己的连接（Java 同语义）
        }
        rows.push(vec![
            Some(p.id.to_string()),
            Some(if user.is_empty() {
                "unauthenticated".into()
            } else {
                user
            }),
            Some(p.host.clone()),
            p.db.read().clone(),
            Some(p.command.read().clone()),
            Some(p.elapsed_secs().to_string()),
            Some("".into()),
            p.sql.read().clone(),
        ]);
        if !full && rows.len() >= 100 {
            break;
        }
    }
    let columns = vec![
        col_i64("Id"),
        col("User"),
        col("Host"),
        col("db"),
        col("Command"),
        col_i64("Time"),
        col("State"),
        col("Info"),
    ];
    Some(EngineOutcome::Rows { columns, rows })
}

/// SHOW CREATE TABLE / SHOW FULL TABLES / SHOW COLUMNS / DESC 本地应答。
/// 返回 None 表示应透传后端。
pub fn try_show_local(
    state: &Arc<ServerState>,
    sess: &SessionContext,
    sql: &str,
) -> Option<EngineOutcome> {
    let norm = sql.trim().trim_end_matches(';').to_lowercase();
    // ---- 本地 mock 组（对应 Java Show*SQLHandler：不依赖 prototype 后端）----
    if let Some(o) = show_local_mock(&norm) {
        return Some(o);
    }
    let meta = state.metadata();
    // 支持 db.table 限定（show create table db2.orders 无需先 USE）
    let qualified = extract_qualified_schema(sql);
    let schema: &str = match &qualified {
        Some((s, _)) => s,
        None => sess.schema.as_deref()?,
    };
    let schema_meta = meta.get_schema(schema)?;

    if norm.starts_with("show create table") || norm.starts_with("show create view") {
        let tname = extract_table_arg(sql, &["show create table", "show create view"])?;
        let tmeta = schema_meta.get_table(&tname)?;
        let ddl = match tmeta {
            TableMeta::Normal { create_sql, .. }
            | TableMeta::Global { create_sql, .. }
            | TableMeta::Sharding { create_sql, .. } => create_sql.clone(),
            TableMeta::View { create_sql } => create_sql.clone(),
        };
        return Some(EngineOutcome::Rows {
            columns: vec![col("Table"), col("Create Table")],
            rows: vec![vec![Some(tname), ddl]],
        });
    }
    if norm.starts_with("show full tables") || norm.starts_with("show tables") {
        let full = norm.starts_with("show full tables");
        let like = extract_like_pattern(sql);
        let mut rows: Vec<Row> = schema_meta
            .tables
            .iter()
            .filter(|(n, _)| like.as_ref().map(|p| sql_like(n, p)).unwrap_or(true))
            .map(|(n, t)| {
                if full {
                    let ttype = match t {
                        TableMeta::View { .. } => "VIEW",
                        _ => "BASE TABLE",
                    };
                    vec![Some(n.clone()), Some(ttype.to_string())]
                } else {
                    vec![Some(n.clone())]
                }
            })
            .collect();
        rows.sort();
        let mut columns = vec![col(&format!("Tables_in_{}", schema))];
        if full {
            columns.push(col("Table_type"));
        }
        return Some(EngineOutcome::Rows { columns, rows });
    }
    if norm.starts_with("show columns")
        || norm.starts_with("show full columns")
        || norm.starts_with("desc ")
        || norm.starts_with("describe ")
    {
        let tname = if norm.starts_with("desc") {
            sql.trim()
                .trim_end_matches(';')
                .split_whitespace()
                .nth(1)?
                .trim_matches('`')
                .to_string()
        } else {
            extract_table_arg(sql, &["show full columns from", "show columns from"])?
        };
        let tmeta = schema_meta.get_table(&tname)?;
        let ddl = match tmeta {
            TableMeta::Normal { create_sql, .. }
            | TableMeta::Global { create_sql, .. }
            | TableMeta::Sharding { create_sql, .. } => create_sql.as_deref(),
            TableMeta::View { .. } => None,
        }?;
        let rows: Vec<Row> = parse_create_table_columns(ddl)
            .into_iter()
            .map(|c| {
                vec![
                    Some(c.name),
                    Some(c.column_type),
                    Some(if c.nullable { "YES" } else { "NO" }.into()),
                    Some(
                        if c.primary {
                            "PRI"
                        } else if c.unique {
                            "UNI"
                        } else {
                            ""
                        }
                        .to_string(),
                    ),
                    c.default,
                    Some(
                        if c.auto_increment {
                            "auto_increment"
                        } else {
                            ""
                        }
                        .to_string(),
                    ),
                ]
            })
            .collect();
        return Some(EngineOutcome::Rows {
            columns: vec![
                col("Field"),
                col("Type"),
                col("Null"),
                col("Key"),
                col("Default"),
                col("Extra"),
            ],
            rows,
        });
    }
    None
}

/// 提取 `show ... from db.t` / `show create table db.t` 中的 schema 限定。
fn extract_qualified_schema(sql: &str) -> Option<(String, String)> {
    // 找最后一个含 '.' 的 token
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .find_map(|tok| {
            let t = tok.trim_matches('`');
            let (a, b) = t.split_once('.')?;
            let a = a.trim_matches('`');
            let b = b.trim_matches('`');
            if a.is_empty() || b.is_empty() {
                None
            } else {
                Some((a.to_string(), b.to_string()))
            }
        })
}

/// 从 `show create table db.t` / `show columns from t` 提取表名。
fn extract_table_arg(sql: &str, prefixes: &[&str]) -> Option<String> {
    let lower = sql.to_lowercase();
    for p in prefixes {
        if let Some(pos) = lower.find(p) {
            let rest = sql[pos + p.len()..].trim();
            let tok = rest
                .split(|c: char| c.is_whitespace() || c == ';')
                .next()?
                .trim_matches('`');
            // 支持 db.table
            let name = tok.rsplit('.').next()?;
            return Some(name.trim_matches('`').to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use mycat3_config::default_config;

    fn state() -> Arc<ServerState> {
        ServerState::new(default_config()).unwrap()
    }

    fn session(schema: Option<&str>) -> SessionContext {
        let mut ctx = SessionContext::new(1, "127.0.0.1".into());
        ctx.schema = schema.map(String::from);
        ctx
    }

    #[test]
    fn test_parse_columns_basic() {
        let cols = parse_create_table_columns(
            "CREATE TABLE t (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(64) DEFAULT 'x', amount DECIMAL(10,2), created DATETIME, KEY idx_name (name))",
        );
        assert_eq!(cols.len(), 4);
        assert_eq!(cols[0].name, "id");
        assert!(cols[0].primary && cols[0].auto_increment && !cols[0].nullable);
        assert_eq!(cols[0].column_type, "bigint");
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].default.as_deref(), Some("'x'"));
        assert_eq!(cols[1].column_type, "varchar(64)");
        assert_eq!(char_max_len("varchar(64)"), Some(64));
        assert_eq!(cols[2].column_type, "decimal(10,2)");
        assert_eq!(base_type("decimal(10,2)"), "decimal");
    }

    #[test]
    fn test_parse_columns_backtick_and_table_pk() {
        let cols = parse_create_table_columns(
            "CREATE TABLE `t` (`a` int, `b` varchar(8) NOT NULL, PRIMARY KEY (`a`))",
        );
        assert_eq!(cols.len(), 2);
        assert!(cols[0].primary);
        assert!(!cols[1].nullable);
    }

    #[test]
    fn test_information_schema_tables() {
        let st = state();
        let sess = session(Some("db1"));
        let out = try_information_schema(
            &st,
            &sess,
            "select * from information_schema.tables where table_schema = 'db1'",
        )
        .unwrap()
        .unwrap();
        match out {
            EngineOutcome::Rows { columns, rows } => {
                assert!(columns.iter().any(|c| c.name == "TABLE_NAME"));
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][2].as_deref(), Some("travelrecord"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_information_schema_database_fn_and_like() {
        let st = state();
        let sess = session(Some("db1"));
        // WHERE TABLE_SCHEMA = DATABASE()
        let out = try_information_schema(
            &st,
            &sess,
            "select TABLE_NAME from information_schema.tables where TABLE_SCHEMA = database()",
        )
        .unwrap()
        .unwrap();
        match out {
            EngineOutcome::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
        // LIKE 过滤
        let out = try_information_schema(
            &st,
            &sess,
            "select * from information_schema.tables where table_name like 'travel%'",
        )
        .unwrap()
        .unwrap();
        match out {
            EngineOutcome::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
        let out = try_information_schema(
            &st,
            &sess,
            "select * from information_schema.tables where table_name like 'nope%'",
        )
        .unwrap()
        .unwrap();
        match out {
            EngineOutcome::Rows { rows, .. } => assert_eq!(rows.len(), 0),
            _ => panic!(),
        }
    }

    #[test]
    fn test_sql_like() {
        assert!(sql_like("travelrecord", "travel%"));
        assert!(sql_like("travelrecord", "%record"));
        assert!(sql_like("travelrecord", "tr_vel%"));
        assert!(!sql_like("travelrecord", "x%"));
        assert!(sql_like("abc", "%"));
        assert!(sql_like("", "%"));
        assert!(!sql_like("", "_"));
    }

    #[test]
    fn test_variables() {
        let st = state();
        let sess = session(Some("db1"));
        assert_eq!(
            lookup_variable(&st, &sess, "@@version_comment").as_deref(),
            Some("Mycat3 Pro (Rust)")
        );
        assert_eq!(
            lookup_variable(&st, &sess, "@@session.autocommit").as_deref(),
            Some("1")
        );
        assert_eq!(
            lookup_variable(&st, &sess, "@@transaction_isolation").as_deref(),
            Some("REPEATABLE-READ")
        );
        assert!(lookup_variable(&st, &sess, "@@nonexistent_var_xyz").is_none());
    }

    #[test]
    fn test_show_variables_like() {
        let st = state();
        let sess = session(None);
        match show_variables(&st, &sess, "show variables like 'max_allowed%'") {
            EngineOutcome::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0].as_deref(), Some("max_allowed_packet"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn test_show_create_table_local() {
        let st = state();
        let sess = session(Some("db1"));
        match try_show_local(&st, &sess, "show create table travelrecord").unwrap() {
            EngineOutcome::Rows { rows, .. } => {
                assert_eq!(rows[0][0].as_deref(), Some("travelrecord"));
                assert!(rows[0][1].as_deref().unwrap().contains("CREATE TABLE"));
            }
            _ => panic!(),
        }
        // 未知表 → None（透传后端）
        assert!(try_show_local(&st, &sess, "show create table nosuch").is_none());
    }

    #[test]
    fn test_show_full_tables_and_columns() {
        let st = state();
        let sess = session(Some("db1"));
        match try_show_local(&st, &sess, "show full tables").unwrap() {
            EngineOutcome::Rows { columns, rows } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(rows[0][1].as_deref(), Some("BASE TABLE"));
            }
            _ => panic!(),
        }
        match try_show_local(&st, &sess, "show columns from travelrecord").unwrap() {
            EngineOutcome::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2); // id, name
                assert_eq!(rows[0][0].as_deref(), Some("id"));
                assert_eq!(rows[0][3].as_deref(), Some("PRI"));
            }
            _ => panic!(),
        }
        // desc 形式
        assert!(try_show_local(&st, &sess, "desc travelrecord").is_some());
    }
}

/// 本地 mock SHOW 语句（对应 Java ShowEngines/ShowPlugins/ShowErrors/ShowWarnings/
/// ShowProcedureStatus/ShowStatus 各 Handler：本地返回结果集，不依赖 prototype）。
fn show_local_mock(norm: &str) -> Option<EngineOutcome> {
    let empty2 = |c1: &str, c2: &str| EngineOutcome::Rows {
        columns: vec![col(c1), col(c2)],
        rows: vec![],
    };
    let empty3 = |c1: &str, c2: &str, c3: &str| EngineOutcome::Rows {
        columns: vec![col(c1), col(c2), col(c3)],
        rows: vec![],
    };
    if norm.starts_with("show engines") {
        // 与 Java ShowEnginesSQLHandler 一致的固定引擎表（InnoDB 支持事务/XA/SAVEPOINT）
        let engines: [(&str, &str, &str, &str, &str, &str); 8] = [
            (
                "InnoDB",
                "DEFAULT",
                "Supports transactions, row-level locking, and foreign keys",
                "YES",
                "YES",
                "YES",
            ),
            (
                "MRG_MYISAM",
                "YES",
                "Collection of identical MyISAM tables",
                "NO",
                "NO",
                "NO",
            ),
            (
                "MEMORY",
                "YES",
                "Hash based, stored in memory, useful for temporary tables",
                "NO",
                "NO",
                "NO",
            ),
            (
                "BLACKHOLE",
                "YES",
                "/dev/null storage engine (anything you write to it disappears)",
                "NO",
                "NO",
                "NO",
            ),
            ("MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO"),
            ("CSV", "YES", "CSV storage engine", "NO", "NO", "NO"),
            ("ARCHIVE", "YES", "Archive storage engine", "NO", "NO", "NO"),
            (
                "PERFORMANCE_SCHEMA",
                "YES",
                "Performance Schema",
                "NO",
                "NO",
                "NO",
            ),
        ];
        return Some(EngineOutcome::Rows {
            columns: vec![
                col("Engine"),
                col("Support"),
                col("Comment"),
                col("Transactions"),
                col("XA"),
                col("Savepoints"),
            ],
            rows: engines
                .iter()
                .map(|(a, b, c, d, e, f)| {
                    vec![
                        Some(a.to_string()),
                        Some(b.to_string()),
                        Some(c.to_string()),
                        Some(d.to_string()),
                        Some(e.to_string()),
                        Some(f.to_string()),
                    ]
                })
                .collect(),
        });
    }
    if norm.starts_with("show plugins") {
        return Some(EngineOutcome::Rows {
            columns: vec![
                col("Name"),
                col("Status"),
                col("Type"),
                col("Library"),
                col("License"),
            ],
            rows: vec![],
        });
    }
    if norm.starts_with("show errors") {
        return Some(empty3("Level", "Code", "Message"));
    }
    if norm.starts_with("show warnings") {
        return Some(empty3("Level", "Code", "Message"));
    }
    if norm.starts_with("show procedure status") {
        return Some(EngineOutcome::Rows {
            columns: vec![
                col("Db"),
                col("Name"),
                col("Type"),
                col("Definer"),
                col("Modified"),
                col("Created"),
                col("Security_type"),
                col("Comment"),
                col("character_set_client"),
                col("collation_connection"),
                col("Database Collation"),
            ],
            rows: vec![],
        });
    }
    if norm.starts_with("show status") {
        return Some(empty2("Variable_name", "Value"));
    }
    None
}
