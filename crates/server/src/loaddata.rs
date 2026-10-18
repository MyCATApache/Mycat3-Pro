//! 服务器端 CSV 批量导入（对应 Java `HintHandler.loaddata`）：
//!
//! ```sql
//! /*!mycat:loaddata{"schemaName":"db1","tableName":"orders","fileName":"/path/data.csv",
//!                   "delimiter":",","quoteChar":"\"","nullString":"\\N",
//!                   "skipHeaderRecord":true}*/
//! ```
//!
//! 读服务器本地 CSV，按表列构造多行 INSERT（1000 行/批），走正常 `analyse` →
//! 路由/分片 → 执行链路（多行 INSERT 按行拆分发往各物理节点，与手工 INSERT 完全一致）。
//! 参数与 Java 版对齐（Apache Commons CSV `CSVFormat.MYSQL` 默认值）：
//! delimiter=`,`、quoteChar=`"`、escape=`\`、nullString=`\N`、ignoreEmptyLines=true、
//! skipHeaderRecord=false；`header` 数组可覆盖列名（否则从配置 create_sql 推导）。

use std::sync::Arc;

use mycat3_protocol::consts::error_code;

use crate::engine::{EngineError, EngineOutcome};
use crate::executor::{execute_route, ExecResponse};
use crate::session::SessionContext;
use crate::state::ServerState;
use crate::tx::TransactionSession;

/// 每批最多行数（与 Java 一致）。
const BATCH_SIZE: usize = 1000;

/// loaddata 参数（JSON 解析结果）。
#[derive(Debug, Clone)]
pub struct LoadDataArgs {
    pub schema_name: String,
    pub table_name: String,
    pub file_name: String,
    pub delimiter: u8,
    pub quote: u8,
    pub null_string: String,
    pub skip_header: bool,
    pub ignore_empty_lines: bool,
    /// 显式列名（缺省时从表 create_sql 推导）
    pub header: Option<Vec<String>>,
}

/// 解析 `loaddata{...}` JSON 参数。
pub fn parse_args(json: &str) -> Result<LoadDataArgs, EngineError> {
    let v: serde_json::Value = serde_json::from_str(json)
        .map_err(|e| EngineError::new(1064, "42000", format!("loaddata JSON 解析失败: {}", e)))?;
    let get = |k: &str| v.get(k).and_then(|x| x.as_str().map(|s| s.to_string()));
    let schema_name = get("schemaName")
        .ok_or_else(|| EngineError::new(1064, "42000", "loaddata 缺少 schemaName"))?;
    let table_name = get("tableName")
        .ok_or_else(|| EngineError::new(1064, "42000", "loaddata 缺少 tableName"))?;
    let file_name =
        get("fileName").ok_or_else(|| EngineError::new(1064, "42000", "loaddata 缺少 fileName"))?;
    let delimiter = get("delimiter")
        .and_then(|s| s.bytes().next())
        .unwrap_or(b',');
    let quote = get("quoteChar")
        .and_then(|s| s.bytes().next())
        .unwrap_or(b'"');
    let null_string = get("nullString").unwrap_or_else(|| "\\N".to_string());
    let skip_header = v
        .get("skipHeaderRecord")
        .and_then(|x| x.as_bool())
        .unwrap_or(false);
    let ignore_empty_lines = v
        .get("ignoreEmptyLines")
        .and_then(|x| x.as_bool())
        .unwrap_or(true);
    let header = v.get("header").and_then(|h| h.as_array()).map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(|s| s.to_string()))
            .collect()
    });
    Ok(LoadDataArgs {
        schema_name,
        table_name,
        file_name,
        delimiter,
        quote,
        null_string,
        skip_header,
        ignore_empty_lines,
        header,
    })
}

// ---------------------------------------------------------------------------
// 极简 MySQL 风格 CSV 解析器
// ---------------------------------------------------------------------------

/// 流式 CSV 解析：quote 包裹、escape+quote/escape 转义、引号内换行、
/// `#` 注释行、空行忽略、nullString → NULL（None）。
/// 字段按需分配（`String`），不做逐字符 format!/replace 热点操作。
pub struct CsvParser<'a> {
    data: &'a [u8],
    pos: usize,
    delimiter: u8,
    quote: u8,
    escape: u8,
    null_string: &'a [u8],
    ignore_empty_lines: bool,
}

impl<'a> CsvParser<'a> {
    pub fn new(
        data: &'a [u8],
        delimiter: u8,
        quote: u8,
        escape: u8,
        null_string: &'a str,
        ignore_empty_lines: bool,
    ) -> Self {
        CsvParser {
            data,
            pos: 0,
            delimiter,
            quote,
            escape,
            null_string: null_string.as_bytes(),
            ignore_empty_lines,
        }
    }

    /// 下一行记录：`None` = EOF。字段 `None` = NULL（nullString）。
    pub fn next_record(&mut self) -> Option<Vec<Option<String>>> {
        let mut fields: Vec<Option<String>> = Vec::new();
        let mut cur: Vec<u8> = Vec::with_capacity(32);
        let mut in_quotes = false;
        let mut field_started = false;
        let mut any = false;
        loop {
            if self.pos >= self.data.len() {
                // EOF：有未收尾字段则收尾（不完整的最后一行）
                if !any && !field_started {
                    return None;
                }
                fields.push(finish_field(&cur, self.null_string));
                return Some(fields);
            }
            let b = self.data[self.pos];
            // 引号开启（仅字段起始处）
            if b == self.quote && !in_quotes && !field_started {
                in_quotes = true;
                field_started = true;
                any = true;
                self.pos += 1;
                continue;
            }
            if in_quotes {
                if b == self.escape
                    && self.pos + 1 < self.data.len()
                    && (self.data[self.pos + 1] == self.quote
                        || self.data[self.pos + 1] == self.escape)
                {
                    cur.push(self.data[self.pos + 1]);
                    self.pos += 2;
                    any = true;
                    continue;
                }
                if b == self.quote {
                    // 标准 CSV：引号内双引号 = 字面引号
                    if self.pos + 1 < self.data.len() && self.data[self.pos + 1] == self.quote {
                        cur.push(self.quote);
                        self.pos += 2;
                        continue;
                    }
                    in_quotes = false;
                    self.pos += 1;
                    continue;
                }
                cur.push(b);
                self.pos += 1;
                any = true;
                continue;
            }
            match b {
                c if c == self.delimiter => {
                    fields.push(finish_field(&cur, self.null_string));
                    cur = Vec::with_capacity(32);
                    field_started = false;
                    self.pos += 1;
                    any = true;
                }
                b'\n' | b'\r' => {
                    if b == b'\r'
                        && self.pos + 1 < self.data.len()
                        && self.data[self.pos + 1] == b'\n'
                    {
                        self.pos += 1;
                    }
                    self.pos += 1;
                    if !any && self.ignore_empty_lines {
                        continue; // 空行
                    }
                    fields.push(finish_field(&cur, self.null_string));
                    return Some(fields);
                }
                b'#' if !field_started && !any => {
                    // 注释行（CSVFormat.MYSQL commentMarker='#'）：跳到行尾
                    while self.pos < self.data.len() && self.data[self.pos] != b'\n' {
                        self.pos += 1;
                    }
                }
                _ => {
                    cur.push(b);
                    field_started = true;
                    any = true;
                    self.pos += 1;
                }
            }
        }
    }
}

/// 字段收尾：内容等于 nullString → NULL，否则文本。
fn finish_field(cur: &[u8], null_string: &[u8]) -> Option<String> {
    if cur == null_string {
        None
    } else {
        Some(String::from_utf8_lossy(cur).into_owned())
    }
}

// ---------------------------------------------------------------------------
// 导入执行
// ---------------------------------------------------------------------------

/// 执行 loaddata：读文件 → 解析 → 分批 INSERT → 走正常路由/分片。
/// 返回 (affected_rows, last_insert_id=0)。
pub async fn run_import(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    json: &str,
) -> Result<EngineOutcome, EngineError> {
    let args = parse_args(json)?;
    let meta = state.metadata();
    let schema_meta = meta.get_schema(&args.schema_name).ok_or_else(|| {
        EngineError::new(
            1049,
            "42000",
            format!("Unknown database '{}'", args.schema_name),
        )
    })?;
    let table_meta = schema_meta.get_table(&args.table_name).ok_or_else(|| {
        EngineError::new(
            1146,
            "42S02",
            format!(
                "Table '{}.{}' doesn't exist",
                args.schema_name, args.table_name
            ),
        )
    })?;

    // 列名：header 参数优先，否则从配置 create_sql 推导（对应 Java table.getColumns()）
    let columns: Vec<String> = match &args.header {
        Some(h) if !h.is_empty() => h.clone(),
        _ => {
            let create_sql = match table_meta {
                mycat3_sql::TableMeta::Normal { create_sql, .. }
                | mycat3_sql::TableMeta::Global { create_sql, .. }
                | mycat3_sql::TableMeta::Sharding { create_sql, .. } => create_sql.as_deref(),
                _ => None,
            }
            .ok_or_else(|| {
                EngineError::general(format!(
                    "表 {}.{} 无 create_sql（loaddata 需要表已在配置中登记或显式 header）",
                    args.schema_name, args.table_name
                ))
            })?;
            let cols = crate::infoschema::parse_create_table_columns(create_sql);
            if cols.is_empty() {
                return Err(EngineError::general(format!(
                    "无法从 create_sql 推导 {}.{} 的列（可显式提供 header 参数）",
                    args.schema_name, args.table_name
                )));
            }
            cols.into_iter().map(|c| c.name).collect()
        }
    };

    // 读文件（异步 IO）
    let bytes = tokio::fs::read(&args.file_name).await.map_err(|e| {
        EngineError::general(format!("loaddata 读文件失败 {}: {}", args.file_name, e))
    })?;
    let text = String::from_utf8(bytes)
        .map_err(|e| EngineError::general(format!("loaddata 文件非 UTF-8: {}", e)))?;
    let mut parser = CsvParser::new(
        text.as_bytes(),
        args.delimiter,
        args.quote,
        b'\\',
        &args.null_string,
        args.ignore_empty_lines,
    );
    if args.skip_header {
        let _ = parser.next_record();
    }

    let mut total_affected: u64 = 0;
    let mut batch: Vec<Vec<Option<String>>> = Vec::with_capacity(BATCH_SIZE);
    let mut line_no: u64 = if args.skip_header { 1 } else { 0 };
    while let Some(record) = parser.next_record() {
        line_no += 1;
        if record.len() != columns.len() {
            return Err(EngineError::new(
                error_code::ER_WRONG_VALUE_COUNT_ON_ROW,
                "21S01",
                format!(
                    "loaddata 第 {} 行: 列数 {} 与表列数 {} 不匹配",
                    line_no,
                    record.len(),
                    columns.len()
                ),
            ));
        }
        batch.push(record);
        if batch.len() >= BATCH_SIZE {
            total_affected = total_affected
                .saturating_add(exec_batch(state, ctx, tx, &args, &columns, &batch).await?);
            batch.clear();
        }
    }
    if !batch.is_empty() {
        total_affected = total_affected
            .saturating_add(exec_batch(state, ctx, tx, &args, &columns, &batch).await?);
    }
    Ok(EngineOutcome::Ok {
        affected_rows: total_affected,
        last_insert_id: 0,
    })
}

/// 构造一批多行 INSERT 并走正常路由执行（分片感知）。
async fn exec_batch(
    state: &Arc<ServerState>,
    ctx: &mut SessionContext,
    tx: &mut TransactionSession,
    args: &LoadDataArgs,
    columns: &[String],
    rows: &[Vec<Option<String>>],
) -> Result<u64, EngineError> {
    let mut sql = String::with_capacity(rows.len() * columns.len() * 16 + 64);
    sql.push_str("INSERT INTO `");
    sql.push_str(&args.schema_name);
    sql.push_str("`.`");
    sql.push_str(&args.table_name);
    sql.push_str("` (`");
    for (i, c) in columns.iter().enumerate() {
        if i > 0 {
            sql.push_str("`, `");
        }
        sql.push_str(c);
    }
    sql.push_str("`) VALUES ");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push('(');
        for (j, v) in row.iter().enumerate() {
            if j > 0 {
                sql.push(',');
            }
            match v {
                None => sql.push_str("NULL"),
                Some(s) => {
                    sql.push('\'');
                    // 与 Java（Druid SQLCharExpr）一致：仅 '' 转义引号，其余原样
                    for ch in s.chars() {
                        if ch == '\'' {
                            sql.push_str("''");
                        } else {
                            sql.push(ch);
                        }
                    }
                    sql.push('\'');
                }
            }
        }
        sql.push(')');
    }

    let meta = state.metadata();
    let routes = mycat3_sql::analyse(&sql, Some(&args.schema_name), &meta).map_err(|e| {
        EngineError::new(
            error_code::ER_PARSE_ERROR,
            "42000",
            format!("loaddata INSERT 路由失败: {}", e),
        )
    })?;
    let mut affected = 0u64;
    for route in routes {
        match execute_route(state, ctx, tx, &route).await {
            Ok(ExecResponse::Ok { affected_rows, .. }) => {
                affected = affected.saturating_add(affected_rows);
            }
            Ok(_) => {}
            Err(e) => {
                let (errno, sqlstate, msg) = e.to_packet_parts();
                return Err(EngineError::new(errno, &sqlstate, msg));
            }
        }
    }
    Ok(affected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args() {
        let a = parse_args(
            r#"{"schemaName":"db1","tableName":"t","fileName":"/tmp/a.csv","delimiter":";","quoteChar":"'","nullString":"NULL","skipHeaderRecord":true}"#,
        )
        .unwrap();
        assert_eq!(a.schema_name, "db1");
        assert_eq!(a.table_name, "t");
        assert_eq!(a.file_name, "/tmp/a.csv");
        assert_eq!(a.delimiter, b';');
        assert_eq!(a.quote, b'\'');
        assert_eq!(a.null_string, "NULL");
        assert!(a.skip_header);
        // 缺必填字段报错
        assert!(parse_args(r#"{"tableName":"t","fileName":"x"}"#).is_err());
    }

    #[test]
    fn test_csv_basic() {
        let mut p = CsvParser::new(b"a,b,c\n1,2,3\n", b',', b'"', b'\\', "\\N", true);
        let r = p.next_record().unwrap();
        assert_eq!(
            r,
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                Some("c".to_string())
            ]
        );
        let r = p.next_record().unwrap();
        assert_eq!(
            r,
            vec![Some("1".into()), Some("2".into()), Some("3".into())]
        );
        assert!(p.next_record().is_none());
    }

    #[test]
    fn test_csv_quotes_escapes_and_newlines() {
        let data = "\"a,b\",\"it's ok\",\"line1\nline2\"\n\"x\"\"y\",a\\b,3\n";
        let mut p = CsvParser::new(data.as_bytes(), b',', b'"', b'\\', "\\N", true);
        let r = p.next_record().unwrap();
        assert_eq!(r[0].as_deref(), Some("a,b"));
        assert_eq!(r[1].as_deref(), Some("it's ok"));
        assert_eq!(r[2].as_deref(), Some("line1\nline2"));
        let r = p.next_record().unwrap();
        assert_eq!(r[0].as_deref(), Some("x\"y"));
        assert_eq!(r[1].as_deref(), Some("a\\b"));
        assert!(p.next_record().is_none());
    }

    #[test]
    fn test_csv_null_empty_and_comments() {
        let data = "1,\\N,\n# comment line\n2,3,4\n\n\n";
        let mut p = CsvParser::new(data.as_bytes(), b',', b'"', b'\\', "\\N", true);
        let r = p.next_record().unwrap();
        assert_eq!(r[0].as_deref(), Some("1"));
        assert_eq!(r[1], None); // \N → NULL
        assert_eq!(r[2].as_deref(), Some(""));
        let r = p.next_record().unwrap();
        assert_eq!(r[0].as_deref(), Some("2"));
        assert!(p.next_record().is_none()); // 空行忽略
    }

    #[test]
    fn test_csv_trailing_line_without_newline() {
        let mut p = CsvParser::new(b"1,2\n3,4", b',', b'"', b'\\', "\\N", true);
        assert_eq!(p.next_record().unwrap().len(), 2);
        assert_eq!(p.next_record().unwrap().len(), 2);
        assert!(p.next_record().is_none());
    }
}
