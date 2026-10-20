//! MySQL → SQL Server (T-SQL) dialect translation.
//!
//! Same contract as the PostgreSQL/Oracle translators: only change syntax
//! shape, never semantics; structures that cannot be translated are rejected
//! explicitly instead of being silently misinterpreted.
//!
//! The MySQL parser is reused because Mycat's routing layer is MySQL-shaped.
//! SQL Server specifics handled here:
//! - identifiers: `` `x` ``/`"x"` → `[x]` (with `]` escaping);
//! - pagination: LIMIT/OFFSET → TOP / OFFSET … FETCH NEXT … ROWS ONLY;
//! - common function/boolean differences;
//! - explicit rejection of MySQL-only upsert/limit/update-lock constructs.

use std::ops::ControlFlow;

use sqlparser::ast::*;
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::DialectError;

pub type MssqlDialectResult<T> = Result<T, DialectError>;

/// Translate MySQL-dialect SQL to T-SQL.
pub fn translate_sql(sql: &str) -> MssqlDialectResult<String> {
    let mut stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| DialectError::Parse(e.to_string()))?;
    let mut out = Vec::with_capacity(stmts.len());
    for mut stmt in stmts.drain(..) {
        precheck(&stmt)?;
        let mut limiter = LimitTransformer { err: None };
        let _ = VisitMut::visit(&mut stmt, &mut limiter);
        if let Some(e) = limiter.err {
            return Err(e);
        }
        let mut tx = TSqlTransformer { err: None };
        let _ = VisitMut::visit(&mut stmt, &mut tx);
        if let Some(e) = tx.err {
            return Err(e);
        }
        let mut text = stmt.to_string();
        // sqlparser renders FETCH FIRST; T-SQL requires FETCH NEXT.
        text = text.replace("FETCH FIRST", "FETCH NEXT");
        // Placeholder inserted for MySQL-style unordered OFFSET.
        text = text.replace("[__MYCAT_MSSQL_ORDER_NULL__]", "(SELECT NULL)");
        text = text.replace("__MYCAT_MSSQL_ORDER_NULL__", "(SELECT NULL)");
        out.push(text);
    }
    Ok(out.join("; "))
}

fn precheck(stmt: &Statement) -> MssqlDialectResult<()> {
    match stmt {
        Statement::Insert(insert) => {
            if insert.replace_into {
                return Err(DialectError::Unsupported(
                    "REPLACE INTO（SQL Server 请改用 MERGE）".into(),
                ));
            }
            if insert.ignore {
                return Err(DialectError::Unsupported(
                    "INSERT IGNORE（SQL Server 无此语法）".into(),
                ));
            }
            if insert.on.is_some() {
                return Err(DialectError::Unsupported(
                    "INSERT ... ON DUPLICATE KEY UPDATE（SQL Server 请改用 MERGE）".into(),
                ));
            }
        }
        Statement::Delete(delete) => {
            if delete.limit.is_some() {
                return Err(DialectError::Unsupported(
                    "DELETE ... LIMIT（SQL Server 请改用 TOP/CTE）".into(),
                ));
            }
        }
        Statement::Query(query) => {
            if !query.locks.is_empty() {
                return Err(DialectError::Unsupported(
                    "SELECT ... FOR UPDATE/LOCK IN SHARE MODE（SQL Server 使用隔离级别/锁提示）"
                        .into(),
                ));
            }
            if !query.limit_by.is_empty() {
                return Err(DialectError::Unsupported(
                    "LIMIT ... BY（SQL Server 无对应语法）".into(),
                ));
            }
            if query.for_clause.is_some() && (query.limit.is_some() || query.offset.is_some()) {
                return Err(DialectError::Unsupported(
                    "FOR XML/JSON + LIMIT/OFFSET 组合暂不支持".into(),
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

struct LimitTransformer {
    err: Option<DialectError>,
}

impl VisitorMut for LimitTransformer {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        if q.limit.is_none() && q.offset.is_none() {
            return ControlFlow::Continue(());
        }
        if q.fetch.is_some() {
            self.err = Some(DialectError::Unsupported(
                "LIMIT 与 FETCH 同时出现暂不支持".into(),
            ));
            return ControlFlow::Break(());
        }
        let limit = match &q.limit {
            Some(Expr::Value(Value::Number(n, _))) => match n.parse::<u64>() {
                Ok(v) => Some(v),
                Err(_) => {
                    self.err = Some(DialectError::Unsupported("LIMIT 必须是数字字面量".into()));
                    return ControlFlow::Break(());
                }
            },
            Some(_) => {
                self.err = Some(DialectError::Unsupported("LIMIT 必须是数字字面量".into()));
                return ControlFlow::Break(());
            }
            None => None,
        };
        let offset = match &q.offset {
            Some(off) => match &off.value {
                Expr::Value(Value::Number(n, _)) => match n.parse::<u64>() {
                    Ok(v) => v,
                    Err(_) => {
                        self.err =
                            Some(DialectError::Unsupported("OFFSET 必须是数字字面量".into()));
                        return ControlFlow::Break(());
                    }
                },
                _ => {
                    self.err = Some(DialectError::Unsupported("OFFSET 必须是数字字面量".into()));
                    return ControlFlow::Break(());
                }
            },
            None => 0,
        };
        q.limit = None;
        q.offset = None;

        // No ORDER BY + no offset: TOP is the most faithful shape.
        if q.order_by.is_none() && offset == 0 {
            if let (Some(n), SetExpr::Select(select)) = (limit, q.body.as_mut()) {
                select.top = Some(Top {
                    with_ties: false,
                    percent: false,
                    quantity: Some(TopQuantity::Constant(n)),
                });
                return ControlFlow::Continue(());
            }
            if offset > 0 {
                self.err = Some(DialectError::Unsupported(
                    "SQL Server 的 OFFSET 需要 ORDER BY".into(),
                ));
                return ControlFlow::Break(());
            }
            // UNION/EXCEPT without ORDER BY and with LIMIT: wrap is not safe
            // without changing column names; reject explicitly.
            self.err = Some(DialectError::Unsupported(
                "无 ORDER BY 的 UNION/LIMIT 暂不支持 SQL Server 翻译".into(),
            ));
            return ControlFlow::Break(());
        }

        if q.order_by.is_none() && offset > 0 {
            // SQL Server requires ORDER BY for OFFSET/FETCH. MySQL permits
            // unordered pagination; use a stable-in-shape placeholder and
            // replace it with `(SELECT NULL)` after rendering. The row order
            // remains unspecified (as in MySQL).
            q.order_by = Some(OrderBy {
                exprs: vec![OrderByExpr {
                    expr: Expr::Identifier(Ident::new("__MYCAT_MSSQL_ORDER_NULL__")),
                    asc: None,
                    nulls_first: None,
                    with_fill: None,
                }],
                interpolate: None,
            });
        }

        if limit.is_some() || offset > 0 {
            q.offset = Some(Offset {
                value: Expr::Value(Value::Number(offset.to_string(), false)),
                rows: OffsetRows::Rows,
            });
            if let Some(n) = limit {
                q.fetch = Some(Fetch {
                    with_ties: false,
                    percent: false,
                    quantity: Some(Expr::Value(Value::Number(n.to_string(), false))),
                });
            }
        }
        ControlFlow::Continue(())
    }
}

struct TSqlTransformer {
    err: Option<DialectError>,
}

impl VisitorMut for TSqlTransformer {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        for id in &mut relation.0 {
            bracket(id);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) => bracket(id),
            Expr::CompoundIdentifier(parts) => {
                for id in parts {
                    bracket(id);
                }
            }
            Expr::Function(_) => {
                if let Err(e) = translate_function(expr) {
                    self.err = Some(e);
                    return ControlFlow::Break(());
                }
            }
            Expr::Value(Value::Boolean(b)) => {
                *expr = Expr::Value(Value::Number(if *b { "1" } else { "0" }.into(), false));
            }
            Expr::RLike { .. } => {
                self.err = Some(DialectError::Unsupported(
                    "REGEXP/RLIKE 没有 SQL Server 等价语义（可用 LIKE 或应用层过滤）".into(),
                ));
                return ControlFlow::Break(());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn bracket(id: &mut Ident) {
    id.quote_style = Some('[');
    // sqlparser's Ident Display does not escape ] for brackets.
    if id.value.contains(']') {
        id.value = id.value.replace(']', "]]");
    }
}

fn translate_function(expr: &mut Expr) -> MssqlDialectResult<()> {
    let Expr::Function(f) = expr else {
        return Ok(());
    };
    let name = f
        .name
        .0
        .last()
        .map(|i| i.value.to_ascii_uppercase())
        .unwrap_or_default();
    match name.as_str() {
        "IFNULL" | "ISNULL" => rename(f, "ISNULL"),
        "LCASE" => rename(f, "LOWER"),
        "UCASE" => rename(f, "UPPER"),
        "IF" => rename(f, "IIF"),
        "CHAR_LENGTH" | "CHARACTER_LENGTH" | "LENGTH" => rename(f, "LEN"),
        "NOW" | "CURRENT_TIMESTAMP" => rename(f, "SYSDATETIME"),
        "UUID" | "UUID_SHORT" => rename(f, "NEWID"),
        "LOCATE" => rename(f, "CHARINDEX"),
        "SUBSTR" => rename(f, "SUBSTRING"),
        "RAND" | "RANDOM" => rename(f, "RAND"),
        "GROUP_CONCAT" => {
            // SQL Server STRING_AGG requires two arguments.
            if let FunctionArguments::List(list) = &mut f.args {
                if list.args.len() == 1 && list.clauses.is_empty() {
                    list.args
                        .push(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(",".into()),
                        ))));
                    rename(f, "STRING_AGG");
                } else {
                    return Err(DialectError::Unsupported(
                        "GROUP_CONCAT 的 SEPARATOR/ORDER BY 形式暂不支持；请改用 STRING_AGG".into(),
                    ));
                }
            }
        }
        "DATE_FORMAT" => {
            return Err(DialectError::Unsupported(
                "DATE_FORMAT 暂不支持（SQL Server 用 FORMAT/CONVERT，格式串语义不同）".into(),
            ));
        }
        "UNIX_TIMESTAMP" => {
            return Err(DialectError::Unsupported(
                "UNIX_TIMESTAMP 暂不支持（SQL Server 用 DATEDIFF_BIG）".into(),
            ));
        }
        // Functions with identical names/arity: LEFT/RIGHT/CONCAT/ABS/ROUND/
        // COALESCE/NULLIF/CAST/SUBSTRING (3 args) etc. are left untouched.
        _ => {}
    }
    Ok(())
}

fn rename(f: &mut Function, new_name: &str) {
    f.name = ObjectName(vec![Ident::new(new_name)]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers() {
        let s = translate_sql("select `id`, `name` from `db1`.`orders` where `id` = 1").unwrap();
        assert!(s.contains("[id]"), "{}", s);
        assert!(s.contains("[db1].[orders]"), "{}", s);
        assert!(!s.contains('`'), "{}", s);
    }

    #[test]
    fn limit_without_order_uses_top() {
        let s = translate_sql("select id from orders limit 10").unwrap();
        assert!(s.contains("TOP"), "{}", s);
        assert!(!s.contains("LIMIT"), "{}", s);
    }

    #[test]
    fn limit_with_order_uses_offset_fetch() {
        let s = translate_sql("select id from orders order by id limit 10 offset 20").unwrap();
        assert!(s.contains("OFFSET 20 ROWS"), "{}", s);
        assert!(s.contains("FETCH NEXT 10 ROWS ONLY"), "{}", s);
        assert!(!s.contains("LIMIT"), "{}", s);
    }

    #[test]
    fn rejects_upsert_and_regexp() {
        assert!(
            translate_sql("insert into t (id) values (1) on duplicate key update id=1").is_err()
        );
        assert!(translate_sql("select * from t where a regexp 'x'").is_err());
    }

    #[test]
    fn translates_common_functions_and_boolean() {
        let s = translate_sql("select ifnull(a, 0), now(), uuid() from t where true").unwrap();
        assert!(s.contains("ISNULL"), "{}", s);
        assert!(s.contains("SYSDATETIME"), "{}", s);
        assert!(s.contains("NEWID"), "{}", s);
        assert!(s.contains("1"), "{}", s);
    }

    #[test]
    fn group_concat_gets_separator() {
        let s = translate_sql("select group_concat(name) from t").unwrap();
        assert!(s.contains("STRING_AGG"), "{}", s);
        assert!(s.contains("','"), "{}", s);
    }
}

// ---------------------------------------------------------------------------
// DDL translation for SQL Server physical tables
// ---------------------------------------------------------------------------

/// Convert a MySQL `CREATE TABLE` template into a T-SQL `CREATE TABLE` batch.
///
/// The template is Mycat's normalized physical DDL (with `__TABLE__`);
/// `schema`/`table` are the physical SQL Server identifiers. Common MySQL
/// types are mapped; unsupported constructs are rejected explicitly.
pub fn translate_ddl(sql: &str, schema: &str, table: &str) -> MssqlDialectResult<String> {
    let mut stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| DialectError::Parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(DialectError::Unsupported(
            "SQL Server 物理建表只接受单条 CREATE TABLE".into(),
        ));
    }
    let mut stmt = stmts.pop().unwrap();
    let Statement::CreateTable(ct) = &mut stmt else {
        return Err(DialectError::Unsupported(
            "SQL Server 物理建表只支持 CREATE TABLE".into(),
        ));
    };
    ct.if_not_exists = false;
    ct.or_replace = false;
    ct.engine = None;
    ct.default_charset = None;
    ct.collation = None;
    ct.table_properties.clear();
    ct.with_options.clear();
    ct.auto_increment_offset = None;
    ct.comment = None;
    ct.partition_by = None;
    ct.cluster_by = None;
    ct.options = None;

    for col in &mut ct.columns {
        col.data_type = map_ddl_type(col.data_type.clone())?;
        col.collation = None;
        let mut keep = Vec::with_capacity(col.options.len());
        for opt in col.options.drain(..) {
            match &opt.option {
                ColumnOption::OnUpdate(_) => {
                    return Err(DialectError::Unsupported(
                        "SQL Server 不支持 ON UPDATE 列选项".into(),
                    ))
                }
                ColumnOption::CharacterSet(_) | ColumnOption::Comment(_) => {}
                ColumnOption::Generated { .. }
                | ColumnOption::Materialized(_)
                | ColumnOption::Ephemeral(_)
                | ColumnOption::Alias(_)
                | ColumnOption::Options(_) => {
                    return Err(DialectError::Unsupported(
                        "SQL Server 不支持的列表达式/生成列选项".into(),
                    ))
                }
                _ => keep.push(opt),
            }
        }
        col.options = keep;
    }
    let schema_name = if schema.is_empty() { "dbo" } else { schema };
    let mut secondary_indexes = Vec::new();
    let mut constraints = Vec::with_capacity(ct.constraints.len());
    for constraint in std::mem::take(&mut ct.constraints) {
        match constraint {
            TableConstraint::PrimaryKey {
                name,
                index_name,
                index_type: _,
                columns,
                index_options: _,
                characteristics: _,
            } => constraints.push(TableConstraint::PrimaryKey {
                name: name.or(index_name),
                index_name: None,
                index_type: None,
                columns,
                index_options: vec![],
                characteristics: None,
            }),
            TableConstraint::Unique {
                name,
                index_name,
                index_type_display: _,
                index_type: _,
                columns,
                index_options: _,
                characteristics: _,
                nulls_distinct: _,
            } => constraints.push(TableConstraint::Unique {
                name: name.or(index_name),
                index_name: None,
                index_type_display: sqlparser::ast::KeyOrIndexDisplay::None,
                index_type: None,
                columns,
                index_options: vec![],
                characteristics: None,
                nulls_distinct: sqlparser::ast::NullsDistinctOption::None,
            }),
            TableConstraint::ForeignKey { .. } | TableConstraint::Check { .. } => {
                constraints.push(constraint)
            }
            TableConstraint::Index {
                name,
                columns,
                index_type: _,
                ..
            } => {
                let index_name = name.unwrap_or_else(|| {
                    let cols = columns
                        .iter()
                        .map(|c| c.value.trim_matches('`'))
                        .collect::<Vec<_>>()
                        .join("_");
                    let generated: String =
                        format!("IX_{}_{}", table, cols).chars().take(128).collect();
                    Ident::new(generated)
                });
                let cols = columns
                    .iter()
                    .map(|c| format!("{}", bracket_ident(&c.value.trim_matches('`'))))
                    .collect::<Vec<_>>()
                    .join(", ");
                secondary_indexes.push(format!(
                    "CREATE INDEX [{}] ON [{}].[{}] ({})",
                    index_name.value.replace(']', "]]"),
                    schema_name.replace(']', "]]"),
                    table.replace(']', "]]"),
                    cols
                ));
            }
            TableConstraint::FulltextOrSpatial { .. } => {
                return Err(DialectError::Unsupported(
                    "SQL Server 物理建表不支持 FULLTEXT/SPATIAL 索引".into(),
                ))
            }
        }
    }
    ct.constraints = constraints;
    ct.name = ObjectName(vec![
        bracket_ident(if schema.is_empty() { "dbo" } else { schema }),
        bracket_ident(table),
    ]);

    let mut ddl = stmt.to_string();
    ddl = ddl.replace("AUTO_INCREMENT", "IDENTITY(1,1)");
    ddl = ddl.replace("BLOB", "VARBINARY(MAX)");
    ddl = ddl.replace("UUID", "UNIQUEIDENTIFIER");
    if !secondary_indexes.is_empty() {
        ddl.push_str(";\n");
        ddl.push_str(&secondary_indexes.join(";\n"));
    }
    // `VARBINARY` without a length is invalid on SQL Server (defaults to 1).
    ddl = ddl.replace("VARBINARY,", "VARBINARY(MAX),");
    ddl = ddl.replace("VARBINARY)", "VARBINARY(MAX))");

    let qualified = format!(
        "[{}].[{}]",
        (if schema.is_empty() { "dbo" } else { schema }).replace(']', "]]"),
        table.replace(']', "]]")
    );
    Ok(format!(
        "IF OBJECT_ID(N'{}', N'U') IS NULL\nBEGIN\n{}\nEND",
        qualified.replace('\'', "''"),
        ddl
    ))
}

fn bracket_ident(value: &str) -> Ident {
    let mut id = Ident::new(value.replace(']', "]]"));
    id.quote_style = Some('[');
    id
}

fn char_len(len: Option<u64>) -> Option<CharacterLength> {
    len.map(|length| CharacterLength::IntegerLength { length, unit: None })
}

fn map_ddl_type(t: DataType) -> MssqlDialectResult<DataType> {
    use DataType::*;
    Ok(match t {
        TinyInt(Some(1)) => Bit(None),
        TinyInt(_) => SmallInt(None),
        UnsignedTinyInt(_) => TinyInt(None),
        SmallInt(_) => SmallInt(None),
        UnsignedSmallInt(_) => Int(None),
        MediumInt(_) => Int(None),
        UnsignedMediumInt(_) => BigInt(None),
        Int(_) | Integer(_) => Int(None),
        UnsignedInt(_) | UnsignedInteger(_) => BigInt(None),
        BigInt(_) => BigInt(None),
        UnsignedBigInt(_) => Decimal(ExactNumberInfo::Precision(20)),
        Float(_) => Float(None),
        Real => Real,
        Double | DoublePrecision => Float(Some(53)),
        Decimal(info) | Numeric(info) | Dec(info) => Decimal(info),
        Datetime(_) => Datetime(None),
        Timestamp(_, _) => Datetime(None),
        Date => Date,
        Time(p, _) => Time(p, TimezoneInfo::None),
        Bool | Boolean => Bit(None),
        Varchar(len) | Char(len) | Character(len) | CharacterVarying(len) => Nvarchar(len),
        String(len) => Nvarchar(char_len(len)),
        Text | TinyText | MediumText | LongText | JSON => Nvarchar(Some(CharacterLength::Max)),
        Blob(_) | TinyBlob | MediumBlob | LongBlob | Bytea => Blob(None),
        Binary(len) | Varbinary(len) => Varbinary(len),
        Uuid => Uuid,
        Enum(_, _) | Set(_) => {
            return Err(DialectError::Unsupported(
                "SQL Server 不支持 ENUM/SET 列（请改用 NVARCHAR + CHECK）".into(),
            ))
        }
        other => {
            return Err(DialectError::Unsupported(format!(
                "SQL Server 物理建表暂不支持类型: {}",
                other
            )))
        }
    })
}

#[cfg(test)]
mod ddl_tests {
    use super::*;

    #[test]
    fn translates_basic_create_table() {
        let sql = translate_ddl(
            "CREATE TABLE IF NOT EXISTS __TABLE__ (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME, amount DECIMAL(12,2))",
            "db1_0",
            "orders_0",
        )
        .unwrap();
        assert!(sql.contains("[db1_0].[orders_0]"), "{}", sql);
        assert!(sql.contains("OBJECT_ID"), "{}", sql);
        assert!(sql.contains("NVARCHAR(64)"), "{}", sql);
        assert!(sql.contains("DECIMAL(12,2)"), "{}", sql);
        assert!(!sql.contains("IF NOT EXISTS"), "{}", sql);
    }

    #[test]
    fn translates_auto_increment_and_text() {
        let sql = translate_ddl(
            "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, body TEXT, payload BLOB)",
            "dbo",
            "t",
        )
        .unwrap();
        assert!(sql.contains("IDENTITY(1,1)"), "{}", sql);
        assert!(sql.contains("NVARCHAR(MAX)"), "{}", sql);
        assert!(sql.contains("VARBINARY(MAX)"), "{}", sql);
    }

    #[test]
    fn translates_mysql_indexes_and_unique_keys() {
        let sql = translate_ddl(
            "CREATE TABLE t (id BIGINT PRIMARY KEY, a INT, b VARCHAR(10), UNIQUE KEY uk_a (a), KEY idx_b (b, a))",
            "db1_0",
            "t",
        )
        .unwrap();
        assert!(sql.contains("UNIQUE (a)"), "{}", sql);
        assert!(!sql.contains("UNIQUE KEY"), "{}", sql);
        assert!(
            sql.contains("CREATE INDEX [idx_b] ON [db1_0].[t] ([b], [a])"),
            "{}",
            sql
        );
    }

    #[test]
    fn rejects_enum() {
        assert!(translate_ddl("CREATE TABLE t (s ENUM('a','b'))", "dbo", "t").is_err());
    }
}
