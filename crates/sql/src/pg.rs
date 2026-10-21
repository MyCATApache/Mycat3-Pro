//! MySQL → PostgreSQL 方言翻译（透传模式起步）：
//!
//! PG 与 MySQL 的大多数语法同源（LIMIT/OFFSET/常见函数），Pro 的物理 SQL 由
//! MySQL 方言 AST 重写生成，主要差异是**标识符引用**：MySQL 反引号 → PG 双引号
//!（双引号保大小写，与 MySQL 反引号语义一致；未引用标识符两者均折叠为小写）。
//!
//! 与 Oracle 翻译器策略相同："只改形状，不改语义"——本层仅做引用转换；
//! MySQL 专有语法（如 `` `x`+0 ``、`ON DUPLICATE KEY UPDATE`）会由 PG 自身报错，
//! 绝不静默改写语义。

use sqlparser::ast::{
    AssignmentTarget, CharacterLength, ColumnOption, DataType, ExactNumberInfo, Expr, Ident,
    ObjectName, Statement, TableConstraint, TimezoneInfo, VisitMut, VisitorMut,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use crate::DialectError;

/// MySQL → PostgreSQL 方言翻译。
pub fn translate_sql(sql: &str) -> Result<String, DialectError> {
    if let Some((index, table)) = crate::route::parse_drop_index(sql) {
        let (schema, _) = split_index_table(&table);
        let index_name = if schema.is_empty() {
            format!("{}", pg_quote(index.trim_matches('`')))
        } else {
            format!("{}.{}", pg_quote(schema), pg_quote(index.trim_matches('`')))
        };
        return Ok(format!("DROP INDEX IF EXISTS {}", index_name));
    }
    let stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| DialectError::Parse(e.to_string()))?;
    let mut out = Vec::with_capacity(stmts.len());
    for mut stmt in stmts {
        // 1) 表达式/关系位置的标识符：反引号 → 双引号
        let mut v = PgQuoteVisitor;
        let _ = VisitMut::visit(&mut stmt, &mut v);
        // 2) 非表达式位置的标识符引用：INSERT 列清单 / UPDATE 赋值目标 / DDL 列定义
        match &mut stmt {
            Statement::Insert(insert) => {
                for c in &mut insert.columns {
                    requote(c);
                }
            }
            Statement::Update { assignments, .. } => {
                for a in assignments {
                    if let AssignmentTarget::ColumnName(name) = &mut a.target {
                        for id in &mut name.0 {
                            requote(id);
                        }
                    }
                }
            }
            Statement::CreateTable(ct) => {
                for c in &mut ct.columns {
                    requote(&mut c.name);
                }
            }
            _ => {}
        }
        out.push(stmt.to_string());
    }
    Ok(out.join("; "))
}

/// 反引号标识符 → 双引号（未引用/已双引号保持原样）。
fn requote(id: &mut Ident) {
    if id.quote_style == Some('`') {
        id.quote_style = Some('"');
    }
}

/// 标识符引用转换访问器。
struct PgQuoteVisitor;

impl VisitorMut for PgQuoteVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        for id in &mut relation.0 {
            requote(id);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(id) => requote(id),
            Expr::CompoundIdentifier(parts) => {
                for id in parts {
                    requote(id);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backtick_to_double_quote() {
        let s = translate_sql("select * from `db1`.`orders_0` where `id` = 1").unwrap();
        assert!(s.contains("\"db1\".\"orders_0\""), "{}", s);
        assert!(s.contains("\"id\" = 1"), "{}", s);
        assert!(!s.contains('`'), "{}", s);
    }

    #[test]
    fn test_unquoted_identifiers_preserved() {
        let s = translate_sql("select id, name from orders where id = 1 limit 10").unwrap();
        assert!(s.contains("SELECT id, name FROM orders"), "{}", s);
    }

    #[test]
    fn test_insert_columns_requoted() {
        let s = translate_sql("insert into `dbx`.`t` (`a`, `b`) values (1, 2)").unwrap();
        assert!(s.contains("(\"a\", \"b\")"), "{}", s);
    }
}

fn split_index_table(table: &str) -> (&str, &str) {
    let table = table.trim().trim_matches('`');
    match table.rsplit_once('.') {
        Some((s, t)) => (s.trim().trim_matches('`'), t.trim().trim_matches('`')),
        None => ("", table),
    }
}

// ---------------------------------------------------------------------------
// Physical DDL translation
// ---------------------------------------------------------------------------

/// Convert a MySQL `CREATE TABLE` template into PostgreSQL DDL.
///
/// The returned batch contains `CREATE TABLE IF NOT EXISTS` plus any MySQL
/// `KEY/INDEX` definitions rewritten as `CREATE INDEX IF NOT EXISTS`.
pub fn translate_ddl(sql: &str, schema: &str, table: &str) -> Result<String, DialectError> {
    let mut stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| DialectError::Parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(DialectError::Unsupported(
            "PostgreSQL 物理建表只接受单条 CREATE TABLE".into(),
        ));
    }
    let mut stmt = stmts.pop().unwrap();
    let Statement::CreateTable(ct) = &mut stmt else {
        return Err(DialectError::Unsupported(
            "PostgreSQL 物理建表只支持 CREATE TABLE".into(),
        ));
    };
    ct.if_not_exists = true;
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

    let mut secondary_indexes = Vec::new();
    for col in &mut ct.columns {
        col.data_type = map_pg_ddl_type(col.data_type.clone())?;
        col.collation = None;
        if col.name.quote_style == Some('`') {
            col.name.quote_style = Some('"');
        }
        let mut keep = Vec::with_capacity(col.options.len());
        for opt in col.options.drain(..) {
            match &opt.option {
                ColumnOption::OnUpdate(_) => {
                    return Err(DialectError::Unsupported(
                        "PostgreSQL 不支持 ON UPDATE 列选项".into(),
                    ))
                }
                ColumnOption::CharacterSet(_) | ColumnOption::Comment(_) => {}
                ColumnOption::Generated { .. }
                | ColumnOption::Materialized(_)
                | ColumnOption::Ephemeral(_)
                | ColumnOption::Alias(_)
                | ColumnOption::Options(_) => {
                    return Err(DialectError::Unsupported(
                        "PostgreSQL 不支持的列表达式/生成列选项".into(),
                    ))
                }
                _ => keep.push(opt),
            }
        }
        col.options = keep;
    }

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
                    Ident::new(
                        format!("ix_{}_{}", table, cols)
                            .chars()
                            .take(60)
                            .collect::<String>(),
                    )
                });
                let cols = columns
                    .iter()
                    .map(|c| format!("{}", pg_quote(&c.value.trim_matches('`'))))
                    .collect::<Vec<_>>()
                    .join(", ");
                secondary_indexes.push(format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {} ({})",
                    pg_quote(&index_name.value),
                    pg_qualified(schema, table),
                    cols
                ));
            }
            TableConstraint::FulltextOrSpatial { .. } => {
                return Err(DialectError::Unsupported(
                    "PostgreSQL 物理建表不支持 FULLTEXT/SPATIAL 索引".into(),
                ))
            }
        }
    }
    ct.constraints = constraints;
    ct.name = ObjectName(vec![
        pg_quote(if schema.is_empty() { "public" } else { schema }),
        pg_quote(table),
    ]);

    let mut ddl = stmt.to_string();
    ddl = ddl.replace("AUTO_INCREMENT", "GENERATED BY DEFAULT AS IDENTITY");
    if !secondary_indexes.is_empty() {
        ddl.push_str(";\n");
        ddl.push_str(&secondary_indexes.join(";\n"));
    }
    Ok(ddl)
}

fn pg_quote(value: &str) -> Ident {
    let mut id = Ident::new(value);
    id.quote_style = Some('"');
    id
}

fn pg_qualified(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        format!("{}", pg_quote(table))
    } else {
        format!("{}.{}", pg_quote(schema), pg_quote(table))
    }
}

fn pg_char_len(len: Option<u64>) -> Option<CharacterLength> {
    len.map(|length| CharacterLength::IntegerLength { length, unit: None })
}

fn map_pg_ddl_type(t: DataType) -> Result<DataType, DialectError> {
    use DataType::*;
    Ok(match t {
        TinyInt(_) | UnsignedTinyInt(_) => SmallInt(None),
        SmallInt(_) | Int2(_) | UnsignedSmallInt(_) => Integer(None),
        MediumInt(_) | Int(_) | Integer(_) | Int4(_) | UnsignedInt(_) | UnsignedInteger(_)
        | UnsignedInt4(_) | UnsignedMediumInt(_) => Integer(None),
        BigInt(_) | Int8(_) | UnsignedInt8(_) => BigInt(None),
        UnsignedBigInt(_) => Numeric(ExactNumberInfo::Precision(20)),
        Float(_) | Float4 => Real,
        Real => Real,
        Double | DoublePrecision | Float8 | Float64 => DoublePrecision,
        Decimal(info) | Numeric(info) | Dec(info) => Numeric(info),
        Datetime(_) | Datetime64(_, _) => Timestamp(None, TimezoneInfo::None),
        Timestamp(p, _) => Timestamp(p, TimezoneInfo::None),
        Date => Date,
        Time(p, _) => Time(p, TimezoneInfo::None),
        Bool | Boolean => Boolean,
        Varchar(len) | Char(len) | Character(len) | CharacterVarying(len) => Varchar(len),
        Nvarchar(len) => Varchar(len),
        String(len) => Varchar(pg_char_len(len)),
        Text
        | TinyText
        | MediumText
        | LongText
        | Clob(_)
        | CharacterLargeObject(_)
        | CharLargeObject(_) => Text,
        JSON | JSONB => JSONB,
        Blob(_) | TinyBlob | MediumBlob | LongBlob | Bytea | Binary(_) | Varbinary(_)
        | Bytes(_) => Bytea,
        Uuid => Uuid,
        Enum(_, _) | Set(_) => {
            return Err(DialectError::Unsupported(
                "PostgreSQL 不支持 ENUM/SET 列（请改用 TEXT + CHECK 或独立枚举表）".into(),
            ))
        }
        other => {
            return Err(DialectError::Unsupported(format!(
                "PostgreSQL 物理建表暂不支持类型: {}",
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
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS \"db1_0\".\"orders_0\""),
            "{}",
            sql
        );
        assert!(sql.contains("TIMESTAMP"), "{}", sql);
        assert!(sql.contains("NUMERIC(12,2)"), "{}", sql);
    }

    #[test]
    fn translates_index_and_identity() {
        let sql = translate_ddl(
            "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, a INT, KEY idx_a (a))",
            "db1",
            "t",
        )
        .unwrap();
        assert!(sql.contains("GENERATED BY DEFAULT AS IDENTITY"), "{}", sql);
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS \"idx_a\" ON \"db1\".\"t\" (\"a\")"),
            "{}",
            sql
        );
    }

    #[test]
    fn empty_schema_uses_table_only() {
        let sql = translate_ddl("CREATE TABLE t (id BIGINT)", "", "t").unwrap();
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS \"public\".\"t\""),
            "{}",
            sql
        );
        assert!(!sql.contains("\"\"."), "{}", sql);
    }

    #[test]
    fn rejects_enum() {
        assert!(translate_ddl("CREATE TABLE t (s ENUM('a','b'))", "db1", "t").is_err());
    }
}
