//! MySQL → PostgreSQL 方言翻译（透传模式起步）：
//!
//! PG 与 MySQL 的大多数语法同源（LIMIT/OFFSET/常见函数），Pro 的物理 SQL 由
//! MySQL 方言 AST 重写生成，主要差异是**标识符引用**：MySQL 反引号 → PG 双引号
//!（双引号保大小写，与 MySQL 反引号语义一致；未引用标识符两者均折叠为小写）。
//!
//! 与 Oracle 翻译器策略相同："只改形状，不改语义"——本层仅做引用转换；
//! MySQL 专有语法（如 `` `x`+0 ``、`ON DUPLICATE KEY UPDATE`）会由 PG 自身报错，
//! 绝不静默改写语义。

use sqlparser::ast::{AssignmentTarget, Expr, Ident, ObjectName, Statement, VisitMut, VisitorMut};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;

use crate::DialectError;

/// MySQL → PostgreSQL 方言翻译。
pub fn translate_sql(sql: &str) -> Result<String, DialectError> {
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
