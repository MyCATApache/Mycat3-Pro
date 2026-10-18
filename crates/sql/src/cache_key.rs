//! SQL 缓存键归一化与形状匹配（对应 Java `SqlResultSetService` 的 toString 精确键，
//! Pro 语义增强）：
//!
//! 缓存模板（`createSqlCache` 的 sql）中的 **`?` 是自由参数槽位**：
//! - **匹配**：来查询的形状（全部字面量→?）与模板形状一致，且模板中的**固定字面量**
//!   （非 `?` 位置）与来查询对应位置的字面量**逐位相同**；
//! - **变体**：模板 `?` 位置的来查询字面量值构成变体键——**不同参数值组合有各自独立
//!   的缓存结果**（绝不共享）；
//! - **预读**：模板无 `?`（具体 SQL）可预执行 + 周期刷新；含 `?` 的模板参数不可预测，
//!   **无法预读**，按需回填 + 变体级 TTL 惰性刷新。
//!
//! `matchMode`：
//! - `PARAMETERIZED`（默认）：如上（形状匹配 + 槽位约束 + 变体缓存）；
//! - `EXACT`：SQL 文本精确匹配（字面量全部固定），单结果 + 可预读（Java 语义）。
//!
//! LIMIT/OFFSET 字面量不参与参数化（分页是结果形状的一部分，每页独立键）。

use sqlparser::ast::{Expr, Value, VisitMut, VisitorMut};
use std::ops::ControlFlow;

/// 归一化结果：形状键 + 字面量槽位（模板含 `?` 时对应位置为 None）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedQuery {
    /// 形状键：PARAMETERIZED 模式全部字面量→?；EXACT 模式为解析后重序列化全文
    pub shape: String,
    /// 按遍历顺序的字面量槽位：`None` = 模板占位符（自由参数）；`Some(v)` = 字面量
    /// （模板用其做固定值约束；来查询全部为 Some）。
    pub slots: Vec<Option<String>>,
}

impl NormalizedQuery {
    /// 自由参数（模板 `?`）位置的下标。
    pub fn free_indices(&self) -> Vec<usize> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_none())
            .map(|(i, _)| i)
            .collect()
    }
}

/// 归一化缓存 SQL。非单条 SELECT 返回 None（不可缓存语句）。
/// `exact`：EXACT 模式（不做字面量替换）；否则 PARAMETERIZED（字面量→?，模板 ? 保留）。
pub fn normalize_cache_query(sql: &str, exact: bool) -> Option<NormalizedQuery> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let mut stmts =
        sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, sql).ok()?;
    if stmts.len() != 1 {
        return None; // 仅缓存单条语句
    }
    let mut stmt = stmts.pop()?;
    if !matches!(stmt, sqlparser::ast::Statement::Query(_)) {
        return None; // 仅缓存 SELECT
    }
    let mut slots: Vec<Option<String>> = Vec::new();
    if !exact {
        // LIMIT/OFFSET 不参与参数化（分页独立缓存键）
        let (limit, offset) = if let sqlparser::ast::Statement::Query(q) = &mut stmt {
            (q.limit.take(), q.offset.take())
        } else {
            (None, None)
        };
        let mut v = SlotVisitor { slots: &mut slots };
        let _ = VisitMut::visit(&mut stmt, &mut v);
        if let sqlparser::ast::Statement::Query(q) = &mut stmt {
            q.limit = limit;
            q.offset = offset;
        }
    }
    Some(NormalizedQuery {
        shape: stmt.to_string(),
        slots,
    })
}

/// 兼容旧接口：PARAMETERIZED 形状键。
pub fn normalize_cache_sql(sql: &str, parameterized: bool) -> Option<String> {
    normalize_cache_query(sql, !parameterized).map(|q| q.shape)
}

/// 收集字面量槽位并替换为 `?`：模板中已有的 `?`（Placeholder）保留并记 None，
/// 真实字面量记 Some(渲染文本) 并替换为 `?`。
struct SlotVisitor<'a> {
    slots: &'a mut Vec<Option<String>>,
}

impl VisitorMut for SlotVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::Value(v) = expr {
            if matches!(v, Value::Placeholder(_)) {
                self.slots.push(None); // 模板自由参数：保留 ?
            } else {
                self.slots.push(Some(v.to_string()));
                *v = Value::Placeholder("?".to_string());
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_normalizes_whitespace() {
        let a = normalize_cache_sql("  select  *  from t where id = 1 ;", false).unwrap();
        let b = normalize_cache_sql("select * from t where id = 1", false).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_parameterized_shape_and_slots() {
        // 来查询：全部槽位为 Some
        let q =
            normalize_cache_query("select * from t where id = 1 and name = 'x'", false).unwrap();
        assert_eq!(q.slots, vec![Some("1".into()), Some("'x'".into())]);
        assert!(q.shape.contains('?'), "{}", q.shape);

        // 模板含 ?：自由参数记 None
        let t = normalize_cache_query("select * from t where id = ? and name = ?", false).unwrap();
        assert_eq!(t.slots, vec![None, None]);
        assert_eq!(t.free_indices(), vec![0, 1]);

        // 模板无 ?：全部固定
        let t = normalize_cache_query("select * from t where id = 5", false).unwrap();
        assert_eq!(t.slots, vec![Some("5".into())]);
        assert!(t.free_indices().is_empty(), "无 ? 模板可预读");
    }

    #[test]
    fn test_limit_offset_not_parameterized() {
        let a = normalize_cache_sql("select * from t limit 0, 10", true).unwrap();
        let b = normalize_cache_sql("select * from t limit 10, 10", true).unwrap();
        assert_ne!(a, b, "不同页必须不同键");
        // WHERE 字面量仍参数化，LIMIT 保留
        let d = normalize_cache_sql("select * from t where id = 1 limit 0, 10", true).unwrap();
        assert!(d.contains('?'), "{}", d);
        assert!(d.contains("LIMIT 10"), "{}", d);
    }

    #[test]
    fn test_non_select_rejected() {
        assert!(normalize_cache_sql("insert into t values (1)", true).is_none());
        assert!(normalize_cache_sql("update t set a=1", true).is_none());
        assert!(normalize_cache_sql("select 1; select 2", true).is_none());
    }
}
