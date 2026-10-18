//! SQL 拦截规则（代理模式 `server.proxy.intercepts`）：
//! 命中规则 → 返回权限错误（默认 1142 / 42000），**在 SQL 缓存与执行之前判定**，
//! 避免命中缓存绕过权限拒绝。
//!
//! 规则：`pattern`（正则，大小写不敏感）+ `stmtType`（ALL/SELECT/DML/DDL/OTHER）
//! + 可选 `errno`/`sqlstate`/`message`。多条规则按配置顺序，首个命中生效。

use mycat3_config::InterceptRule;

use crate::engine::EngineError;

/// 语句类别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StmtClass {
    Select,
    Dml,
    Ddl,
    Other,
}

/// 按首个关键字分类（轻量，不依赖 sqlparser；带前导注释/空白容错）。
pub fn classify(sql: &str) -> StmtClass {
    let s = sql.trim_start();
    // 跳过前导注释（/* */ 与 -- 行注释）
    let mut rest = s;
    loop {
        if let Some(r) = rest.strip_prefix("/*") {
            match r.find("*/") {
                Some(i) => rest = r[i + 2..].trim_start(),
                None => return StmtClass::Other,
            }
        } else if let Some(r) = rest.strip_prefix("--") {
            match r.find('\n') {
                Some(i) => rest = r[i + 1..].trim_start(),
                None => return StmtClass::Other,
            }
        } else {
            break;
        }
    }
    let word: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase();
    match word.as_str() {
        "SELECT" | "WITH" | "SHOW" | "EXPLAIN" | "DESC" | "DESCRIBE" | "TABLE" | "VALUES" => {
            StmtClass::Select
        }
        "INSERT" | "UPDATE" | "DELETE" | "REPLACE" | "MERGE" | "CALL" | "LOAD" => StmtClass::Dml,
        "CREATE" | "ALTER" | "DROP" | "TRUNCATE" | "RENAME" | "COMMENT" | "GRANT" | "REVOKE" => {
            StmtClass::Ddl
        }
        _ => StmtClass::Other,
    }
}

impl StmtClass {
    fn matches(&self, rule: &str) -> bool {
        match rule.trim().to_ascii_uppercase().as_str() {
            "ALL" | "" => true,
            "SELECT" => *self == StmtClass::Select,
            "DML" | "INSERT" | "UPDATE" | "DELETE" => *self == StmtClass::Dml,
            "DDL" => *self == StmtClass::Ddl,
            "OTHER" => *self == StmtClass::Other,
            _ => false,
        }
    }
}

/// 编译后的规则（正则预编译，热路径零解析）。
pub struct CompiledRule {
    pub re: regex::Regex,
    pub stmt: String,
    pub errno: u16,
    pub sqlstate: String,
    pub message: String,
}

impl std::fmt::Debug for CompiledRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledRule")
            .field("pattern", &self.re.as_str())
            .field("stmt", &self.stmt)
            .field("errno", &self.errno)
            .finish()
    }
}

/// 编译配置规则；正则非法返回错误（启动校验与热重载均使用）。
pub fn compile(rules: &[InterceptRule]) -> Result<Vec<CompiledRule>, String> {
    let mut out = Vec::with_capacity(rules.len());
    for (i, r) in rules.iter().enumerate() {
        let re = regex::RegexBuilder::new(&r.pattern)
            .case_insensitive(true)
            .build()
            .map_err(|e| format!("intercept[{}] pattern '{}' invalid: {}", i, r.pattern, e))?;
        let stmt = r.stmt_type.trim().to_ascii_uppercase();
        if !matches!(
            stmt.as_str(),
            "" | "ALL" | "SELECT" | "DML" | "DDL" | "OTHER" | "INSERT" | "UPDATE" | "DELETE"
        ) {
            return Err(format!(
                "intercept[{}] stmtType '{}' invalid",
                i, r.stmt_type
            ));
        }
        out.push(CompiledRule {
            re,
            stmt,
            errno: r.errno.unwrap_or(1142),
            sqlstate: r.sqlstate.clone().unwrap_or_else(|| "42000".to_string()),
            message: r.message.clone().unwrap_or_else(|| {
                "Access denied: statement intercepted by proxy policy".to_string()
            }),
        });
    }
    Ok(out)
}

/// 检查一条 SQL 是否被拦截。命中返回权限错误。
pub fn check(rules: &[CompiledRule], sql: &str) -> Option<EngineError> {
    if rules.is_empty() {
        return None;
    }
    let class = classify(sql);
    for r in rules {
        if !class.matches(&r.stmt) {
            continue;
        }
        if r.re.is_match(sql) {
            return Some(EngineError::new(r.errno, &r.sqlstate, r.message.clone()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(pattern: &str, stmt: &str) -> InterceptRule {
        InterceptRule {
            pattern: pattern.into(),
            stmt_type: stmt.into(),
            message: None,
            errno: None,
            sqlstate: None,
        }
    }

    #[test]
    fn test_classify() {
        assert_eq!(classify("select 1"), StmtClass::Select);
        assert_eq!(classify("  /* c */ SELECT 1"), StmtClass::Select);
        assert_eq!(classify("-- note\ninsert into t values(1)"), StmtClass::Dml);
        assert_eq!(classify("DROP TABLE t"), StmtClass::Ddl);
        assert_eq!(classify("call p()"), StmtClass::Dml);
        assert_eq!(classify("set @x=1"), StmtClass::Other);
    }

    #[test]
    fn test_check_pattern_and_type() {
        let rules = compile(&[
            rule(r"(?i)drop\s+table", "DDL"),
            rule(r"secret_table", "SELECT"),
        ])
        .unwrap();
        assert!(check(&rules, "drop table t").is_some());
        // 类型不匹配：SELECT 规则不看 DDL 语句
        let rules2 = compile(&[rule("t", "SELECT")]).unwrap();
        assert!(check(&rules2, "drop table t").is_none());
        assert!(check(&rules2, "select * from t").is_some());
        // 大小写不敏感
        let rules3 = compile(&[rule("SECRET_TABLE", "ALL")]).unwrap();
        assert!(check(&rules3, "select * from secret_table").is_some());
        // 未命中
        assert!(check(&rules, "select 1").is_none());
        // 空规则
        assert!(check(&[], "anything").is_none());
    }

    #[test]
    fn test_custom_error() {
        let mut r = rule("forbidden", "ALL");
        r.errno = Some(1044);
        r.sqlstate = Some("42000".into());
        r.message = Some("no way".into());
        let rules = compile(&[r]).unwrap();
        let e = check(&rules, "select 'forbidden'").unwrap();
        assert_eq!(e.errno, 1044);
        assert_eq!(e.message, "no way");
    }

    #[test]
    fn test_invalid_pattern_rejected() {
        assert!(compile(&[rule("([", "ALL")]).is_err());
        assert!(compile(&[rule("x", "BADTYPE")]).is_err());
    }
}
