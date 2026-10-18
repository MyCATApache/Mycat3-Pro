//! SQL 文本级 PREPARE 支持（MySQL / PostgreSQL 前端，会话本地语义）：
//!
//! - PREPARE 语句**不下发后端**：代理会话本地存储（name → SQL 模板），
//!   EXECUTE 时参数替换后走正常 `analyse → 路由 → 方言翻译`（与 COM_STMT/扩展协议
//!   Parse-Bind 同构），因此 MySQL/PG/Oracle 后端天然全支持；
//! - MySQL：`PREPARE n FROM '...'` / `EXECUTE n USING @a,...` / `DEALLOCATE PREPARE n`；
//!   参数为用户变量，需 `SET @a = 字面量` 会话内字面量赋值（非字面量显式报错）；
//! - PG：`PREPARE n [(types)] AS stmt` / `EXECUTE n(args)` / `DEALLOCATE n|ALL`；
//!   参数为内联表达式，直接文本替换进 `$1..$n`；
//! - 每次 EXECUTE 重新解析路由（与协议级预编译一致，DDL 变更自动反映）。

/// 文本预编译语句条目。
#[derive(Debug, Clone)]
pub struct TextPrepared {
    pub sql: String,
    /// 参数个数（MySQL `?` 或 PG `$n`）
    pub params: usize,
}

/// MySQL `PREPARE n FROM 'sql'`：返回 (语句名, SQL, 参数个数)。引号感知。
pub fn parse_mysql_prepare(sql: &str) -> Option<(String, String)> {
    let s = sql.trim();
    let head = s.get(..7)?.to_ascii_lowercase();
    if head != "prepare" || !s[7..].starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let after = s[7..].trim_start();
    let name = take_word(after)?;
    let rest = after[name.len()..].trim_start();
    if !rest.to_ascii_lowercase().starts_with("from") {
        return None;
    }
    let rest = &rest[4..].trim_start();
    let (template, _) = take_quoted_string(rest)?;
    Some((name.to_lowercase(), template))
}

/// MySQL `EXECUTE n [USING @a, @b, ...]`：返回 (语句名, USING 变量名列表)。
pub fn parse_mysql_execute(sql: &str) -> Option<(String, Vec<String>)> {
    let s = sql.trim();
    let head = s.get(..7)?.to_ascii_lowercase();
    if head != "execute" || !s[7..].starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let after = s[7..].trim_start();
    let name = take_word(after)?;
    let rest = after[name.len()..].trim_start();
    let mut vars = Vec::new();
    let rest = if rest.to_ascii_lowercase().starts_with("using") {
        let list = rest[5..].trim_start().trim_end_matches(';');
        for v in list.split(',') {
            let v = v.trim().trim_start_matches('@');
            if v.is_empty() {
                return None;
            }
            vars.push(v.to_lowercase());
        }
        ""
    } else {
        rest.trim_end_matches(';')
    };
    if !rest.is_empty() {
        return None;
    }
    Some((name.to_lowercase(), vars))
}

/// MySQL `DEALLOCATE PREPARE n`：返回语句名。
pub fn parse_mysql_deallocate(sql: &str) -> Option<String> {
    let s = sql.trim();
    let lower = s.to_ascii_lowercase();
    let rest = lower.strip_prefix("deallocate prepare")?.trim_start();
    let name = take_word(rest)?;
    if !rest[name.len()..].trim().trim_end_matches(';').is_empty() {
        return None;
    }
    Some(name.to_lowercase())
}

/// MySQL `SET @a = 字面量 [, @b = 字面量 ...]`：返回 (变量名, 字面量) 列表。
/// 仅字面量赋值可跟踪（数字/引号串/NULL）；其余返回 None（不跟踪、语句照常 OK）。
pub fn parse_mysql_set_vars(sql: &str) -> Option<Vec<(String, String)>> {
    let s = sql.trim().trim_end_matches(';').trim();
    let lower = s.to_ascii_lowercase();
    if !lower.starts_with("set ") {
        return None;
    }
    let body = &s[4..];
    let mut out = Vec::new();
    let mut cur = body;
    loop {
        let cur_t = cur.trim_start();
        if !cur_t.starts_with('@') {
            return None;
        }
        let name = take_word(&cur_t[1..])?.to_lowercase();
        let rest = cur_t[1 + name.len()..].trim_start();
        // 赋值运算符 := 或 =
        let rest = if let Some(stripped) = rest.strip_prefix(":=") {
            stripped
        } else if let Some(stripped) = rest.strip_prefix('=') {
            stripped
        } else {
            return None;
        }
        .trim_start();
        // 字面量：引号串 / NULL / 数字（含负号、小数点、科学计数法）
        let (value, consumed) = if rest.starts_with('\'') || rest.starts_with('"') {
            let (v, consumed) = take_quoted_string(rest)?;
            (format!("'{}'", v), consumed)
        } else {
            let end = rest
                .find(|c: char| c == ',' || c.is_whitespace())
                .unwrap_or(rest.len());
            let v = &rest[..end];
            if v.eq_ignore_ascii_case("null") {
                ("NULL".to_string(), end)
            } else if v.parse::<f64>().is_ok() {
                (v.to_string(), end)
            } else {
                return None; // 表达式/函数：不跟踪
            }
        };
        out.push((name, value));
        let after_val = &rest[consumed..];
        let after_t = after_val.trim_start();
        if after_t.is_empty() {
            return Some(out);
        }
        if !after_t.starts_with(',') {
            return None;
        }
        cur = &after_t[1..];
    }
}

/// PG `PREPARE n [(types)] AS stmt`：返回 (语句名, SQL 模板)。引号感知。
pub fn parse_pg_prepare(sql: &str) -> Option<(String, String)> {
    let s = sql.trim();
    let head = s.get(..7)?.to_ascii_lowercase();
    if head != "prepare" || !s[7..].starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let after = s[7..].trim_start();
    let name = take_word(after)?;
    let mut rest = after[name.len()..].trim_start();
    // 可选 (类型列表)
    if rest.starts_with('(') {
        let mut depth = 0;
        let mut end = None;
        for (i, c) in rest.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(i + 1);
                        break;
                    }
                }
                _ => {}
            }
        }
        rest = rest[end?..].trim_start();
    }
    let lower = rest.to_ascii_lowercase();
    let tpl = lower.strip_prefix("as")?.trim_start();
    Some((name.to_lowercase(), tpl.to_string()))
}

/// PG `EXECUTE n(args)`：返回 (语句名, 参数字面量列表)。引号感知。
pub fn parse_pg_execute(sql: &str) -> Option<(String, Vec<String>)> {
    let s = sql.trim().trim_end_matches(';').trim();
    let head = s.get(..7)?.to_ascii_lowercase();
    if head != "execute" || !s[7..].starts_with(|c: char| c.is_whitespace()) {
        return None;
    }
    let after = s[7..].trim_start();
    let name = take_word(after)?;
    let rest = after[name.len()..].trim_start();
    let args_str = rest.strip_prefix('(')?.trim_end();
    let args_str = args_str.strip_suffix(')')?.trim();
    let mut args = Vec::new();
    if !args_str.is_empty() {
        for a in split_args(args_str) {
            args.push(a.to_string());
        }
    }
    Some((name.to_lowercase(), args))
}

/// PG `DEALLOCATE n | ALL`：Some(name) = 单个；None 且合法 = ALL。
pub fn parse_pg_deallocate(sql: &str) -> Option<Option<String>> {
    let s = sql.trim().trim_end_matches(';').trim();
    let lower = s.to_ascii_lowercase();
    let rest = lower.strip_prefix("deallocate")?.trim_start();
    if rest.eq_ignore_ascii_case("all") {
        return Some(None);
    }
    let name = take_word(rest)?;
    if !rest[name.len()..].trim().is_empty() {
        return None;
    }
    Some(Some(name.to_lowercase()))
}

// ---------------------------------------------------------------------------
// 参数替换
// ---------------------------------------------------------------------------

/// 把参数值按 `?` 占位符替换进模板（MySQL 路径）。值按"数字裸写/其余引号转义"。
pub fn substitute_question_params(template: &str, values: &[String]) -> Option<String> {
    let mut out = String::with_capacity(template.len() + values.len() * 8);
    let bytes = template.as_bytes();
    let mut idx = 0;
    let mut i = 0;
    let mut seg = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' | b'`' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\\' && q != b'`' {
                        i += 2;
                        continue;
                    }
                    if bytes[i] == q {
                        if i + 1 < bytes.len() && bytes[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            }
            b'?' => {
                out.push_str(&template[seg..i]);
                out.push_str(&quote_untyped(values.get(idx)?));
                idx += 1;
                i += 1;
                seg = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&template[seg..]);
    Some(out)
}

/// 把参数值按 `$n` 占位符替换进模板（PG 路径）。
pub fn substitute_dollar_params(template: &str, values: &[String]) -> Option<String> {
    let mut out = String::with_capacity(template.len() + values.len() * 8);
    let bytes = template.as_bytes();
    let mut idx = 0;
    let mut i = 0;
    let mut seg = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let q = bytes[i];
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == q {
                        if i + 1 < bytes.len() && bytes[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            }
            b'$' if i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() => {
                let mut j = i + 1;
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                let n: usize = template[i + 1..j].parse().ok()?;
                if n < 1 || n > values.len() {
                    return None;
                }
                out.push_str(&template[seg..i]);
                out.push_str(&quote_untyped(&values[n - 1]));
                idx += 1;
                i = j;
                seg = i;
            }
            _ => i += 1,
        }
    }
    out.push_str(&template[seg..]);
    Some(out)
}

/// 无类型字面量引用：NULL 原样、数字裸写、已是 SQL 引号字面量原样保留、
/// 其余裸内容 `'...'` 转义。
fn quote_untyped(v: &str) -> String {
    let t = v.trim();
    if t.eq_ignore_ascii_case("null") {
        return "NULL".to_string();
    }
    let quoted =
        (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"'));
    if quoted {
        // 已是合法 SQL 引号字面量（含 '' 转义）：原样保留，避免双重转义
        return t.to_string();
    }
    if !t.is_empty() && t.parse::<f64>().is_ok() {
        return t.to_string();
    }
    let mut s = String::with_capacity(t.len() + 2);
    s.push('\'');
    for ch in t.chars() {
        if ch == '\'' {
            s.push_str("''");
        } else {
            s.push(ch);
        }
    }
    s.push('\'');
    s
}

// ---------------------------------------------------------------------------
// 词法辅助
// ---------------------------------------------------------------------------

/// 连续词（字母/数字/下划线）的结束下标；`pos` 必须位于词首。
fn take_word(s: &str) -> Option<&str> {
    let b = s.as_bytes();
    let mut end = 0;
    while end < b.len() && (b[end].is_ascii_alphanumeric() || b[end] == b'_') {
        end += 1;
    }
    if end == 0 {
        None
    } else {
        Some(&s[..end])
    }
}

/// 引号字符串（'' 转义）：返回 (内容, 消费字节数)。
fn take_quoted_string(s: &str) -> Option<(String, usize)> {
    let b = s.as_bytes();
    let q = *b.first()?;
    if q != b'\'' && q != b'"' {
        return None;
    }
    let mut i = 1;
    let mut out = String::new();
    while i < b.len() {
        if b[i] == b'\\' && i + 1 < b.len() {
            out.push(b[i + 1] as char);
            i += 2;
            continue;
        }
        if b[i] == q {
            if i + 1 < b.len() && b[i + 1] == q {
                out.push(q as char);
                i += 2;
                continue;
            }
            return Some((out, i + 1));
        }
        out.push(b[i] as char);
        i += 1;
    }
    None
}

/// 顶层分号切分（PG 简单查询多语句；引号感知，字符串内分号不切）。
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut quote: Option<char> = None;
    let mut start = 0;
    for (i, c) in sql.char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                ';' => {
                    let stmt = sql[start..i].trim();
                    if !stmt.is_empty() {
                        out.push(stmt.to_string());
                    }
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    let tail = sql[start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

/// 引号感知的逗号切分（PG EXECUTE 参数列表）。
fn split_args(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0;
    let mut quote: Option<char> = None;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match quote {
            Some(q) => {
                if c == q {
                    quote = None;
                }
            }
            None => match c {
                '\'' | '"' => quote = Some(c),
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    out.push(s[start..i].trim());
                    start = i + 1;
                }
                _ => {}
            },
        }
    }
    out.push(s[start..].trim());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_prepare() {
        let (n, sql) =
            parse_mysql_prepare("PREPARE s1 FROM 'select * from t where id = ?'").unwrap();
        assert_eq!(n, "s1");
        assert!(sql.contains('?'));
        // 引号内 '' 转义
        let (_, sql) = parse_mysql_prepare("prepare p2 from 'select ''x'' as v'").unwrap();
        assert_eq!(sql, "select 'x' as v");
        assert!(parse_mysql_prepare("select 1").is_none());
    }

    #[test]
    fn test_mysql_execute_and_deallocate() {
        let (n, vars) = parse_mysql_execute("EXECUTE s1 USING @a, @b").unwrap();
        assert_eq!(n, "s1");
        assert_eq!(vars, vec!["a", "b"]);
        let (n, vars) = parse_mysql_execute("execute s2").unwrap();
        assert_eq!(n, "s2");
        assert!(vars.is_empty());
        assert_eq!(
            parse_mysql_deallocate("DEALLOCATE PREPARE s1").unwrap(),
            "s1"
        );
        assert!(parse_mysql_deallocate("deallocate prepare").is_none());
    }

    #[test]
    fn test_mysql_set_vars() {
        let v = parse_mysql_set_vars("SET @a = 1, @b = 'x', @c = null").unwrap();
        assert_eq!(
            v,
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "'x'".to_string()),
                ("c".to_string(), "NULL".to_string()),
            ]
        );
        let v = parse_mysql_set_vars("SET @d := 2.5").unwrap();
        assert_eq!(v[0], ("d".to_string(), "2.5".to_string()));
        // 非字面量 → 不跟踪
        assert!(parse_mysql_set_vars("SET @e = (select max(id) from t)").is_none());
        assert!(parse_mysql_set_vars("SET names utf8").is_none());
    }

    #[test]
    fn test_pg_prepare_execute_deallocate() {
        let (n, sql) = parse_pg_prepare("PREPARE p1 AS select * from t where id = $1").unwrap();
        assert_eq!(n, "p1");
        assert!(sql.contains("$1"));
        let (n, sql) = parse_pg_prepare("prepare p2 (int, text) as select $1, $2").unwrap();
        assert_eq!(n, "p2");
        assert!(sql.contains("$2"));
        let (n, args) = parse_pg_execute("EXECUTE p1(1, 'x')").unwrap();
        assert_eq!(n, "p1");
        assert_eq!(args, vec!["1", "'x'"]);
        let (n, args) = parse_pg_execute("execute p1()").unwrap();
        assert!(args.is_empty());
        assert_eq!(n, "p1");
        assert_eq!(
            parse_pg_deallocate("DEALLOCATE p1").unwrap(),
            Some("p1".into())
        );
        assert_eq!(parse_pg_deallocate("deallocate all").unwrap(), None);
    }

    #[test]
    fn test_split_statements() {
        let v = split_statements("select 1; select 'a;b'; select 2");
        assert_eq!(v, vec!["select 1", "select 'a;b'", "select 2"]);
        assert!(split_statements("  ").is_empty());
        assert_eq!(split_statements("select 1").len(), 1);
    }

    #[test]
    fn test_substitution() {
        let t = "select * from t where id = ? and name = ?";
        let out = substitute_question_params(t, &["1".into(), "'o''b'".into()]).unwrap();
        assert_eq!(out, "select * from t where id = 1 and name = 'o''b'");
        // 引号内 ? 不替换
        let out = substitute_question_params("select '?' as v, ?", &["5".into()]).unwrap();
        assert_eq!(out, "select '?' as v, 5");
        // PG $n
        let out = substitute_dollar_params(
            "select $1, $2 from t where a = $2",
            &["1".into(), "'x'".into()],
        )
        .unwrap();
        assert_eq!(out, "select 1, 'x' from t where a = 'x'");
        assert!(substitute_dollar_params("select $3", &["1".into()]).is_none());
    }
}
