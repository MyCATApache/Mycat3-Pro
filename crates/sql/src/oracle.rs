//! MySQL → Oracle SQL 方言翻译器。
//!
//! 策略（与 Java 版 jOOQ `parsingDataSource` 定位一致）：
//! - **只管语法形状，不管语义**；翻译不了的结构显式报错，绝不静默产生错误语义
//! - 标识符：MySQL 反引号 → Oracle 双引号（保大小写）；Oracle 内建名
//!   （ROWNUM/SYSDATE/SYSTIMESTAMP/NVL/TRUNC/TO_CHAR/DUAL 等）不引用
//! - `LIMIT n [OFFSET m]` → ROWNUM 经典 top-N 包装（版本无关，含 ORDER BY 语义）：
//!   offset=0：`SELECT * FROM (inner ORDER BY ...) WHERE ROWNUM <= n`
//!   offset>0：`SELECT * FROM (SELECT t.*, ROWNUM rn FROM (inner) t WHERE ROWNUM <= n+m) WHERE rn > m`
//! - 多行 `INSERT INTO t VALUES (...),(...)` → `INSERT ALL ... SELECT 1 FROM "DUAL"`
//! - 函数映射：IFNULL/ISNULL→NVL、IF→CASE、CONCAT→||、NOW→LOCALTIMESTAMP、
//!   DATE_FORMAT→TO_CHAR（%-格式转换）、LEFT/RIGHT/SUBSTRING→SUBSTR、
//!   LENGTH→LENGTHB、RAND→DBMS_RANDOM.VALUE、UUID→SYS_GUID、MD5/SHA1→STANDARD_HASH、
//!   DATE_ADD/SUB→INTERVAL 运算、TIMESTAMPDIFF→日期差运算、REGEXP→REGEXP_LIKE 等
//! - 布尔字面量 TRUE/FALSE → 1/0
//! - 明确报错：`INSERT ... ON DUPLICATE KEY UPDATE`、`INSERT IGNORE`、`REPLACE INTO`、
//!   `DELETE ... LIMIT`、`CREATE/DROP TABLE IF [NOT] EXISTS`、LAST_INSERT_ID()、未知函数

use std::ops::ControlFlow;

use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::*;
use sqlparser::ast::{VisitMut, VisitorMut};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DialectError {
    #[error("SQL parse error: {0}")]
    Parse(String),
    #[error("Oracle 不支持: {0}")]
    Unsupported(String),
}

pub type DialectResult<T> = Result<T, DialectError>;

/// 将 MySQL 方言 SQL 翻译为 Oracle 方言 SQL。
pub fn translate_sql(sql: &str) -> DialectResult<String> {
    if let Some((index, table)) = crate::route::parse_drop_index(sql) {
        let (schema, _) = ora_split_index_table(&table);
        let index_name = if schema.is_empty() {
            format!("{}", ora_quote(index.trim_matches('`')))
        } else {
            format!(
                "{}.{}",
                ora_quote(schema),
                ora_quote(index.trim_matches('`'))
            )
        };
        return Ok(format!(
            "BEGIN\n  EXECUTE IMMEDIATE 'DROP INDEX {}';\nEXCEPTION\n  WHEN OTHERS THEN\n    IF SQLCODE != -1418 THEN RAISE; END IF;\nEND;",
            index_name.replace('\'', "''")
        ));
    }
    let mut stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| DialectError::Parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(DialectError::Unsupported(
            "多语句批暂不支持翻译为 Oracle（请逐条执行）".into(),
        ));
    }
    let mut stmt = stmts.pop().unwrap();

    // 1. 语句级预检：明确不支持的构造
    precheck(&stmt)?;

    // 2. LIMIT 包装（先于引用，避免 ROWNUM 被加引号）
    let mut limit_visitor = LimitWrapper { err: None };
    let _ = VisitMut::visit(&mut stmt, &mut limit_visitor);
    if let Some(e) = limit_visitor.err {
        return Err(e);
    }

    // 3. 标识符引用 + 函数翻译
    let mut tx = OracleExprTransformer { err: None };
    let _ = VisitMut::visit(&mut stmt, &mut tx);
    if let Some(e) = tx.err {
        return Err(e);
    }

    // 3.5 非表达式位置的标识符引用：INSERT 列清单 / UPDATE 赋值目标 / DDL 列定义
    match &mut stmt {
        Statement::Insert(insert) => {
            for c in &mut insert.columns {
                if !is_builtin(&c.value) {
                    c.quote_style = Some('"');
                }
            }
        }
        Statement::Update { assignments, .. } => {
            for a in assignments {
                if let AssignmentTarget::ColumnName(name) = &mut a.target {
                    for id in &mut name.0 {
                        if !is_builtin(&id.value) {
                            id.quote_style = Some('"');
                        }
                    }
                }
            }
        }
        Statement::CreateTable(ct) => {
            for c in &mut ct.columns {
                if !is_builtin(&c.name.value) {
                    c.name.quote_style = Some('"');
                }
            }
        }
        _ => {}
    }

    // 4. INSERT 多行重写（在引用完成后，组件 to_string 已是 Oracle 风格）
    if let Statement::Insert(insert) = &stmt {
        if let Some(text) = render_insert_all(insert)? {
            return Ok(text);
        }
    }

    // 5. Oracle 不允许派生表别名的 AS 关键字（`(...) AS "t"` → `(...) "t"`）
    let mut sql_text = stmt.to_string();
    if sql_text.contains(") AS \"") {
        sql_text = sql_text.replace(") AS \"", ") \"");
    }
    Ok(sql_text)
}

// ---------------------------------------------------------------------------
// 语句级预检
// ---------------------------------------------------------------------------

fn precheck(stmt: &Statement) -> DialectResult<()> {
    match stmt {
        Statement::Insert(insert) => {
            if insert.replace_into {
                return Err(DialectError::Unsupported(
                    "REPLACE INTO（请改用 MERGE 或先 DELETE 再 INSERT）".into(),
                ));
            }
            if insert.ignore {
                return Err(DialectError::Unsupported("INSERT IGNORE".into()));
            }
            if insert.on.is_some() {
                return Err(DialectError::Unsupported(
                    "INSERT ... ON DUPLICATE KEY UPDATE（请改用 Oracle MERGE）".into(),
                ));
            }
        }
        Statement::Delete(delete) => {
            if delete.limit.is_some() {
                return Err(DialectError::Unsupported("DELETE ... LIMIT".into()));
            }
        }
        Statement::CreateTable(ct) => {
            if ct.if_not_exists {
                return Err(DialectError::Unsupported(
                    "CREATE TABLE IF NOT EXISTS（Oracle 无此语法；分片物理建表请先 DROP 或手工执行）".into(),
                ));
            }
        }
        Statement::Drop {
            if_exists: true, ..
        } => {
            return Err(DialectError::Unsupported(
                "DROP ... IF EXISTS（Oracle 无此语法）".into(),
            ));
        }
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// LIMIT → ROWNUM 包装
// ---------------------------------------------------------------------------

/// Oracle 内建标识符（翻译器自生成，不引用）。
const ORACLE_BUILTINS: &[&str] = &[
    "ROWNUM",
    "RN",
    "SYSDATE",
    "SYSTIMESTAMP",
    "LOCALTIMESTAMP",
    "CURRENT_TIMESTAMP",
    "SYS_EXTRACT_UTC",
    "SYS_GUID",
    "DUAL",
    "NVL",
    "COALESCE",
    "NULLIF",
    "TRUNC",
    "TO_CHAR",
    "TO_DATE",
    "TO_NUMBER",
    "SUBSTR",
    "LENGTH",
    "LENGTHB",
    "CEIL",
    "FLOOR",
    "ROUND",
    "ABS",
    "MOD",
    "POWER",
    "SQRT",
    "EXP",
    "LN",
    "LOG",
    "SIGN",
    "UPPER",
    "LOWER",
    "LTRIM",
    "RTRIM",
    "REPLACE",
    "INSTR",
    "GREATEST",
    "LEAST",
    "DBMS_RANDOM",
    "VALUE",
    "STANDARD_HASH",
    "LISTAGG",
    "NVL2",
    "REGEXP_LIKE",
    "RPAD",
    "LPAD",
    "TRIM",
    "CHR",
    "ASCII",
    "CONCAT",
    "TO_TIMESTAMP",
    "CAST",
    "EXTRACT",
    "MONTHS_BETWEEN",
    "ADD_MONTHS",
    "LAST_DAY",
    "NEXT_DAY",
    "DECODE",
    "ROWID",
    "VARCHAR2",
    "NUMBER",
    "TIMESTAMP",
];

struct LimitWrapper {
    err: Option<DialectError>,
}

impl VisitorMut for LimitWrapper {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        if q.limit.is_none() && q.offset.is_none() {
            return ControlFlow::Continue(());
        }
        let count = match &q.limit {
            Some(Expr::Value(Value::Number(n, _))) => match n.parse::<u64>() {
                Ok(v) => v,
                Err(_) => {
                    self.err = Some(DialectError::Unsupported("LIMIT 必须是数字字面量".into()));
                    return ControlFlow::Break(());
                }
            },
            Some(_) => {
                self.err = Some(DialectError::Unsupported("LIMIT 必须是数字字面量".into()));
                return ControlFlow::Break(());
            }
            None => u64::MAX,
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
        // 把当前 Query 作为内层（ORDER BY 保留），limit/offset 移除
        q.limit = None;
        q.offset = None;
        let inner = q.clone();

        let rownum_le = |n: u64| Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new("ROWNUM"))),
            op: BinaryOperator::LtEq,
            right: Box::new(Expr::Value(Value::Number(n.to_string(), false))),
        };
        let derived = |subquery: Box<Query>, alias: &str| {
            let mut alias_ident = Ident::new(alias);
            alias_ident.quote_style = Some('"'); // 与内层 "t".* 引用一致，且避免 AS 关键字
            TableFactor::Derived {
                lateral: false,
                subquery,
                alias: Some(TableAlias {
                    name: alias_ident,
                    columns: vec![],
                }),
            }
        };
        let select_wrap =
            |tf: TableFactor, selection: Option<Expr>, projection: Vec<SelectItem>| {
                SetExpr::Select(Box::new(Select {
                    select_token: AttachedToken::empty(),
                    distinct: None,
                    top: None,
                    top_before_distinct: false,
                    projection,
                    into: None,
                    from: vec![TableWithJoins {
                        relation: tf,
                        joins: vec![],
                    }],
                    lateral_views: vec![],
                    selection,
                    group_by: GroupByExpr::Expressions(vec![], vec![]),
                    cluster_by: vec![],
                    distribute_by: vec![],
                    sort_by: vec![],
                    having: None,
                    named_window: vec![],
                    qualify: None,
                    prewhere: None,
                    window_before_qualify: false,
                    value_table_mode: None,
                    connect_by: None,
                }))
            };

        q.body = Box::new(if offset == 0 {
            // SELECT * FROM (inner) t WHERE ROWNUM <= count
            select_wrap(
                derived(Box::new(inner), "t"),
                Some(rownum_le(count)),
                vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
            )
        } else {
            // SELECT * FROM (SELECT t.*, ROWNUM rn FROM (inner) t WHERE ROWNUM <= offset+count)
            //   WHERE rn > offset
            // 中间层必须投影 t.*，否则外层 * 只能展开出 rn 一列（数据列全部丢失）
            let t_ident = {
                let mut i = Ident::new("t");
                i.quote_style = Some('"');
                i
            };
            let middle = select_wrap(
                derived(Box::new(inner), "t"),
                Some(rownum_le(offset.saturating_add(count))),
                vec![
                    SelectItem::QualifiedWildcard(
                        ObjectName(vec![t_ident]),
                        WildcardAdditionalOptions::default(),
                    ),
                    SelectItem::ExprWithAlias {
                        expr: Expr::Identifier(Ident::new("ROWNUM")),
                        alias: Ident::new("rn"),
                    },
                ],
            );
            select_wrap(
                derived(
                    Box::new(Query {
                        with: None,
                        body: Box::new(middle),
                        order_by: None,
                        limit: None,
                        limit_by: vec![],
                        offset: None,
                        fetch: None,
                        locks: vec![],
                        for_clause: None,
                        settings: None,
                        format_clause: None,
                    }),
                    "t2",
                ),
                Some(Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(Ident::new("rn"))),
                    op: BinaryOperator::Gt,
                    right: Box::new(Expr::Value(Value::Number(offset.to_string(), false))),
                }),
                vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())],
            )
        });
        // 继续遍历新 body（内部 Query 的 limit 已被清除，不会死循环）
        ControlFlow::Continue(())
    }
}

// ---------------------------------------------------------------------------
// 表达式变换：标识符引用 + 函数翻译
// ---------------------------------------------------------------------------

struct OracleExprTransformer {
    err: Option<DialectError>,
}

impl VisitorMut for OracleExprTransformer {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<()> {
        for ident in &mut relation.0 {
            if !is_builtin(&ident.value) {
                ident.quote_style = Some('"');
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            Expr::Identifier(i) => {
                if !is_builtin(&i.value) {
                    i.quote_style = Some('"');
                }
            }
            Expr::CompoundIdentifier(parts) => {
                for p in parts.iter_mut() {
                    if !is_builtin(&p.value) {
                        p.quote_style = Some('"');
                    }
                }
            }
            Expr::Function(_) => {
                if let Err(e) = translate_function(expr) {
                    self.err = Some(e);
                    return ControlFlow::Break(());
                }
            }
            Expr::RLike { regexp: true, .. } => {
                // MySQL `a REGEXP b` → REGEXP_LIKE(a, b)
                replace_regexp(expr);
            }
            Expr::Value(Value::Boolean(b)) => {
                let v = if *b { "1" } else { "0" };
                *expr = Expr::Value(Value::Number(v.to_string(), false));
            }
            Expr::Interval(interval) => {
                // MySQL INTERVAL 1 DAY → Oracle INTERVAL '1' DAY（值需加引号）
                if let Expr::Value(Value::Number(n, _)) = &*interval.value {
                    interval.value = Box::new(Expr::Value(Value::SingleQuotedString(n.clone())));
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

fn is_builtin(name: &str) -> bool {
    ORACLE_BUILTINS.iter().any(|b| b.eq_ignore_ascii_case(name))
}

/// REGEXP 处理（在 BinaryOp 中原地替换为 REGEXP_LIKE 函数）。
fn replace_regexp(expr: &mut Expr) {
    let (a, b) = match expr {
        Expr::RLike { expr, pattern, .. } => (expr.clone(), pattern.clone()),
        _ => return,
    };
    let func = Function {
        name: ObjectName(vec![Ident::new("REGEXP_LIKE")]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(*a)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(*b)),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    };
    *expr = Expr::Function(func);
}

/// 按序提取 n 个参数（remove(0) 保持顺序）。
fn take_args(args: &mut Vec<FunctionArg>, n: usize, name: &str) -> DialectResult<Vec<Expr>> {
    if args.len() < n {
        return Err(DialectError::Unsupported(format!("{} 参数不足", name)));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        match args.remove(0) {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => out.push(e),
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
                return Err(DialectError::Unsupported(format!("{} 不支持 * 参数", name)))
            }
            _ => {
                return Err(DialectError::Unsupported(format!(
                    "{} 参数形式不支持",
                    name
                )))
            }
        }
    }
    Ok(out)
}

/// 提取全部剩余参数（CONCAT 变长）。
fn take_args_all(args: &mut Vec<FunctionArg>) -> DialectResult<Vec<Expr>> {
    let n = args.len();
    take_args(args, n, "CONCAT")
}

/// 函数翻译。`expr` 为 `Expr::Function`（或 REGEXP 二元操作），可被整体替换。
fn translate_function(expr: &mut Expr) -> DialectResult<()> {
    if let Expr::RLike { .. } = expr {
        replace_regexp(expr);
        return Ok(());
    }
    let Expr::Function(f) = expr else {
        return Ok(());
    };
    let name = f
        .name
        .0
        .last()
        .map(|i| i.value.to_uppercase())
        .unwrap_or_default();
    let (mut args, dup, clauses) = match std::mem::replace(&mut f.args, FunctionArguments::None) {
        FunctionArguments::List(list) => (list.args, list.duplicate_treatment, list.clauses),
        _ => return Ok(()),
    };
    macro_rules! take {
        ($n:expr) => {
            take_args(&mut args, $n, &name)?
        };
    }

    match name.as_str() {
        // 简单重命名（保持参数）
        "IFNULL" | "ISNULL" => {
            rename(f, "NVL");
            restore_args(f, args, dup, clauses);
        }
        "LCASE" => {
            rename(f, "LOWER");
            restore_args(f, args, dup, clauses);
        }
        "UCASE" => {
            rename(f, "UPPER");
            restore_args(f, args, dup, clauses);
        }
        "CEILING" => {
            rename(f, "CEIL");
            restore_args(f, args, dup, clauses);
        }
        "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
            rename(f, "LENGTH");
            restore_args(f, args, dup, clauses);
        }
        "LENGTH" => {
            rename(f, "LENGTHB");
            restore_args(f, args, dup, clauses);
        }
        "TRUNCATE" => {
            rename(f, "TRUNC");
            restore_args(f, args, dup, clauses);
        }
        "MID" | "SUBSTRING" => {
            rename(f, "SUBSTR");
            restore_args(f, args, dup, clauses);
        }
        "LEFT" => {
            let mut p = take!(2);
            let a = p.remove(0);
            let n = p.remove(0);
            rebuild_args(f, vec![a, Expr::Value(Value::Number("1".into(), false)), n]);
            rename(f, "SUBSTR");
        }
        "RIGHT" => {
            let mut p = take!(2);
            let a = p.remove(0);
            let n = p.remove(0);
            let neg = Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(n),
            };
            rebuild_args(f, vec![a, neg]);
            rename(f, "SUBSTR");
        }
        "CONCAT" => {
            let p = take_args_all(&mut args)?;
            if p.len() < 2 {
                return Err(DialectError::Unsupported("CONCAT 参数不足".into()));
            }
            let mut iter = p.into_iter();
            let mut acc = iter.next().unwrap();
            for next in iter {
                acc = Expr::BinaryOp {
                    left: Box::new(acc),
                    op: BinaryOperator::StringConcat,
                    right: Box::new(next),
                };
            }
            *expr = acc;
        }
        "IF" => {
            let mut p = take!(3);
            let cond = p.remove(0);
            let then = p.remove(0);
            let els = p.remove(0);
            *expr = Expr::Case {
                operand: None,
                conditions: vec![cond],
                results: vec![then],
                else_result: Some(Box::new(els)),
            };
        }
        "NOW" => {
            // 时间伪函数空括号在 Oracle 报 ORA-30088，需裸标识符形式 CURRENT_TIMESTAMP
            take!(0);
            *expr = Expr::Identifier(Ident::new("CURRENT_TIMESTAMP"));
        }
        "CURDATE" => {
            take!(0);
            rebuild_args(f, vec![Expr::Identifier(Ident::new("SYSDATE"))]);
            rename(f, "TRUNC");
        }
        "CURTIME" => {
            take!(0);
            rebuild_args(
                f,
                vec![
                    Expr::Identifier(Ident::new("SYSDATE")),
                    Expr::Value(Value::SingleQuotedString("HH24:MI:SS".into())),
                ],
            );
            rename(f, "TO_CHAR");
        }
        "UTC_TIMESTAMP" | "UTC_TIME" => {
            take!(0);
            rebuild_args(f, vec![Expr::Identifier(Ident::new("SYSTIMESTAMP"))]);
            rename(f, "SYS_EXTRACT_UTC");
        }
        "UNIX_TIMESTAMP" => {
            take!(0);
            let to_date = Expr::Function(make_function(
                "TO_DATE",
                vec![
                    Expr::Value(Value::SingleQuotedString("1970-01-01".into())),
                    Expr::Value(Value::SingleQuotedString("YYYY-MM-DD".into())),
                ],
            ));
            let diff = Expr::BinaryOp {
                left: Box::new(Expr::Identifier(Ident::new("SYSDATE"))),
                op: BinaryOperator::Minus,
                right: Box::new(to_date),
            };
            *expr = Expr::BinaryOp {
                left: Box::new(diff),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Value(Value::Number("86400".into(), false))),
            };
        }
        "DATE_FORMAT" => {
            let mut p = take!(2);
            let d = p.remove(0);
            let fmt = p.remove(0);
            let oracle_fmt = match fmt {
                Expr::Value(Value::SingleQuotedString(s)) => mysql_to_oracle_format(&s),
                _ => {
                    return Err(DialectError::Unsupported(
                        "DATE_FORMAT 格式必须是字符串字面量".into(),
                    ))
                }
            };
            rebuild_args(
                f,
                vec![d, Expr::Value(Value::SingleQuotedString(oracle_fmt))],
            );
            rename(f, "TO_CHAR");
        }
        "STR_TO_DATE" => {
            let mut p = take!(2);
            let v = p.remove(0);
            let fmt = p.remove(0);
            let oracle_fmt = match fmt {
                Expr::Value(Value::SingleQuotedString(s)) => mysql_to_oracle_format(&s),
                _ => {
                    return Err(DialectError::Unsupported(
                        "STR_TO_DATE 格式必须是字符串字面量".into(),
                    ))
                }
            };
            rebuild_args(
                f,
                vec![v, Expr::Value(Value::SingleQuotedString(oracle_fmt))],
            );
            rename(f, "TO_DATE");
        }
        "RAND" => {
            take!(0);
            f.name = ObjectName(vec![Ident::new("DBMS_RANDOM"), Ident::new("VALUE")]);
            rebuild_args(f, vec![]);
        }
        "UUID" => {
            take!(0);
            rebuild_args(f, vec![]);
            rename(f, "SYS_GUID");
        }
        "MD5" | "SHA1" | "SHA" => {
            let mut p = take!(1);
            let v = p.remove(0);
            let algo = if name == "MD5" { "MD5" } else { "SHA1" };
            rebuild_args(
                f,
                vec![v, Expr::Value(Value::SingleQuotedString(algo.into()))],
            );
            rename(f, "STANDARD_HASH");
        }
        "SHA2" => {
            let mut p = take!(2);
            let v = p.remove(0);
            let bits = p.remove(0);
            let algo = match bits {
                Expr::Value(Value::Number(n, _)) => match n.as_str() {
                    "256" => "SHA256",
                    "384" => "SHA384",
                    "512" => "SHA512",
                    other => {
                        return Err(DialectError::Unsupported(format!(
                            "SHA2 位数 {} 不支持（仅 256/384/512）",
                            other
                        )))
                    }
                },
                _ => return Err(DialectError::Unsupported("SHA2 位数必须是字面量".into())),
            };
            rebuild_args(
                f,
                vec![v, Expr::Value(Value::SingleQuotedString(algo.into()))],
            );
            rename(f, "STANDARD_HASH");
        }
        "DATE_ADD" | "ADDDATE" | "DATE_SUB" | "SUBDATE" => {
            let mut p = take!(2);
            let d = p.remove(0);
            let interval = p.remove(0);
            let op = if name == "DATE_SUB" || name == "SUBDATE" {
                BinaryOperator::Minus
            } else {
                BinaryOperator::Plus
            };
            *expr = Expr::BinaryOp {
                left: Box::new(d),
                op,
                right: Box::new(interval),
            };
        }
        "DATEDIFF" => {
            let mut p = take!(2);
            let a = p.remove(0);
            let b = p.remove(0);
            let trunc = |e: Expr| Expr::Function(make_function("TRUNC", vec![e]));
            *expr = Expr::BinaryOp {
                left: Box::new(trunc(a)),
                op: BinaryOperator::Minus,
                right: Box::new(trunc(b)),
            };
        }
        "TIMESTAMPDIFF" => {
            let mut p = take!(3);
            let unit = p.remove(0);
            let a = p.remove(0);
            let b = p.remove(0);
            let unit_name = match &unit {
                Expr::Identifier(i) => i.value.to_uppercase(),
                Expr::Value(Value::SingleQuotedString(s)) => s.to_uppercase(),
                _ => {
                    return Err(DialectError::Unsupported(
                        "TIMESTAMPDIFF 单位必须是字面量".into(),
                    ))
                }
            };
            let diff = |l: Expr, r: Expr| Expr::BinaryOp {
                left: Box::new(l),
                op: BinaryOperator::Minus,
                right: Box::new(r),
            };
            let mul = |e: Expr, n: &str| Expr::BinaryOp {
                left: Box::new(e),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Value(Value::Number(n.into(), false))),
            };
            let paren = |e: Expr| Expr::Nested(Box::new(e));
            *expr = match unit_name.as_str() {
                "SECOND" => mul(paren(diff(b, a)), "86400"),
                "MINUTE" => mul(paren(diff(b, a)), "1440"),
                "HOUR" => mul(paren(diff(b, a)), "24"),
                "DAY" => Expr::Function(make_function("TRUNC", vec![diff(b, a)])),
                "MONTH" => Expr::Function(make_function("MONTHS_BETWEEN", vec![b, a])),
                other => {
                    return Err(DialectError::Unsupported(format!(
                        "TIMESTAMPDIFF 单位 {} 不支持",
                        other
                    )))
                }
            };
        }
        "GROUP_CONCAT" => {
            let mut p = take!(1);
            let col = p.remove(0);
            let col_clone = col.clone();
            rebuild_args(
                f,
                vec![col, Expr::Value(Value::SingleQuotedString(",".into()))],
            );
            rename(f, "LISTAGG");
            f.within_group = vec![OrderByExpr {
                expr: col_clone,
                asc: Some(true),
                nulls_first: None,
                with_fill: None,
            }];
        }
        "CAST" => {
            for a in args.iter_mut() {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                    if let Expr::Cast { data_type, .. } = e {
                        match data_type {
                            DataType::Custom(nm, _)
                                if nm.0.iter().any(|i| {
                                    i.value.eq_ignore_ascii_case("SIGNED")
                                        || i.value.eq_ignore_ascii_case("UNSIGNED")
                                }) =>
                            {
                                *data_type = DataType::Numeric(ExactNumberInfo::None);
                            }
                            DataType::Datetime(_) => {
                                *data_type = DataType::Timestamp(None, TimezoneInfo::None)
                            }
                            _ => {}
                        }
                    }
                }
            }
            restore_args(f, args, dup, clauses);
        }
        "LAST_INSERT_ID" => {
            return Err(DialectError::Unsupported(
                "LAST_INSERT_ID()（Oracle 无此语义，自增列请使用 IDENTITY/序列）".into(),
            ));
        }
        // 同名可直传函数
        "COUNT" | "SUM" | "AVG" | "MAX" | "MIN" | "UPPER" | "LOWER" | "LTRIM" | "RTRIM"
        | "REPLACE" | "ROUND" | "FLOOR" | "CEIL" | "ABS" | "SIGN" | "EXP" | "SQRT" | "POWER"
        | "MOD" | "GREATEST" | "LEAST" | "COALESCE" | "NULLIF" | "TRIM" | "INSTR" | "SUBSTR"
        | "TO_NUMBER" | "TO_CHAR" | "TO_DATE" | "NVL" | "NVL2" | "DECODE" | "TRUNC"
        | "ADD_MONTHS" | "LAST_DAY" | "CHR" | "ASCII" | "LENGTHB" | "REGEXP_LIKE" | "LISTAGG"
        | "STANDARD_HASH" | "SYS_GUID" | "EXTRACT" => {
            restore_args(f, args, dup, clauses);
        }
        "LOG" => {
            let single = args.len() == 1;
            restore_args(f, args, dup, clauses);
            if single {
                rename(f, "LN");
            }
        }
        other => {
            return Err(DialectError::Unsupported(format!(
                "函数 {}(…) 暂不支持翻译为 Oracle（请改写或在后端使用 MySQL）",
                other
            )));
        }
    }
    Ok(())
}

fn rename(f: &mut Function, name: &str) {
    f.name = ObjectName(vec![Ident::new(name)]);
}

/// 恢复原始参数列表（保留 DISTINCT/ALL 修饰与子句）。
fn restore_args(
    f: &mut Function,
    args: Vec<FunctionArg>,
    dup: Option<DuplicateTreatment>,
    clauses: Vec<FunctionArgumentClause>,
) {
    f.args = FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: dup,
        args,
        clauses,
    });
}

fn rebuild_args(f: &mut Function, exprs: Vec<Expr>) {
    f.args = FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args: exprs
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect(),
        clauses: vec![],
    });
}

fn make_function(name: &str, exprs: Vec<Expr>) -> Function {
    Function {
        name: ObjectName(vec![Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: exprs
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

// ---------------------------------------------------------------------------
// INSERT ALL
// ---------------------------------------------------------------------------

/// 多行 VALUES 的 INSERT → `INSERT ALL INTO t (...) VALUES (...) ... SELECT 1 FROM "DUAL"`。
/// 单行或非 VALUES 源返回 None（走普通 to_string）。
fn render_insert_all(insert: &Insert) -> DialectResult<Option<String>> {
    let values = match insert.source.as_deref().map(|q| q.body.as_ref()) {
        Some(SetExpr::Values(v)) => v,
        _ => return Ok(None),
    };
    if values.rows.len() <= 1 {
        return Ok(None);
    }
    let table = insert.table_name.to_string();
    let cols = if insert.columns.is_empty() {
        String::new()
    } else {
        format!(
            " ({})",
            insert
                .columns
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let mut out = String::from("INSERT ALL\n");
    for row in &values.rows {
        let vals = row
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("  INTO {} {} VALUES ({})\n", table, cols, vals));
    }
    out.push_str("SELECT 1 FROM \"DUAL\"");
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// MySQL %-日期格式 → Oracle 格式
// ---------------------------------------------------------------------------

pub fn mysql_to_oracle_format(fmt: &str) -> String {
    let mut out = String::with_capacity(fmt.len() + 4);
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '%' && i + 1 < chars.len() {
            let token = chars[i + 1];
            let mapped = match token {
                'Y' => "YYYY",
                'y' => "YY",
                'm' => "MM",
                'c' => "FMMM",
                'd' => "DD",
                'e' => "FMDD",
                'H' => "HH24",
                'k' => "FMHH24",
                'h' | 'I' => "HH12",
                'l' => "FMHH12",
                'i' => "MI",
                's' | 'S' => "SS",
                'p' => "AM",
                'M' => "MONTH",
                'b' => "MON",
                'a' => "DY",
                'W' => "DAY",
                'j' => "DDD",
                'w' => "D",
                'T' => "HH24:MI:SS",
                'r' => "HH12:MI:SS AM",
                'f' => "FF6",
                '%' => "%",
                other => {
                    // 未识别 token 原样保留（含 %）
                    out.push('%');
                    out.push(other);
                    i += 2;
                    continue;
                }
            };
            out.push_str(mapped);
            i += 2;
        } else {
            out.push(chars[i]);
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(sql: &str) -> String {
        translate_sql(sql).unwrap()
    }

    #[test]
    fn test_identifier_quoting() {
        let s = t("select * from db1_0.orders_0 where id = 1");
        assert!(s.contains("\"db1_0\".\"orders_0\""), "{}", s);
    }

    #[test]
    fn test_limit_offset_wrapping() {
        let s = t("select * from orders order by id desc limit 10");
        assert!(s.contains("ROWNUM <= 10"), "{}", s);
        assert!(!s.contains("LIMIT"), "{}", s);

        let s = t("select * from orders order by id limit 10 offset 5");
        assert!(s.contains("ROWNUM <= 15"), "{}", s);
        assert!(s.contains("rn > 5"), "{}", s);
        // 中间层必须携带数据列投影（BUG：曾有版本只投影 rn，外层 * 丢光业务列）
        assert!(s.contains("\"t\".*"), "{}", s);

        // 无 limit 不包装
        let s = t("select * from orders");
        assert!(!s.contains("ROWNUM"), "{}", s);
    }

    #[test]
    fn test_limit_no_order() {
        let s = t("select count(*) from orders limit 1");
        assert!(s.contains("ROWNUM <= 1"), "{}", s);
    }

    #[test]
    fn test_function_mapping() {
        assert_eq!(
            t("select ifnull(a, 0) from t"),
            "SELECT NVL(\"a\", 0) FROM \"t\""
        );
        assert_eq!(
            t("select if(a=1, 'y', 'n') from t"),
            "SELECT CASE WHEN \"a\" = 1 THEN 'y' ELSE 'n' END FROM \"t\""
        );
        assert_eq!(
            t("select concat(a, b, c) from t"),
            "SELECT \"a\" || \"b\" || \"c\" FROM \"t\""
        );
        assert_eq!(
            t("select left(name, 3) from t"),
            "SELECT SUBSTR(\"name\", 1, 3) FROM \"t\""
        );
        assert_eq!(
            t("select right(name, 3) from t"),
            "SELECT SUBSTR(\"name\", -3) FROM \"t\""
        );
        let s = t("select now()");
        assert_eq!(s, "SELECT CURRENT_TIMESTAMP");
        let s = t("select curdate()");
        assert!(s.contains("TRUNC(SYSDATE)"), "{}", s);
        let s = t("select uuid()");
        assert!(s.contains("SYS_GUID()"), "{}", s);
        let s = t("select md5(x) from t");
        assert!(s.contains("STANDARD_HASH(\"x\", 'MD5')"), "{}", s);
    }

    #[test]
    fn test_regexp() {
        let s = t("select * from t where name regexp '^a'");
        assert!(s.contains("REGEXP_LIKE(\"name\", '^a')"), "{}", s);
    }

    #[test]
    fn test_date_format() {
        assert_eq!(
            mysql_to_oracle_format("%Y-%m-%d %H:%i:%s"),
            "YYYY-MM-DD HH24:MI:SS"
        );
        assert_eq!(mysql_to_oracle_format("%y%m%d"), "YYMMDD");
        let s = t("select date_format(created, '%Y-%m-%d') from t");
        assert!(s.contains("TO_CHAR(\"created\", 'YYYY-MM-DD')"), "{}", s);
    }

    #[test]
    fn test_insert_all() {
        let s = t("insert into orders (id, name) values (1, 'a'), (2, 'b'), (3, 'c')");
        assert!(s.starts_with("INSERT ALL"), "{}", s);
        assert_eq!(s.matches("INTO").count(), 3, "{}", s);
        assert!(s.contains("SELECT 1 FROM \"DUAL\""), "{}", s);
        // 单行保持普通 INSERT
        let s = t("insert into orders (id, name) values (1, 'a')");
        assert!(s.to_uppercase().starts_with("INSERT INTO"), "{}", s);
    }

    #[test]
    fn test_date_add() {
        let s = t("select date_add(created, interval 1 day) from t");
        assert!(s.contains("INTERVAL '1' DAY"), "{}", s);
        assert!(s.contains("\"created\" + INTERVAL"), "{}", s);
        let s = t("select date_sub(created, interval 3 month) from t");
        assert!(s.contains("\"created\" - INTERVAL"), "{}", s);
    }

    #[test]
    fn test_timestampdiff() {
        let s = t("select timestampdiff(day, a, b) from t");
        assert!(s.contains("TRUNC(\"b\" - \"a\")"), "{}", s);
        let s = t("select timestampdiff(hour, a, b) from t");
        assert!(s.contains("(\"b\" - \"a\") * 24"), "{}", s);
    }

    #[test]
    fn test_group_concat() {
        let s = t("select group_concat(name) from t");
        assert!(
            s.contains("LISTAGG(\"name\", ',') WITHIN GROUP (ORDER BY \"name\" ASC)"),
            "{}",
            s
        );
    }

    #[test]
    fn test_boolean_literal() {
        let s = t("select * from t where active = true");
        assert!(s.contains("= 1"), "{}", s);
    }

    #[test]
    fn test_unsupported_errors() {
        assert!(translate_sql("insert into t values (1) on duplicate key update a=1").is_err());
        assert!(translate_sql("insert ignore into t values (1)").is_err());
        assert!(translate_sql("replace into t values (1)").is_err());
        assert!(translate_sql("delete from t limit 5").is_err());
        assert!(translate_sql("drop table if exists t").is_err());
        assert!(translate_sql("create table if not exists t (a int)").is_err());
        assert!(translate_sql("select last_insert_id()").is_err());
        assert!(translate_sql("select json_extract(x, '$.a')").is_err());
    }

    #[test]
    fn test_builtin_not_quoted() {
        let s = t("select * from t where id > 0 limit 3");
        assert!(s.contains("ROWNUM <= 3"), "{}", s);
        assert!(!s.contains("\"ROWNUM\""), "{}", s);
    }

    #[test]
    fn test_sharding_pushdown_shape() {
        // 路由层生成的物理 SQL 形态
        let s = t("SELECT * FROM db2_0.orders_0 WHERE id = 3");
        assert_eq!(s, "SELECT * FROM \"db2_0\".\"orders_0\" WHERE \"id\" = 3");
    }
}

fn ora_split_index_table(table: &str) -> (&str, &str) {
    let table = table.trim().trim_matches('`');
    match table.rsplit_once('.') {
        Some((s, t)) => (s.trim().trim_matches('`'), t.trim().trim_matches('`')),
        None => ("", table),
    }
}

// ---------------------------------------------------------------------------
// Physical DDL translation
// ---------------------------------------------------------------------------

/// Convert a MySQL `CREATE TABLE` template into an idempotent Oracle PL/SQL
/// block (ORA-00955 ignored), including MySQL `KEY/INDEX` definitions.
pub fn translate_ddl(sql: &str, schema: &str, table: &str) -> DialectResult<String> {
    let mut stmts =
        Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| DialectError::Parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(DialectError::Unsupported(
            "Oracle 物理建表只接受单条 CREATE TABLE".into(),
        ));
    }
    let mut stmt = stmts.pop().unwrap();
    let Statement::CreateTable(ct) = &mut stmt else {
        return Err(DialectError::Unsupported(
            "Oracle 物理建表只支持 CREATE TABLE".into(),
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
        col.data_type = map_ora_ddl_type(col.data_type.clone())?;
        col.collation = None;
        col.name = ora_quote(&col.name.value);
        let mut keep = Vec::with_capacity(col.options.len());
        for opt in col.options.drain(..) {
            match &opt.option {
                ColumnOption::OnUpdate(_) => {
                    return Err(DialectError::Unsupported(
                        "Oracle 不支持 ON UPDATE 列选项".into(),
                    ))
                }
                ColumnOption::CharacterSet(_) | ColumnOption::Comment(_) => {}
                ColumnOption::Generated { .. }
                | ColumnOption::Materialized(_)
                | ColumnOption::Ephemeral(_)
                | ColumnOption::Alias(_)
                | ColumnOption::Options(_) => {
                    return Err(DialectError::Unsupported(
                        "Oracle 不支持的列表达式/生成列选项".into(),
                    ))
                }
                _ => keep.push(opt),
            }
        }
        col.options = keep;
    }

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
                columns: columns.into_iter().map(|c| ora_quote(&c.value)).collect(),
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
                index_type_display: KeyOrIndexDisplay::None,
                index_type: None,
                columns: columns.into_iter().map(|c| ora_quote(&c.value)).collect(),
                index_options: vec![],
                characteristics: None,
                nulls_distinct: NullsDistinctOption::None,
            }),
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                characteristics,
            } => constraints.push(TableConstraint::ForeignKey {
                name,
                columns: columns.into_iter().map(|c| ora_quote(&c.value)).collect(),
                foreign_table,
                referred_columns: referred_columns
                    .into_iter()
                    .map(|c| ora_quote(&c.value))
                    .collect(),
                on_delete,
                on_update,
                characteristics,
            }),
            TableConstraint::Check { .. } => constraints.push(constraint),
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
                        format!("IX_{}_{}", table, cols)
                            .chars()
                            .take(128)
                            .collect::<String>(),
                    )
                });
                let cols = columns
                    .iter()
                    .map(|c| format!("{}", ora_quote(c.value.trim_matches('`'))))
                    .collect::<Vec<_>>()
                    .join(", ");
                let index_qualified = if schema.is_empty() {
                    format!("{}", ora_quote(&index_name.value))
                } else {
                    format!("{}.{}", ora_quote(schema), ora_quote(&index_name.value))
                };
                secondary_indexes.push(format!(
                    "CREATE INDEX {} ON {} ({})",
                    index_qualified,
                    ora_qualified(schema, table),
                    cols
                ));
            }
            TableConstraint::FulltextOrSpatial { .. } => {
                return Err(DialectError::Unsupported(
                    "Oracle 物理建表不支持 FULLTEXT/SPATIAL 索引".into(),
                ))
            }
        }
    }
    ct.constraints = constraints;
    ct.name = if schema.is_empty() {
        ObjectName(vec![ora_quote(table)])
    } else {
        ObjectName(vec![ora_quote(schema), ora_quote(table)])
    };

    let create_sql = stmt
        .to_string()
        .replace("AUTO_INCREMENT", "GENERATED BY DEFAULT AS IDENTITY");
    let mut block = String::from("BEGIN\n");
    block.push_str(&ora_exec_block(&create_sql, &[-955]));
    for sql in &secondary_indexes {
        block.push_str(&ora_exec_block(sql, &[-955, -1408]));
    }
    block.push_str("END;");
    Ok(block)
}

fn ora_quote(value: &str) -> Ident {
    let mut id = Ident::new(value.replace('"', "\"\""));
    id.quote_style = Some('"');
    id
}

fn ora_qualified(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        format!("{}", ora_quote(table))
    } else {
        format!("{}.{}", ora_quote(schema), ora_quote(table))
    }
}

fn ora_exec_block(sql: &str, ignore_codes: &[i32]) -> String {
    let escaped = sql.replace('\'', "''");
    let cond = ignore_codes
        .iter()
        .map(|c| format!("SQLCODE != {}", c))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "  BEGIN\n    EXECUTE IMMEDIATE '{}';\n  EXCEPTION\n    WHEN OTHERS THEN\n      IF {} THEN RAISE; END IF;\n  END;\n",
        escaped, cond
    )
}

fn ora_len(len: Option<CharacterLength>, default: u64) -> String {
    match len {
        Some(CharacterLength::IntegerLength { length, .. }) => length.to_string(),
        Some(CharacterLength::Max) | None => default.to_string(),
    }
}

fn ora_type(name: &str, args: Vec<String>) -> DataType {
    DataType::Custom(ObjectName(vec![Ident::new(name)]), args)
}

fn map_ora_ddl_type(t: DataType) -> DialectResult<DataType> {
    use DataType::*;
    Ok(match t {
        TinyInt(x) => {
            if x == Some(1) {
                ora_type("NUMBER", vec!["1".into()])
            } else {
                ora_type("NUMBER", vec!["3".into()])
            }
        }
        UnsignedTinyInt(_) => ora_type("NUMBER", vec!["3".into()]),
        SmallInt(_) | Int2(_) => ora_type("NUMBER", vec!["5".into()]),
        UnsignedSmallInt(_) | UnsignedInt2(_) => ora_type("NUMBER", vec!["5".into()]),
        MediumInt(_) | Int(_) | Integer(_) | Int4(_) => ora_type("NUMBER", vec!["10".into()]),
        UnsignedMediumInt(_) | UnsignedInt(_) | UnsignedInteger(_) | UnsignedInt4(_) => {
            ora_type("NUMBER", vec!["10".into()])
        }
        BigInt(_) | Int8(_) | UnsignedInt8(_) => ora_type("NUMBER", vec!["19".into()]),
        UnsignedBigInt(_) => ora_type("NUMBER", vec!["20".into()]),
        Float(_) | Float4 | Real => ora_type("BINARY_FLOAT", vec![]),
        Double | DoublePrecision | Float8 | Float64 => ora_type("BINARY_DOUBLE", vec![]),
        Decimal(info) | Numeric(info) | Dec(info) => match info {
            ExactNumberInfo::None => ora_type("NUMBER", vec![]),
            ExactNumberInfo::Precision(p) => ora_type("NUMBER", vec![p.to_string()]),
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                ora_type("NUMBER", vec![p.to_string(), s.to_string()])
            }
        },
        Varchar(len) | Nvarchar(len) | CharacterVarying(len) | CharVarying(len) => {
            ora_type("VARCHAR2", vec![ora_len(len, 4000)])
        }
        Char(len) | Character(len) => ora_type("CHAR", vec![ora_len(len, 1)]),
        String(len) => ora_type(
            "VARCHAR2",
            vec![ora_len(
                len.map(|length| CharacterLength::IntegerLength { length, unit: None }),
                4000,
            )],
        ),
        Text
        | TinyText
        | MediumText
        | LongText
        | Clob(_)
        | CharacterLargeObject(_)
        | CharLargeObject(_)
        | JSON
        | JSONB => ora_type("CLOB", vec![]),
        Blob(_) | TinyBlob | MediumBlob | LongBlob | Bytea => ora_type("BLOB", vec![]),
        Binary(len) | Varbinary(len) | Bytes(len) => match len {
            Some(n) if n <= 2000 => ora_type("RAW", vec![n.to_string()]),
            _ => ora_type("BLOB", vec![]),
        },
        Bool | Boolean => ora_type("NUMBER", vec!["1".into()]),
        Date => Date,
        Datetime(_) | Datetime64(_, _) | Timestamp(_, _) => ora_type("TIMESTAMP", vec![]),
        Time(_, _) => return Err(DialectError::Unsupported(
            "Oracle 无 MySQL TIME 语义（INTERVAL 与 TIME 行为不同），请改用 TIMESTAMP 或 VARCHAR2"
                .into(),
        )),
        Uuid => ora_type("VARCHAR2", vec!["36".into()]),
        Enum(_, _) | Set(_) => {
            return Err(DialectError::Unsupported(
                "Oracle 不支持 ENUM/SET 列（请改用 VARCHAR2 + CHECK）".into(),
            ))
        }
        other => {
            return Err(DialectError::Unsupported(format!(
                "Oracle 物理建表暂不支持类型: {}",
                other
            )))
        }
    })
}

#[cfg(test)]
mod ddl_tests {
    use super::*;

    #[test]
    fn translates_create_table() {
        let sql = translate_ddl(
            "CREATE TABLE IF NOT EXISTS __TABLE__ (id BIGINT PRIMARY KEY, name VARCHAR(64), created DATETIME, amount DECIMAL(12,2))",
            "DB1_0",
            "ORDERS_0",
        )
        .unwrap();
        assert!(
            sql.contains("CREATE TABLE \"DB1_0\".\"ORDERS_0\""),
            "{}",
            sql
        );
        assert!(sql.contains("NUMBER(19)"), "{}", sql);
        assert!(sql.contains("VARCHAR2(64)"), "{}", sql);
        assert!(sql.contains("TIMESTAMP"), "{}", sql);
        assert!(sql.contains("EXCEPTION"), "{}", sql);
    }

    #[test]
    fn translates_identity_and_index() {
        let sql = translate_ddl(
            "CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, a INT, KEY idx_a (a))",
            "DB1",
            "T",
        )
        .unwrap();
        assert!(sql.contains("GENERATED BY DEFAULT AS IDENTITY"), "{}", sql);
        assert!(sql.contains("CREATE INDEX \"DB1\".\"idx_a\""), "{}", sql);
    }

    #[test]
    fn empty_schema_uses_table_only() {
        let sql = translate_ddl("CREATE TABLE t (id BIGINT)", "", "t").unwrap();
        assert!(sql.contains("CREATE TABLE \"t\""), "{}", sql);
        assert!(!sql.contains("\"\"."), "{}", sql);
    }

    #[test]
    fn rejects_enum() {
        assert!(translate_ddl("CREATE TABLE t (s ENUM('a','b'))", "DB1", "T").is_err());
    }
}
