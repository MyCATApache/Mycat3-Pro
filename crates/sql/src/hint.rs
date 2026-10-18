//! Mycat 提示（hint）解析。对应 `MycatdbCommand.getHintRoute` 与会话 hint 指令：
//! `/*!mycat:target=c0*/ select ...`、`/*!mycat:sql=...*/`、`/*!mycat:master*/` 等。

/// 解析出的 hint 指令。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hint {
    /// 强制路由到的 target（TARGET= / DATANODE=）
    pub target: Option<String>,
    /// 强制 schema（SCHEMA=）
    pub schema: Option<String>,
    /// 强制读主库
    pub master: bool,
    /// 强制读从库
    pub slave: bool,
    /// `sql=` 内嵌 SQL：直接透传执行
    pub inner_sql: Option<String>,
    /// `showTopology{表名}`：返回表物理拓扑（对应 Java HintHandler.showTopology）
    pub show_topology: Option<String>,
    /// `executeTimeout=n`（毫秒）：语句级超时（对应 Java EXECUTE_TIMEOUT）
    pub execute_timeout_ms: Option<u64>,
    /// `showConfigText`：导出配置全文 JSON（对应 Java HintHandler.showConfigText）
    pub show_config_text: bool,
    /// `showSlowSql[{"trace_id": n}]`：慢 SQL 记录（Some(Some(id)) = 按 trace_id 过滤）
    pub show_slow_sql: Option<Option<u64>>,
    /// `setSqlTimeFilter{"value": n}`：设置慢 SQL 阈值（毫秒）
    pub set_sql_time_filter: Option<u64>,
    /// `getSqlTimeFilter`：查询当前慢 SQL 阈值
    pub get_sql_time_filter: bool,
    /// `loaddata{...}`：服务器端 CSV 批量导入参数 JSON（对应 Java HintHandler.loaddata）
    pub loaddata: Option<String>,
    /// 配置管理/运维指令（`createDataSource{...}`、`pauseServer{...}` 等）：
    /// 指令名（小写）→ 参数 JSON（对应 Java mycatDmlHandler 的 cmd=body 组）
    pub command: Option<(String, String)>,
    /// 未识别的 mycat 指令 token（调用方据此显式报错而非静默忽略）
    pub unknown: Vec<String>,
}

impl Hint {
    /// 是否识别出任何有效指令（unknown 报错判定的依据）。
    pub fn is_empty(&self) -> bool {
        self.target.is_none()
            && self.schema.is_none()
            && !self.master
            && !self.slave
            && self.inner_sql.is_none()
            && self.show_topology.is_none()
            && self.execute_timeout_ms.is_none()
            && !self.show_config_text
            && self.show_slow_sql.is_none()
            && self.set_sql_time_filter.is_none()
            && !self.get_sql_time_filter
            && self.loaddata.is_none()
            && self.command.is_none()
    }
}

/// 若 SQL 以 `/*!mycat:...*/` 开头则解析 hint，返回 (hint, 剩余 SQL)。
/// 非 hint 输入原样返回。
pub fn parse_hint(sql: &str) -> (Option<Hint>, String) {
    let trimmed = sql.trim_start();
    if !trimmed.starts_with("/*!mycat:") {
        return (None, sql.to_string());
    }
    let end = match trimmed.find("*/") {
        Some(i) => i,
        None => return (None, sql.to_string()),
    };
    let body = &trimmed["/*!mycat:".len()..end];
    let rest = trimmed[end + 2..].trim_start().to_string();

    let mut hint = Hint::default();
    // sql= 允许包含空格的原始 SQL，优先处理
    if let Some(pos) = body.to_lowercase().find("sql=") {
        let (before, inner) = body.split_at(pos);
        hint.inner_sql = Some(inner["sql=".len()..].trim().to_string());
        parse_tokens(before, &mut hint);
        return (Some(hint), rest);
    }
    // 花括号 JSON 指令（JSON 内含逗号/空格，不能按 token 切分）：整体捕获后遮蔽。
    // 支持 Java 的 `cmd={...}` 与 Pro 的 `cmd{...}` 两种写法（键与 { 之间可带 = 与空白）。
    let mut captured: std::collections::BTreeMap<&str, String> = Default::default();
    let mut masked = body.to_string();
    let braced_keys = [
        "loaddata",
        "setsqltimefilter",
        "showslowsql",
        "createdatasource",
        "dropdatasource",
        "createcluster",
        "dropcluster",
        "createuser",
        "dropuser",
        "setsequence",
        "createschema",
        "dropschema",
        "setreadytoclosesql",
        "pauseserver",
        "createsqlcache",
        "dropsqlcache",
        "showsqlcaches",
        "setuserdialect",
    ];
    for key in braced_keys {
        let lower = masked.to_lowercase();
        let Some(pos) = lower.find(key) else {
            continue;
        };
        // 键后可选 = 与空白，再到 '{'
        let after_key = &masked[pos + key.len()..];
        let mut idx = 0;
        while idx < after_key.len()
            && (after_key.as_bytes()[idx] == b'='
                || after_key.as_bytes()[idx].is_ascii_whitespace())
        {
            idx += 1;
        }
        if after_key.as_bytes().get(idx) != Some(&b'{') {
            continue;
        }
        // 连同外层花括号捕获，保证是合法 JSON 文档
        let start = pos + key.len() + idx;
        let mut depth = 1usize;
        let mut in_str = false;
        let mut end_pos = start + 1;
        let b = masked.as_bytes();
        while end_pos < b.len() {
            match b[end_pos] {
                b'\\' if in_str => {
                    end_pos += 1; // JSON 转义序列（如 \"）：跳过下一字符
                }
                b'"' => in_str = !in_str,
                b'{' if !in_str => depth += 1,
                b'}' if !in_str => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            end_pos += 1;
        }
        if depth == 0 {
            let json = masked[start..end_pos + 1].to_string();
            masked.replace_range(pos..end_pos + 1, " ");
            captured.insert(key, json);
        }
    }
    parse_tokens(&masked, &mut hint);
    if let Some(json) = captured.remove("loaddata") {
        hint.loaddata = Some(json);
    }
    // 仅花括号形式覆盖（裸 showSlowSql 已由 parse_tokens 置 Some(None)）
    if let Some(json) = captured.remove("showslowsql") {
        hint.show_slow_sql = Some(
            serde_json::from_str::<serde_json::Value>(&json)
                .ok()
                .and_then(|v| v.get("trace_id").and_then(|t| t.as_u64())),
        );
    }
    if let Some(json) = captured.remove("setsqltimefilter") {
        // Java 的 value 是字符串（如 "500"），两种形式都接受
        hint.set_sql_time_filter = serde_json::from_str::<serde_json::Value>(&json)
            .ok()
            .and_then(|v| v.get("value").cloned())
            .and_then(|t| {
                t.as_u64()
                    .or_else(|| t.as_str().and_then(|s| s.parse::<u64>().ok()))
            });
    }
    // 其余花括号指令 → 通用 (指令名, JSON)（配置管理/运维命令，engine 层分派）
    if hint.command.is_none() {
        for key in braced_keys {
            if let Some(json) = captured.remove(key) {
                hint.command = Some((key.to_string(), json));
                break;
            }
        }
    }
    (Some(hint), rest)
}

fn parse_tokens(body: &str, hint: &mut Hint) {
    for token in body.split(|c: char| c == ',' || c.is_whitespace() || c == ';') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let lower = token.to_lowercase();
        if let Some((k, _)) = lower.split_once('=') {
            let v = token[k.len() + 1..]
                .trim()
                .trim_matches(|c| c == '\'' || c == '"');
            match k.trim() {
                "target" | "datanode" => hint.target = Some(v.to_string()),
                "schema" => hint.schema = Some(v.to_string()),
                "index" => hint.target = Some(v.to_string()),
                "executetimeout" => {
                    hint.execute_timeout_ms = v.parse::<u64>().ok();
                }
                "showtopology" => hint.show_topology = Some(v.to_string()),
                // setAcceptConnect=0/1：0 停止接入、1 恢复接入（Java 同语义）
                "setacceptconnect" => {
                    hint.command = Some(("setacceptconnect".to_string(), v.to_string()));
                }
                _ => hint.unknown.push(token.to_string()),
            }
        } else {
            match lower.as_str() {
                "master" => hint.master = true,
                "slave" => hint.slave = true,
                "showconfigtext" => hint.show_config_text = true,
                "showslowsql" => hint.show_slow_sql = Some(None),
                "getsqltimefilter" => hint.get_sql_time_filter = true,
                // 无参运维指令（Java mycatDmlHandler 同名指令）
                "repairphysicaltable" => {
                    hint.command = Some(("repairphysicaltable".to_string(), "{}".to_string()));
                }
                "resumeserver" => {
                    hint.command = Some(("resumeserver".to_string(), "{}".to_string()));
                }
                "loadconfigfromfile" => {
                    hint.command = Some(("loadconfigfromfile".to_string(), "{}".to_string()));
                }
                "showsqlcaches" => {
                    hint.command = Some(("showsqlcaches".to_string(), "{}".to_string()));
                }
                _ => {
                    if !lower.starts_with("showtopology{") {
                        hint.unknown.push(token.to_string());
                    }
                }
            }
            // showTopology{表名}（Java HintHandler 的花括号形式）
            if lower.starts_with("showtopology{") {
                let inner = &token[lower.find('{').unwrap() + 1..];
                hint.show_topology = Some(inner.trim_end_matches('}').to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_hint() {
        let (h, sql) = parse_hint("select 1");
        assert!(h.is_none());
        assert_eq!(sql, "select 1");
    }

    #[test]
    fn test_target_hint() {
        let (h, sql) = parse_hint("/*!mycat:target=c0*/ select * from t");
        let h = h.unwrap();
        assert_eq!(h.target.as_deref(), Some("c0"));
        assert_eq!(sql, "select * from t");
    }

    #[test]
    fn test_master_slave_hint() {
        let (h, _) = parse_hint("/*!mycat:master*/ select 1");
        assert!(h.unwrap().master);
        let (h, _) = parse_hint("/*!mycat:slave*/ select 1");
        assert!(h.unwrap().slave);
    }

    #[test]
    fn test_datanode_and_schema() {
        let (h, sql) = parse_hint("/*!mycat:datanode=c1,schema=db2*/ show tables");
        let h = h.unwrap();
        assert_eq!(h.target.as_deref(), Some("c1"));
        assert_eq!(h.schema.as_deref(), Some("db2"));
        assert_eq!(sql, "show tables");
    }

    #[test]
    fn test_inner_sql() {
        let (h, _) = parse_hint("/*!mycat:sql=select * from physical_t where id=1*/");
        let h = h.unwrap();
        assert_eq!(
            h.inner_sql.as_deref(),
            Some("select * from physical_t where id=1")
        );
    }

    #[test]
    fn test_show_config_text_and_slow_sql() {
        let (h, _) = parse_hint("/*!mycat:showConfigText*/");
        assert!(h.unwrap().show_config_text);
        let (h, _) = parse_hint("/*!mycat:showSlowSql*/");
        assert_eq!(h.unwrap().show_slow_sql, Some(None));
        let (h, _) = parse_hint("/*!mycat:showSlowSql{\"trace_id\":42}*/");
        assert_eq!(h.unwrap().show_slow_sql, Some(Some(42)));
        let (h, _) = parse_hint("/*!mycat:getSqlTimeFilter*/");
        assert!(h.unwrap().get_sql_time_filter);
    }

    #[test]
    fn test_set_sql_time_filter() {
        let (h, _) = parse_hint("/*!mycat:setSqlTimeFilter{\"value\":\"500\"}*/");
        assert_eq!(h.unwrap().set_sql_time_filter, Some(500));
        // 与其它指令混用
        let (h, _) = parse_hint("/*!mycat:master,setSqlTimeFilter{\"value\":\"250\"}*/ select 1");
        let h = h.unwrap();
        assert!(h.master);
        assert_eq!(h.set_sql_time_filter, Some(250));
    }

    #[test]
    fn test_loaddata_json_capture() {
        // JSON 内含逗号/空格/引号：整体捕获，不被 token 化破坏
        let (h, sql) = parse_hint(
            r#"/*!mycat:loaddata{"schemaName":"db1","tableName":"orders","fileName":"/tmp/d.csv","delimiter":",","quoteChar":"\"","skipHeaderRecord":true}*/"#,
        );
        let h = h.unwrap();
        let json = h.loaddata.unwrap();
        assert!(json.contains(r#""schemaName":"db1""#), "{}", json);
        assert!(json.contains(r#""skipHeaderRecord":true"#), "{}", json);
        assert!(json.contains(r#""fileName":"/tmp/d.csv""#), "{}", json);
        assert_eq!(sql, "");
    }
}

#[cfg(test)]
mod extra_tests {
    use super::*;

    #[test]
    fn test_unknown_hint_collected() {
        let (h, _) = parse_hint("/*!mycat:createIndex*/");
        let h = h.unwrap();
        assert_eq!(h.unknown, vec!["createIndex".to_string()]);
        assert!(h.is_empty());
        // 混合已知+未知：未知被忽略（Java 兼容），不算空 hint
        let (h, _) = parse_hint("/*!mycat:master,foo*/ select 1");
        let h = h.unwrap();
        assert!(h.master);
        assert!(!h.unknown.is_empty());
        assert!(!h.is_empty());
    }

    #[test]
    fn test_command_hints() {
        let (h, _) =
            parse_hint("/*!mycat:createDataSource{\"name\":\"ds9\",\"url\":\"jdbc:mysql://x\"}*/");
        let h = h.unwrap();
        assert_eq!(
            h.command.as_ref().map(|(k, _)| k.as_str()),
            Some("createdatasource")
        );
        assert!(h.command.unwrap().1.contains("\"name\":\"ds9\""));
        // Java 的 cmd={...} 写法
        let (h, _) = parse_hint("/*!mycat:dropDataSource={\"name\":\"ds9\"}*/");
        let h = h.unwrap();
        assert_eq!(
            h.command.as_ref().map(|(k, _)| k.as_str()),
            Some("dropdatasource")
        );
        // 无参指令
        let (h, _) = parse_hint("/*!mycat:repairPhysicalTable*/");
        assert_eq!(
            h.unwrap().command.as_ref().map(|(k, _)| k.as_str()),
            Some("repairphysicaltable")
        );
        let (h, _) = parse_hint("/*!mycat:setAcceptConnect=1*/");
        assert_eq!(
            h.unwrap().command,
            Some(("setacceptconnect".to_string(), "1".to_string()))
        );
        let (h, _) = parse_hint("/*!mycat:loadConfigFromFile*/");
        assert_eq!(
            h.unwrap().command.as_ref().map(|(k, _)| k.as_str()),
            Some("loadconfigfromfile")
        );
    }
}
