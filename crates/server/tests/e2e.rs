//! 端到端集成测试：
//! 1. 启动两个 mock MySQL 后端（模拟 4 个分片节点分布在两个数据源上）
//! 2. 构建 Mycat3 Pro 前端服务（握手/认证/路由/归并全链路）
//! 3. 用原生 MySQL 客户端连接前端，验证：
//!    - 认证失败路径（错误密码 → ERR 1045）
//!    - 分片键路由（select where id=N → 命中正确物理表）
//!    - 全表扫描 + 多节点行归并（4 节点各 1 行 → 客户端 4 行）
//!    - COUNT(*) 跨分片求和归并
//!    - ORDER BY + LIMIT 跨分片归并排序
//!    - INSERT 按分片键路由 / 多行按节点拆分
//!    - UPDATE 全节点扇出（affectedRows 求和）
//!    - 本地命令：show databases / show tables / use / explain / select @@version / show @@cluster
//!    - 预处理语句：PREPARE + EXECUTE（参数替换 + 二进制结果集）

use std::sync::Arc;
use std::time::Duration;

use mycat3_client::{BackendEndpoint, ClientError, MySQLConnection, QueryOutcome};
use mycat3_config::*;
use mycat3_protocol::consts::capability::DEFAULT_SERVER_CAPABILITIES;
use mycat3_protocol::consts::{field_type, AUTH_PLUGIN_NATIVE};
use mycat3_protocol::frame::{encode_frames, encode_packet, FrameDecoder};
use mycat3_protocol::packets::{
    build_handshake_v10, build_ok_packet, build_text_row, parse_auth_response, ColumnDef,
};
use mycat3_server::bootstrap::start_server_on;
use mycat3_server::state::ServerState;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// mock 后端收到的 SQL 记录
type SqlLog = Arc<Mutex<Vec<String>>>;

/// 测试运行时：16MB 工作线程栈（前端 → 执行 → 后端客户端的深层异步 poll 链，
/// debug 构建下 sqlparser 栈帧较大；生产 runtime 在 main.rs 中设 8MB）
fn big_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
}

/// 极简 mock MySQL 后端（经典 EOF 模式，不用 DEPRECATE_EOF，顺带覆盖前端两种回写路径）：
/// - select count(*) → 单行 "5"
/// - 其他 select → 单行 [tag, 回显SQL]（tag 标识后端与其"分片"）
/// - 其余语句 → OK affected=1
async fn mock_backend(listener: TcpListener, tag: &'static str, log: SqlLog) {
    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(x) => x,
            Err(_) => return,
        };
        let log = log.clone();
        tokio::spawn(async move {
            let seed = [b'm'; 20];
            let greeting = build_handshake_v10(
                "8.0.32-mock",
                1,
                &seed,
                DEFAULT_SERVER_CAPABILITIES,
                45,
                2,
                AUTH_PLUGIN_NATIVE,
            );
            if socket
                .write_all(&encode_packet(0, &greeting))
                .await
                .is_err()
            {
                return;
            }
            let mut buf = vec![0u8; 64 * 1024];
            let mut decoder = FrameDecoder::new();
            let mut authed = false;
            loop {
                let n = match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                let packets = match decoder.feed(&buf[..n]) {
                    Ok(p) => p,
                    Err(_) => break,
                };
                for p in packets {
                    let cmd = *p.payload.first().unwrap_or(&0);
                    if !authed {
                        // 认证包：接受任意密码
                        let _ = parse_auth_response(&p.payload);
                        authed = true;
                        let ok = build_ok_packet(0, 0, 2, "");
                        let mut out = Vec::new();
                        let mut s = p.seq + 1;
                        encode_frames(&mut s, &ok, &mut out);
                        if socket.write_all(&out).await.is_err() {
                            return;
                        }
                        continue;
                    }
                    if cmd == mycat3_protocol::consts::command::COM_QUIT {
                        return;
                    }
                    if cmd != mycat3_protocol::consts::command::COM_QUERY {
                        let ok = build_ok_packet(0, 0, 2, "");
                        let mut out = Vec::new();
                        let mut s = p.seq + 1;
                        encode_frames(&mut s, &ok, &mut out);
                        let _ = socket.write_all(&out).await;
                        continue;
                    }
                    let sql = String::from_utf8_lossy(&p.payload[1..]).to_string();
                    log.lock().push(sql.clone());
                    let lower = sql.to_lowercase();
                    let mut out = Vec::new();
                    let mut s = p.seq + 1;
                    if lower.starts_with("select") {
                        if lower.contains("count(") {
                            // count → 5
                            let cols = vec![ColumnDef::new("count", field_type::LONGLONG)];
                            let rows = vec![build_text_row(&[Some(b"5")])];
                            write_resultset(&mut out, &mut s, &cols, &rows);
                        } else {
                            let cols = vec![
                                ColumnDef::new("src", field_type::VAR_STRING),
                                ColumnDef::new("id", field_type::LONGLONG),
                            ];
                            // id 值取 tag 尾号便于排序断言
                            let id = tag.chars().last().unwrap_or('0').to_string();
                            let rows =
                                vec![build_text_row(&[Some(tag.as_bytes()), Some(id.as_bytes())])];
                            write_resultset(&mut out, &mut s, &cols, &rows);
                        }
                    } else {
                        let ok = build_ok_packet(1, 0, 2, "");
                        encode_frames(&mut s, &ok, &mut out);
                    }
                    if socket.write_all(&out).await.is_err() {
                        return;
                    }
                }
            }
        });
    }
}

fn write_resultset(out: &mut Vec<u8>, seq: &mut u8, cols: &[ColumnDef], rows: &[Vec<u8>]) {
    encode_frames(
        seq,
        &mycat3_protocol::packets::build_column_count(cols.len()),
        out,
    );
    for c in cols {
        encode_frames(seq, &c.encode(), out);
    }
    let eof = mycat3_protocol::packets::build_eof(0, 2);
    encode_frames(seq, &eof, out);
    for r in rows {
        encode_frames(seq, r, out);
    }
    encode_frames(seq, &eof, out);
}

fn test_config(backend_ports: [u16; 2]) -> MycatRouterConfig {
    let mut cfg = MycatRouterConfig::new();
    cfg.server.server.ip = "127.0.0.1".into();
    cfg.server.server.port = 0;
    cfg.server.monitor.open = false;
    for (i, port) in backend_ports.iter().enumerate() {
        cfg.datasources.insert(
            format!("ds{}", i),
            DatasourceConfig {
                name: format!("ds{}", i),
                url: format!("jdbc:mysql://127.0.0.1:{}/db1", port),
                user: "root".into(),
                password: "".into(),
                db_type: "mysql".into(),
                weight: 0,
                max_con: 16,
                min_con: 1,
                max_retry_count: 3,
                max_connect_timeout: 5000,
                init_sqls: vec![],
                instance_type: InstanceType::ReadWrite,
                idle_timeout: 60_000,
                r#type: "NATIVE".into(),
            },
        );
    }
    // 集群：主 ds0，从 ds1（读写分离 + 心跳配置）
    cfg.clusters.insert(
        "c0".into(),
        ClusterConfig {
            name: "c0".into(),
            cluster_type: ReplicaType::MasterSlave,
            switch_type: ReplicaSwitchType::NotSwitch,
            read_balance_type: BalanceType::None,
            read_balance_name: None,
            write_balance_name: None,
            masters: vec!["ds0".into()],
            replicas: vec!["ds1".into()],
            heartbeat: HeartbeatConfig {
                min_switch_time_interval: 300,
                ..Default::default()
            },
            max_con: 16,
        },
    );
    // orders：4 节点分片（ds0: orders_0/orders_1, ds1: orders_2/orders_3），id % 4
    let schema_json = r#"{
      "schemaName": "db1",
      "targetName": "ds0",
      "shardingTables": {
        "orders": {
          "partition": {
            "data": [
              ["ds0", "db1", "orders_0"],
              ["ds0", "db1", "orders_1"],
              ["ds1", "db1", "orders_2"],
              ["ds1", "db1", "orders_3"]
            ]
          },
          "function": {
            "clazz": "io.mycat.router.mycat1xfunction.PartitionByMod",
            "properties": {"columnName": "id", "count": "4"}
          }
        }
      },
      "normalTables": {
        "config_tbl": {"locality": {"targetName": "ds0", "schemaName": "db1", "tableName": "config_tbl"}}
      },
      "globalTables": {
        "dict": {"broadcast": [{"targetName": "ds0"}, {"targetName": "ds1"}]}
      }
    }"#;
    let schema: LogicSchemaConfig = serde_json::from_str(schema_json).unwrap();
    cfg.schemas.insert("db1".into(), schema);
    cfg.users.insert(
        "root".into(),
        UserConfig {
            username: "root".into(),
            password: "123456".into(),
            ip: None,
            transaction_type: "proxy".into(),
            dialect: "mysql".into(),
            schema: Some("db1".into()),
            schemas: vec!["db1".into()],
            isolation: 3,
            read_only: false,
            login_limit: -1,
            role: None,
            encrypt_type: None,
            encode_key: None,
            decrypted: std::sync::Arc::new(parking_lot::Mutex::new(None)),
        },
    );
    cfg
}

struct TestEnv {
    frontend_port: u16,
    logs: [SqlLog; 2],
    _state: Arc<ServerState>,
}

async fn setup() -> TestEnv {
    setup_with(test_config).await
}

/// 自定义配置的测试环境（`build` 在端口确定后调用）。
async fn setup_with(build: impl FnOnce([u16; 2]) -> MycatRouterConfig) -> TestEnv {
    // 两个 mock 后端
    let l0 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p0 = l0.local_addr().unwrap().port();
    let p1 = l1.local_addr().unwrap().port();
    let log0: SqlLog = Arc::new(Mutex::new(Vec::new()));
    let log1: SqlLog = Arc::new(Mutex::new(Vec::new()));
    tokio::spawn(mock_backend(l0, "ds0", log0.clone()));
    tokio::spawn(mock_backend(l1, "ds1", log1.clone()));

    let cfg = build([p0, p1]);
    let state = ServerState::new(cfg).unwrap();
    // mock 后端不支持 XA：关闭启动恢复（避免恢复探测污染 mock 日志断言）
    state
        .xa_recovery
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let frontend_port = listener.local_addr().unwrap().port();
    let st = state.clone();
    tokio::spawn(async move {
        let _ = start_server_on(st, listener).await;
    });
    // 等待就绪
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", frontend_port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // 启动期 XA 恢复会对后端发 XA RECOVER 探测：清掉 mock 记录，避免干扰断言
    log0.lock().clear();
    log1.lock().clear();
    TestEnv {
        frontend_port,
        logs: [log0, log1],
        _state: state,
    }
}

fn frontend_endpoint(port: u16, password: &str) -> BackendEndpoint {
    BackendEndpoint {
        host: "127.0.0.1".into(),
        port,
        user: "root".into(),
        password: password.into(),
        database: "db1".into(),
        connect_timeout: Duration::from_secs(5),
        query_timeout: None,
    }
}

async fn connect_frontend(env: &TestEnv) -> MySQLConnection {
    MySQLConnection::connect(frontend_endpoint(env.frontend_port, "123456"))
        .await
        .unwrap()
}

fn rows_str(out: &QueryOutcome) -> Vec<Vec<String>> {
    match out {
        QueryOutcome::Rows(rs) => rs
            .decode_rows()
            .iter()
            .map(|r| {
                r.iter()
                    .map(|c| {
                        c.as_ref()
                            .map(|b| String::from_utf8_lossy(b).to_string())
                            .unwrap_or_else(|| "NULL".into())
                    })
                    .collect()
            })
            .collect(),
        QueryOutcome::Ok(_) => vec![],
    }
}

#[test]
fn test_e2e_wrong_password() {
    big_rt().block_on(test_e2e_wrong_password_inner())
}

async fn test_e2e_wrong_password_inner() {
    let env = setup().await;
    let res = MySQLConnection::connect(frontend_endpoint(env.frontend_port, "wrongpass")).await;
    match res {
        Err(mycat3_client::ClientError::Server { errno, message, .. }) => {
            assert_eq!(errno, 1045);
            assert!(message.contains("Access denied"), "{}", message);
        }
        other => panic!("expected access denied, got {:?}", other.is_ok()),
    }
}

#[test]
fn test_e2e_sharding_route_and_merge() {
    big_rt().block_on(test_e2e_sharding_route_and_merge_inner())
}

async fn test_e2e_sharding_route_and_merge_inner() {
    let env = setup().await;
    let mut conn = connect_frontend(&env).await;

    // 0. 握手后服务端版本来自配置
    assert!(conn.server_version.contains("mycat"));

    // 1. 分片键路由：id=3 → ds1.orders_3
    let out = conn
        .query("select * from orders where id = 3")
        .await
        .unwrap();
    let rows = rows_str(&out);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "ds1");
    {
        let l1 = env.logs[1].lock();
        assert!(l1.iter().any(|s| s.contains("db1.orders_3")), "{:?}", *l1);
        assert!(env.logs[0].lock().is_empty());
    }

    // 2. 全表扫描：4 节点各回 1 行 → 归并 4 行
    let out = conn
        .query("select * from orders where name = 'x'")
        .await
        .unwrap();
    let rows = rows_str(&out);
    assert_eq!(rows.len(), 4);
    let srcs: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(srcs.iter().filter(|s| **s == "ds0").count(), 2);
    assert_eq!(srcs.iter().filter(|s| **s == "ds1").count(), 2);

    // 3. COUNT(*) 归并：4 节点各 5 → 20
    let out = conn.query("select count(*) from orders").await.unwrap();
    let rows = rows_str(&out);
    assert_eq!(rows[0][0], "20");

    // 4. ORDER BY id DESC + LIMIT 2 归并：
    //    ds0 节点行 id=0，ds1 节点行 id=1 → desc 排序 → [1,1]（各 ds1 两行）
    let out = conn
        .query("select * from orders order by id desc limit 2")
        .await
        .unwrap();
    let rows = rows_str(&out);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][1], "1");
    assert_eq!(rows[1][1], "1");

    // 5. ORDER BY id ASC LIMIT 2 → [0,0]
    let out = conn
        .query("select * from orders order by id limit 2")
        .await
        .unwrap();
    let rows = rows_str(&out);
    assert_eq!(rows[0][1], "0");
    assert_eq!(rows[1][1], "0");

    // 6. IN 路由：id in (1, 2) → orders_1(ds0) + orders_2(ds1)
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    let out = conn
        .query("select * from orders where id in (1, 2)")
        .await
        .unwrap();
    assert_eq!(rows_str(&out).len(), 2);
    assert!(env.logs[0].lock().iter().any(|s| s.contains("orders_1")));
    assert!(env.logs[1].lock().iter().any(|s| s.contains("orders_2")));

    // 7. 单表（NORMAL）路由
    env.logs[0].lock().clear();
    let out = conn.query("select * from config_tbl").await.unwrap();
    assert_eq!(rows_str(&out).len(), 1);
    assert!(env.logs[0]
        .lock()
        .iter()
        .any(|s| s.contains("db1.config_tbl")));

    // 8. 广播表读 → 单 target
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    let out = conn.query("select * from dict").await.unwrap();
    assert_eq!(rows_str(&out).len(), 1);
    assert_eq!(env.logs[1].lock().len(), 0);

    conn.quit().await;
}

#[test]
fn test_e2e_dml_and_ddl() {
    big_rt().block_on(test_e2e_dml_and_ddl_inner())
}

async fn test_e2e_dml_and_ddl_inner() {
    let env = setup().await;
    let mut conn = connect_frontend(&env).await;

    // INSERT 单行 → 命中分片（5%4=1 → ds0.orders_1），affected=1
    env.logs[0].lock().clear();
    match conn
        .query("insert into orders (id, name) values (5, 'a')")
        .await
        .unwrap()
    {
        QueryOutcome::Ok(ok) => assert_eq!(ok.affected_rows, 1),
        _ => panic!("expected OK"),
    }
    assert!(env.logs[0]
        .lock()
        .iter()
        .any(|s| s.contains("orders_1") && s.contains("'a'")));

    // INSERT 多行拆分：id 4→ds0.orders_0, id 7→ds1.orders_3
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    match conn
        .query("insert into orders (id, name) values (4, 'x'), (7, 'y')")
        .await
        .unwrap()
    {
        QueryOutcome::Ok(ok) => assert_eq!(ok.affected_rows, 2), // 1+1 求和
        _ => panic!(),
    }
    assert!(env.logs[0]
        .lock()
        .iter()
        .any(|s| s.contains("orders_0") && s.contains("'x'")));
    assert!(env.logs[1]
        .lock()
        .iter()
        .any(|s| s.contains("orders_3") && s.contains("'y'")));

    // UPDATE 无分片键 → 4 节点扇出，affected 求和 = 4
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    match conn
        .query("update orders set name = 'z' where amount > 1")
        .await
        .unwrap()
    {
        QueryOutcome::Ok(ok) => assert_eq!(ok.affected_rows, 4),
        _ => panic!(),
    }
    assert_eq!(env.logs[0].lock().len(), 2);
    assert_eq!(env.logs[1].lock().len(), 2);

    // 广播表 INSERT → 两个 target 各一份，affected=2
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    match conn
        .query("insert into dict (id) values (1)")
        .await
        .unwrap()
    {
        QueryOutcome::Ok(ok) => assert_eq!(ok.affected_rows, 2),
        _ => panic!(),
    }
    assert_eq!(env.logs[0].lock().len(), 1);
    assert_eq!(env.logs[1].lock().len(), 1);

    // CREATE TABLE：配置自动化流程（对应 Java CreateTableSQLHandler，普通表 →
    // locality=库默认 target 并尝试物理建表；test_config 无 c0 集群数据源，
    // 物理建表失败按 Java 语义 log-and-continue，配置仍生效）
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    conn.query("create table newtbl (id bigint primary key)")
        .await
        .unwrap();
    // 元数据立即生效：show tables 出现 newtbl
    let out = conn.query("show tables").await.unwrap();
    let found = match out {
        mycat3_client::QueryOutcome::Rows(rs) => rs
            .decode_rows()
            .iter()
            .any(|r| r[0].as_deref() == Some(&b"newtbl"[..])),
        _ => false,
    };
    assert!(found, "newtbl should appear in show tables after DDL");

    conn.quit().await;
}

#[test]
fn test_e2e_local_commands() {
    big_rt().block_on(test_e2e_local_commands_inner())
}

async fn test_e2e_local_commands_inner() {
    let env = setup().await;
    let mut conn = connect_frontend(&env).await;

    // show databases（本地）
    let rows = rows_str(&conn.query("show databases").await.unwrap());
    assert!(rows.iter().any(|r| r[0] == "db1"));

    // show tables（本地）
    let rows = rows_str(&conn.query("show tables").await.unwrap());
    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(names.contains(&"orders"));
    assert!(names.contains(&"dict"));

    // select @@version（本地探针）
    let rows = rows_str(&conn.query("select @@version").await.unwrap());
    assert!(rows[0][0].contains("mycat"));

    // select database()
    let rows = rows_str(&conn.query("select database()").await.unwrap());
    assert_eq!(rows[0][0], "db1");

    // explain（本地）：id=6 → 6%4=2 → data 顺序 [ds0,ds0,ds1,ds1] → ds1.orders_2
    let rows = rows_str(
        &conn
            .query("explain select * from orders where id=6")
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "ds1");
    assert!(rows[0][1].contains("orders_2"), "{}", rows[0][1]);
    conn.quit().await;
}

#[test]
fn test_e2e_admin_and_tx() {
    big_rt().block_on(test_e2e_admin_and_tx_inner())
}

async fn test_e2e_admin_and_tx_inner() {
    let env = setup().await;
    let mut conn = connect_frontend(&env).await;

    // Pro 管理命令
    let rows = rows_str(&conn.query("show @@cluster").await.unwrap());
    assert!(!rows.is_empty());
    let rows = rows_str(&conn.query("show @@version").await.unwrap());
    assert!(rows[0][0].contains("mycat"));

    // 事务：BEGIN → 写（固定连接）→ COMMIT
    conn.query("begin").await.unwrap();
    env.logs[0].lock().clear();
    conn.query("insert into orders (id, name) values (9, 'tx')")
        .await
        .unwrap(); // 9%4=1 → ds0
    conn.query("commit").await.unwrap();
    assert!(env.logs[0].lock().iter().any(|s| s.contains("BEGIN")));
    assert!(env.logs[0].lock().iter().any(|s| s.contains("COMMIT")));

    // SET autocommit=0 → 写 → SET autocommit=1 必须提交固定连接，
    // 且不能把 MySQL 的 SET autocommit 发给后端。
    conn.query("set autocommit=0").await.unwrap();
    env.logs[0].lock().clear();
    env.logs[1].lock().clear();
    conn.query("insert into orders (id, name) values (10, 'implicit')")
        .await
        .unwrap(); // 10%4=2 → ds1
    conn.query("set autocommit=1").await.unwrap();
    {
        let logs = env.logs[1].lock();
        assert!(
            logs.iter().any(|s| s.eq_ignore_ascii_case("COMMIT")),
            "SET autocommit=1 必须提交隐式事务: {:?}",
            *logs
        );
        assert!(
            !logs.iter().any(|s| s.to_lowercase().contains("autocommit")),
            "不应把 MySQL SET autocommit 透传给后端: {:?}",
            *logs
        );
    }

    // SET / USE 本地
    match conn.query("use db1").await {
        Ok(_) => {}
        Err(e) => panic!("use failed: {}", e),
    }

    // PING
    conn.ping().await.unwrap();

    conn.quit().await;
}

#[test]
fn test_e2e_prepared_statement() {
    big_rt().block_on(test_e2e_prepared_statement_inner())
}

async fn test_e2e_prepared_statement_inner() {
    let env = setup().await;
    let mut conn = connect_frontend(&env).await;

    // 手工发送 COM_STMT_PREPARE / EXECUTE（client crate 未封装，直接用底层协议）
    // PREPARE
    let prepare_payload =
        mycat3_protocol::packets::build_com_stmt_prepare("select * from orders where id = ?");
    let resp = raw_command(&mut conn, &prepare_payload).await;
    assert_eq!(resp[0][0], 0x00, "prepare OK header");
    let stmt_id = u32::from_le_bytes([resp[0][1], resp[0][2], resp[0][3], resp[0][4]]);
    assert_eq!(stmt_id, 1);
    let num_params = u16::from_le_bytes([resp[0][7], resp[0][8]]);
    assert_eq!(num_params, 1);

    // EXECUTE：id = 7 → ds1.orders_3
    let mut w = mycat3_protocol::PacketWriter::new();
    w.write_u8(mycat3_protocol::consts::command::COM_STMT_EXECUTE);
    w.write_int4(stmt_id);
    w.write_u8(0); // flags
    w.write_int4(1); // iteration
    w.write_u8(0); // null bitmap
    w.write_u8(1); // new params bound
    w.write_int2(field_type::LONGLONG as u16);
    w.write_int8(7);
    let resp = raw_command(&mut conn, &w.into_bytes()).await;
    // 期望二进制结果集：第一包为列数 2
    assert!(!resp.is_empty());
    assert_eq!(resp[0][0], 2, "column count = 2");
    // 后端应收到替换后的 SQL
    assert!(
        env.logs[1]
            .lock()
            .iter()
            .any(|s| s.contains("orders_3") && s.contains("7")),
        "{:?}",
        *env.logs[1].lock()
    );

    // CLOSE（无响应）
    let mut w = mycat3_protocol::PacketWriter::new();
    w.write_u8(mycat3_protocol::consts::command::COM_STMT_CLOSE);
    w.write_int4(stmt_id);
    conn_execute_no_response(&mut conn, &w.into_bytes()).await;

    conn.quit().await;
}

/// 代理模式下的二进制协议预编译：单目标透传、SQL 不改写。
#[test]
fn test_proxy_mode_prepared_statement() {
    big_rt().block_on(async {
        let env = setup_with(proxy_test_config).await;
        let mut conn = connect_frontend(&env).await;
        let prepare_payload =
            mycat3_protocol::packets::build_com_stmt_prepare("select * from orders where id = ?");
        let resp = raw_command(&mut conn, &prepare_payload).await;
        assert_eq!(resp[0][0], 0x00, "prepare OK header");
        let stmt_id = u32::from_le_bytes([resp[0][1], resp[0][2], resp[0][3], resp[0][4]]);
        let mut w = mycat3_protocol::PacketWriter::new();
        w.write_u8(mycat3_protocol::consts::command::COM_STMT_EXECUTE);
        w.write_int4(stmt_id);
        w.write_u8(0);
        w.write_int4(1);
        w.write_u8(0);
        w.write_u8(1);
        w.write_int2(field_type::LONGLONG as u16);
        w.write_int8(3);
        let resp = raw_command(&mut conn, &w.into_bytes()).await;
        assert!(!resp.is_empty());
        assert_eq!(resp[0][0], 2, "column count = 2");
        // 代理模式：SQL 原样到达 ds0（分片模式会改写为 orders_3 并发往 ds1）
        {
            let l0 = env.logs[0].lock();
            assert!(
                l0.iter().any(|s| s == "select * from orders where id = 3"),
                "预处理 SQL 必须原样透传: {:?}",
                *l0
            );
            assert!(!l0.iter().any(|s| s.contains("orders_3")), "{:?}", *l0);
        }
        assert!(env.logs[1].lock().is_empty());
        let mut w = mycat3_protocol::PacketWriter::new();
        w.write_u8(mycat3_protocol::consts::command::COM_STMT_CLOSE);
        w.write_int4(stmt_id);
        conn_execute_no_response(&mut conn, &w.into_bytes()).await;
        conn.quit().await;
    });
}

/// 通过已认证连接发送原始命令载荷并收集响应包。
async fn raw_command(conn: &mut MySQLConnection, payload: &[u8]) -> Vec<Vec<u8>> {
    conn.raw_command(payload).await.unwrap()
}

async fn conn_execute_no_response(conn: &mut MySQLConnection, payload: &[u8]) {
    let _ = conn.raw_command_silent(payload).await;
}

/// 纯代理模式：SQL 原样透传、拦截（缓存前）、本地管理命令、SQL 缓存命中不触达后端。
fn proxy_test_config(ports: [u16; 2]) -> MycatRouterConfig {
    let mut cfg = test_config(ports);
    cfg.server.mode = "proxy".into();
    cfg.server.proxy.target = Some("ds0".into());
    cfg.server.proxy.intercepts = vec![InterceptRule {
        pattern: "forbidden_table".into(),
        stmt_type: "ALL".into(),
        message: Some("denied by test policy".into()),
        errno: Some(1142),
        sqlstate: None,
    }];
    cfg.sql_caches.insert(
        "cnt_cache".into(),
        SqlCacheConfig {
            name: "cnt_cache".into(),
            sql: "select count(*) from orders".into(),
            refresh_interval: 3600,
            initial_delay: 3600,
            match_mode: "PARAMETERIZED".into(),
            max_rows: 1000,
            max_bytes: 0,
            schema_name: None,
        },
    );
    cfg
}

#[test]
fn test_proxy_mode_passthrough_intercept_and_cache() {
    big_rt().block_on(async {
        let env = setup_with(proxy_test_config).await;
        let mut conn = connect_frontend(&env).await;
        {
            let l1 = env.logs[1].lock();
            assert!(l1.is_empty(), "代理模式不应访问 ds1: {:?}", *l1);
        }

        // 1) 透传：SQL 原样到达 ds0（分片模式会重写为 db1.orders_3 并发往 ds1）
        env.logs[0].lock().clear();
        let out = conn
            .query("select * from orders where id = 3")
            .await
            .unwrap();
        assert!(!rows_str(&out).is_empty());
        {
            let logs = env.logs[0].lock();
            assert!(
                logs.iter()
                    .any(|s| s == "select * from orders where id = 3"),
                "SQL 必须原样透传: {:?}",
                *logs
            );
            assert!(
                !logs.iter().any(|s| s.contains("orders_3")),
                "代理模式不得改写表名: {:?}",
                *logs
            );
        }

        // 2) 拦截：命中规则返回权限错误，且不触达后端
        env.logs[0].lock().clear();
        let err = conn
            .query("select * from forbidden_table")
            .await
            .unwrap_err();
        match err {
            ClientError::Server { errno, message, .. } => {
                assert_eq!(errno, 1142, "{}", message);
                assert!(message.contains("denied by test policy"), "{}", message);
            }
            other => panic!("expected server error, got {:?}", other),
        }
        assert!(
            env.logs[0].lock().is_empty(),
            "被拦截语句不得到达后端: {:?}",
            *env.logs[0].lock()
        );

        // 3) 本地管理命令仍可用
        let out = conn.query("show @@version").await.unwrap();
        assert!(!rows_str(&out).is_empty());

        // 4) 事务命令：会话固定连接上原样下推（后端事务语义）
        env.logs[0].lock().clear();
        conn.query("begin").await.unwrap();
        conn.query("rollback").await.unwrap();
        {
            let logs = env.logs[0].lock();
            assert!(
                logs.iter().any(|s| s.eq_ignore_ascii_case("begin")),
                "{:?}",
                *logs
            );
            assert!(
                logs.iter().any(|s| s.eq_ignore_ascii_case("rollback")),
                "{:?}",
                *logs
            );
        }

        // 5) SQL 缓存：首次执行后，重复查询不再触达后端
        env.logs[0].lock().clear();
        let first = conn.query("select count(*) from orders").await.unwrap();
        assert!(!rows_str(&first).is_empty());
        let n = env.logs[0].lock().len();
        assert!(n >= 1, "首次查询应触达后端");
        for _ in 0..3 {
            let _ = conn.query("select count(*) from orders").await.unwrap();
        }
        assert_eq!(
            env.logs[0].lock().len(),
            n,
            "缓存命中不应触达后端: {:?}",
            *env.logs[0].lock()
        );

        // 6) 拦截优先于缓存：热重载加入匹配该缓存 SQL 的规则后，即使缓存有值也必须拒绝
        let mut cfg2 = (*env._state.config()).clone();
        cfg2.server.proxy.intercepts.push(InterceptRule {
            pattern: r"count\(\*\) from orders".into(),
            stmt_type: "SELECT".into(),
            message: Some("cache-bypass blocked".into()),
            errno: Some(1142),
            sqlstate: None,
        });
        env._state.reload(cfg2, None).unwrap();
        let err = conn.query("select count(*) from orders").await.unwrap_err();
        match err {
            ClientError::Server { errno, message, .. } => {
                assert_eq!(errno, 1142, "{}", message);
                assert!(message.contains("cache-bypass blocked"), "{}", message);
            }
            other => panic!("expected server error, got {:?}", other),
        }
    });
}
