//! DDL 配置自动化端到端测试：
//! CREATE DATABASE → CREATE TABLE（DRDS 分片语法）→ 物理建表 → INSERT 序列填充 →
//! 分片路由 → BROADCAST 表 → DROP TABLE → CREATE USER → PROCESSLIST/KILL →
//! information_schema → loadConfigFromFile 热重载。
//! mock 后端记录收到的全部 SQL（心跳 SQL 过滤后断言）。

use std::sync::Arc;
use std::time::Duration;

use mycat3_client::{BackendEndpoint, MySQLConnection, QueryOutcome};
use mycat3_config::*;
use mycat3_protocol::consts::capability::DEFAULT_SERVER_CAPABILITIES;
use mycat3_protocol::consts::AUTH_PLUGIN_NATIVE;
use mycat3_protocol::frame::{encode_frames, encode_packet, FrameDecoder};
use mycat3_protocol::packets::{build_handshake_v10, build_ok_packet, parse_auth_response};
use mycat3_server::bootstrap::start_server_on;
use mycat3_server::state::ServerState;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

type SqlLog = Arc<Mutex<Vec<String>>>;

/// mock 后端：接受任意认证，所有命令回 OK(affected=0)，记录 SQL。
async fn mock_backend(listener: TcpListener, log: SqlLog) {
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
                        let _ = parse_auth_response(&p.payload);
                        authed = true;
                    } else if cmd == mycat3_protocol::consts::command::COM_QUERY {
                        let sql = String::from_utf8_lossy(&p.payload[1..]).to_string();
                        log.lock().push(sql);
                    } else if cmd == mycat3_protocol::consts::command::COM_QUIT {
                        return;
                    }
                    let ok = build_ok_packet(0, 0, 2, "");
                    let mut out = Vec::new();
                    let mut s = p.seq + 1;
                    encode_frames(&mut s, &ok, &mut out);
                    if socket.write_all(&out).await.is_err() {
                        return;
                    }
                }
            }
        });
    }
}

struct Env {
    port: u16,
    log: SqlLog,
    home: tempfile::TempDir,
    state: Arc<ServerState>,
}

async fn setup() -> Env {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bport = listener.local_addr().unwrap().port();
    let log: SqlLog = Arc::new(Mutex::new(Vec::new()));
    tokio::spawn(mock_backend(listener, log.clone()));

    let home = tempfile::tempdir().unwrap();
    let mut cfg = MycatRouterConfig::new();
    cfg.server.server.ip = "127.0.0.1".into();
    cfg.server.server.port = 0;
    cfg.server.monitor.open = false;
    for (name, i) in [("ds0", 0), ("ds1", 1)] {
        let _ = i;
        cfg.datasources.insert(
            name.into(),
            DatasourceConfig {
                name: name.into(),
                url: format!("jdbc:mysql://127.0.0.1:{}/mysql", bport),
                user: "root".into(),
                password: "".into(),
                db_type: "mysql".into(),
                weight: 0,
                max_con: 16,
                min_con: 1,
                max_retry_count: 3,
                max_connect_timeout: 3000,
                init_sqls: vec![],
                instance_type: InstanceType::ReadWrite,
                idle_timeout: 60_000,
                r#type: "NATIVE".into(),
            },
        );
    }
    // 两个 c* 集群（storeNum=2）
    for (cname, ds) in [("c0", "ds0"), ("c1", "ds1")] {
        cfg.clusters.insert(
            cname.into(),
            ClusterConfig {
                name: cname.into(),
                cluster_type: ReplicaType::MasterSlave,
                switch_type: ReplicaSwitchType::NotSwitch,
                read_balance_type: BalanceType::None,
                read_balance_name: None,
                write_balance_name: None,
                masters: vec![ds.into()],
                replicas: vec![],
                heartbeat: HeartbeatConfig {
                    min_switch_time_interval: 300,
                    ..Default::default()
                },
                max_con: 16,
            },
        );
    }
    cfg.clusters.insert(
        "prototype".into(),
        ClusterConfig {
            name: "prototype".into(),
            cluster_type: ReplicaType::MasterSlave,
            switch_type: ReplicaSwitchType::NotSwitch,
            read_balance_type: BalanceType::None,
            read_balance_name: None,
            write_balance_name: None,
            masters: vec!["ds0".into()],
            replicas: vec![],
            heartbeat: HeartbeatConfig::default(),
            max_con: 16,
        },
    );
    cfg.users.insert(
        "root".into(),
        UserConfig {
            username: "root".into(),
            password: "".into(),
            ip: None,
            transaction_type: "proxy".into(),
            dialect: "mysql".into(),
            schema: None,
            schemas: vec![],
            isolation: 3,
            read_only: false,
            login_limit: -1,
            role: None,
            encrypt_type: None,
            encode_key: None,
            decrypted: std::sync::Arc::new(parking_lot::Mutex::new(None)),
        },
    );
    let state = ServerState::with_home(cfg.clone(), Some(home.path().to_path_buf())).unwrap();
    // 初始配置落盘（reload 测试需要文件）
    FileConfigStorage::new(home.path()).save_all(&cfg).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let st = state.clone();
    tokio::spawn(async move {
        let _ = start_server_on(st, listener).await;
    });
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Env {
        port,
        log,
        home,
        state,
    }
}

async fn connect(env: &Env, user: &str, password: &str, db: &str) -> MySQLConnection {
    MySQLConnection::connect(BackendEndpoint {
        host: "127.0.0.1".into(),
        port: env.port,
        user: user.into(),
        password: password.into(),
        database: db.into(),
        connect_timeout: Duration::from_secs(5),
        query_timeout: None,
    })
    .await
    .unwrap()
}

/// 过滤心跳/初始化 SQL，只看业务 SQL。
fn business_sql(log: &SqlLog) -> Vec<String> {
    log.lock()
        .iter()
        .filter(|s| {
            let l = s.to_lowercase();
            !l.starts_with("select 1")
                && !l.starts_with("show slave status")
                && !l.starts_with("select @@read_only")
        })
        .cloned()
        .collect()
}

fn big_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
}

/// 在大栈线程中运行（block_on 所在线程同样承载深层异步 poll 链）
fn run_big<F: FnOnce() + Send + 'static>(f: F) {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn test_ddl_automation_e2e() {
    run_big(|| big_rt().block_on(ddl_automation()))
}

async fn ddl_automation() {
    let env = setup().await;
    let mut conn = connect(&env, "root", "", "").await;

    // ---- CREATE DATABASE ----
    conn.query("create database dbx").await.unwrap();
    assert!(
        env.home.path().join("schemas/dbx.schema.json").exists(),
        "schema file not written"
    );
    let rows = match conn.query("show databases").await.unwrap() {
        QueryOutcome::Rows(rs) => rs.decode_rows(),
        _ => panic!(),
    };
    assert!(rows.iter().any(|r| r[0].as_deref() == Some(&b"dbx"[..])));

    // ---- CREATE TABLE 分片（DRDS 语法，hash 相同 → flatten ${index}）----
    env.log.lock().clear();
    conn.query("use dbx").await.unwrap();
    conn.query("CREATE TABLE t (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(32)) dbpartition by hash(id) dbpartitions 2 tbpartition by hash(id) tbpartitions 2")
        .await
        .unwrap();
    // 配置落盘
    let schema_json =
        std::fs::read_to_string(env.home.path().join("schemas/dbx.schema.json")).unwrap();
    assert!(schema_json.contains("\"t\""), "{}", schema_json);
    assert!(schema_json.contains("mappingFormat"));
    // 物理 DDL：CREATE DATABASE dbx_0 / dbx_1 + 4 张物理表
    let sqls = business_sql(&env.log);
    assert!(
        sqls.iter()
            .any(|s| s.contains("CREATE DATABASE IF NOT EXISTS `dbx_0`")),
        "{:?}",
        sqls
    );
    assert!(
        sqls.iter()
            .any(|s| s.contains("CREATE DATABASE IF NOT EXISTS `dbx_1`")),
        "{:?}",
        sqls
    );
    let creates: Vec<&String> = sqls
        .iter()
        .filter(|s| s.contains("CREATE TABLE IF NOT EXISTS"))
        .collect();
    assert_eq!(creates.len(), 4, "{:?}", creates);
    assert!(
        creates.iter().any(|s| s.contains("`dbx_0`.`t_0`")),
        "{:?}",
        creates
    );
    assert!(
        creates.iter().any(|s| s.contains("`dbx_1`.`t_3`")),
        "{:?}",
        creates
    );
    // 物理 DDL 已去除 dbpartition 子句
    assert!(!creates
        .iter()
        .any(|s| s.to_lowercase().contains("dbpartition")));

    // ---- INSERT 自增序列填充 + 分片路由 ----
    env.log.lock().clear();
    conn.query("INSERT INTO t (name) VALUES ('alice')")
        .await
        .unwrap();
    let sqls = business_sql(&env.log);
    assert_eq!(sqls.len(), 1, "{:?}", sqls);
    let ins = &sqls[0];
    // 填充了 id 列与 snowflake 值（大整数），并路由到单个物理表
    assert!(
        ins.contains("(`name`, `id`)")
            || ins.contains("(name, id)")
            || ins.to_lowercase().contains("id"),
        "{}",
        ins
    );
    assert!(ins.to_lowercase().contains("values"), "{}", ins);
    // 提取填充的 id 值验证路由正确性
    let id_val = extract_big_int_after_values(ins);
    let expect_node = (id_val % 4).rem_euclid(4); // flatten: index = |hash| % 4
    assert!(
        ins.contains(&format!("t_{}", expect_node)),
        "id={} expect t_{} got {}",
        id_val,
        expect_node,
        ins
    );

    // 显式 NULL 也触发填充
    env.log.lock().clear();
    conn.query("INSERT INTO t (id, name) VALUES (NULL, 'bob')")
        .await
        .unwrap();
    let sqls = business_sql(&env.log);
    let id2 = extract_big_int_after_values(&sqls[0]);
    assert!(id2 > 0, "NULL not filled: {}", sqls[0]);

    // 显式值路由（id=3 → t_3 → c1/ds1）
    env.log.lock().clear();
    conn.query("INSERT INTO t (id, name) VALUES (3, 'x')")
        .await
        .unwrap();
    assert!(business_sql(&env.log)[0].contains("t_3"));

    // ---- BROADCAST 表 ----
    env.log.lock().clear();
    conn.query("CREATE TABLE g (id INT) BROADCAST")
        .await
        .unwrap();
    let schema_json =
        std::fs::read_to_string(env.home.path().join("schemas/dbx.schema.json")).unwrap();
    assert!(schema_json.contains("globalTables"));
    env.log.lock().clear();
    conn.query("INSERT INTO g VALUES (1)").await.unwrap();
    // 广播写扇出 c0+c1
    assert_eq!(
        business_sql(&env.log).len(),
        2,
        "{:?}",
        business_sql(&env.log)
    );

    // ---- information_schema ----
    match conn
        .query("select table_name from information_schema.tables where table_schema='dbx'")
        .await
        .unwrap()
    {
        QueryOutcome::Rows(rs) => {
            let rows = rs.decode_rows();
            let names: Vec<String> = rows
                .iter()
                .map(|r| String::from_utf8_lossy(r[0].as_ref().unwrap()).to_string())
                .collect();
            assert!(
                names.contains(&"t".to_string()) && names.contains(&"g".to_string()),
                "{:?}",
                names
            );
        }
        _ => panic!(),
    }
    match conn.query("select column_name, column_key from information_schema.columns where table_schema='dbx' and table_name='t'").await.unwrap() {
        QueryOutcome::Rows(rs) => {
            let rows = rs.decode_rows();
            assert!(rows.iter().any(|r| {
                String::from_utf8_lossy(r[0].as_ref().unwrap()) == "id"
                    && String::from_utf8_lossy(r[1].as_ref().unwrap()) == "PRI"
            }), "{:?}", rows);
        }
        _ => panic!(),
    }

    // ---- SHOW CREATE TABLE（返回逻辑 DDL，含 dbpartition 子句）----
    match conn.query("show create table t").await.unwrap() {
        QueryOutcome::Rows(rs) => {
            let rows = rs.decode_rows();
            let ddl = String::from_utf8_lossy(rows[0][1].as_ref().unwrap()).to_string();
            assert!(ddl.to_lowercase().contains("dbpartition by"), "{}", ddl);
        }
        _ => panic!(),
    }

    // ---- CREATE USER + 新用户登录 ----
    conn.query("CREATE USER bob IDENTIFIED BY 'secret'")
        .await
        .unwrap();
    assert!(env.home.path().join("users/bob.user.json").exists());
    let mut bob = connect(&env, "bob", "secret", "").await;
    assert!(!bob.server_version.is_empty());
    // 错误密码拒绝
    let bad = MySQLConnection::connect(BackendEndpoint {
        host: "127.0.0.1".into(),
        port: env.port,
        user: "bob".into(),
        password: "wrong".into(),
        database: "".into(),
        connect_timeout: Duration::from_secs(3),
        query_timeout: None,
    })
    .await;
    assert!(bad.is_err());

    // ---- SHOW PROCESSLIST / KILL ----
    match conn.query("show full processlist").await.unwrap() {
        QueryOutcome::Rows(rs) => {
            let rows = rs.decode_rows();
            assert!(rows.len() >= 2, "{:?}", rows.len()); // root + bob
        }
        _ => panic!("processlist should be local rows"),
    }
    // kill bob 的连接
    let bob_id = match conn.query("show full processlist").await.unwrap() {
        QueryOutcome::Rows(rs) => {
            let rows = rs.decode_rows();
            rows.iter()
                .find(|r| String::from_utf8_lossy(r[1].as_ref().unwrap()) == "bob")
                .map(|r| String::from_utf8_lossy(r[0].as_ref().unwrap()).to_string())
                .unwrap()
        }
        _ => panic!(),
    };
    conn.query(&format!("kill {}", bob_id)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    // bob 连接应已断开
    assert!(
        bob.ping().await.is_err(),
        "killed connection should be dead"
    );

    // ---- DROP TABLE：配置移除 + 物理 DROP 扇出 ----
    env.log.lock().clear();
    conn.query("DROP TABLE dbx.t").await.unwrap();
    let schema_json =
        std::fs::read_to_string(env.home.path().join("schemas/dbx.schema.json")).unwrap();
    assert!(
        !schema_json.contains("\"t\""),
        "table t should be removed from config"
    );
    let drops = business_sql(&env.log)
        .iter()
        .filter(|s| s.to_uppercase().starts_with("DROP TABLE"))
        .count();
    assert_eq!(drops, 4, "{:?}", business_sql(&env.log));

    // ---- 配置热重载 hint ----
    // 手工改文件（新增 schema），reload 后生效
    let mut cfg2 = (*env.state.config()).clone();
    cfg2.schemas.insert(
        "manual_db".into(),
        LogicSchemaConfig {
            schema_name: "manual_db".into(),
            target_name: "c0".into(),
            ..Default::default()
        },
    );
    FileConfigStorage::new(env.home.path())
        .save_all(&cfg2)
        .unwrap();
    conn.query("/*!mycat:loadConfigFromFile*/").await.unwrap();
    let rows = match conn.query("show databases").await.unwrap() {
        QueryOutcome::Rows(rs) => rs.decode_rows(),
        _ => panic!(),
    };
    assert!(
        rows.iter()
            .any(|r| r[0].as_deref() == Some(&b"manual_db"[..])),
        "reload did not pick up manual_db"
    );

    // ---- DROP DATABASE ----
    conn.query("DROP DATABASE manual_db").await.unwrap();
    assert!(!env
        .home
        .path()
        .join("schemas/manual_db.schema.json")
        .exists());

    // ---- CREATE SEQUENCE + 计数序列填充 ----
    conn.query("CREATE SEQUENCE dbx.counter START WITH 500 INCREMENT BY 5 SIMPLE")
        .await
        .unwrap();
    assert!(env
        .home
        .path()
        .join("sequences/dbx.counter.sequence.json")
        .exists());

    conn.quit().await;
}

/// 从 INSERT ... VALUES (<num>, ...) 或 (..., <num>) 中提取最大的大整数（snowflake id）。
fn extract_big_int_after_values(sql: &str) -> i64 {
    let pos = sql.to_uppercase().find("VALUES").unwrap_or(0);
    let rest = &sql[pos..];
    let mut best: i64 = -1;
    for tok in rest.split(|c: char| !c.is_ascii_digit() && c != '-') {
        if let Ok(v) = tok.parse::<i64>() {
            if v > best {
                best = v;
            }
        }
    }
    assert!(best > 1000, "no snowflake id found in {}", sql);
    best
}

#[test]
fn test_show_users_and_sequence_config() {
    run_big(|| {
        big_rt().block_on(async {
            let env = setup().await;
            let mut conn = connect(&env, "root", "", "").await;
            conn.query("create user alice identified by 'pw'")
                .await
                .unwrap();
            match conn.query("show users").await.unwrap() {
                QueryOutcome::Rows(rs) => {
                    let rows = rs.decode_rows();
                    let names: Vec<String> = rows
                        .iter()
                        .map(|r| String::from_utf8_lossy(r[0].as_ref().unwrap()).to_string())
                        .collect();
                    assert!(
                        names.contains(&"alice".to_string()) && names.contains(&"root".to_string())
                    );
                }
                _ => panic!(),
            }
            conn.query("drop user alice").await.unwrap();
            assert!(!env.home.path().join("users/alice.user.json").exists());
            // root 不可删除
            assert!(conn.query("drop user root").await.is_err());
            conn.quit().await;
        })
    })
}

#[test]
fn test_savepoint_forwarding() {
    run_big(|| {
        big_rt().block_on(async {
            let env = setup().await;
            let mut conn = connect(&env, "root", "", "").await;
            conn.query("create database dbx").await.unwrap();
            conn.query("use dbx").await.unwrap();
            conn.query("create table nt (id bigint primary key, v varchar(10))")
                .await
                .unwrap();
            // 事务 + savepoint：转发到固定连接
            conn.query("begin").await.unwrap();
            env.log.lock().clear();
            conn.query("insert into nt values (1, 'a')").await.unwrap();
            conn.query("savepoint sp1").await.unwrap();
            conn.query("insert into nt values (2, 'b')").await.unwrap();
            conn.query("rollback to savepoint sp1").await.unwrap();
            conn.query("commit").await.unwrap();
            let sqls = business_sql(&env.log);
            assert!(
                sqls.iter()
                    .any(|s| s.to_uppercase().contains("SAVEPOINT `SP1`")
                        || s.to_lowercase().contains("savepoint `sp1`")),
                "{:?}",
                sqls
            );
            assert!(
                sqls.iter()
                    .any(|s| s.to_lowercase().contains("rollback to savepoint")),
                "{:?}",
                sqls
            );
            conn.quit().await;
        })
    })
}
