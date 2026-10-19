//! Mycat3 Pro 可执行入口。
//!
//! 对应 Java 版 `io.mycat.MycatCore.main`：
//! 1. 解析命令行 / 定位 MYCAT_HOME
//! 2. 加载（或生成默认）JSON 配置
//! 3. 构建 ServerState（元数据 / 副本运行时 / 连接管理器）
//! 4. 启动心跳定时任务、监控 HTTP、前端 MySQL 协议监听
//! 5. Ctrl-C 优雅停机
//!
//! 命令：
//! - `mycat3-pro start [--home DIR]`  启动服务（默认）
//! - `mycat3-pro init  [--home DIR]`  生成默认配置
//! - `mycat3-pro check [--home DIR]`  校验配置
//! - `mycat3-pro version`              打印版本

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use mycat3_config::{default_config, FileConfigStorage, MycatRouterConfig};
use mycat3_server::bootstrap::start_server;
use mycat3_server::monitor::start_monitor;
use mycat3_server::state::ServerState;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[command(name = "mycat3-pro", version = VERSION, about = "Mycat3 Pro — 分布式数据库中间件（Rust 实现）")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// 启动服务（默认命令）
    Start {
        /// 配置目录（MYCAT_HOME）。默认取环境变量 MYCAT3_PRO_HOME / MYCAT_HOME，否则 ./mycat-home
        #[arg(long)]
        home: Option<PathBuf>,
    },
    /// 生成默认配置文件
    Init {
        #[arg(long)]
        home: Option<PathBuf>,
    },
    /// 校验配置文件合法性
    Check {
        #[arg(long)]
        home: Option<PathBuf>,
    },
    /// 打印版本信息
    Version,
}

fn resolve_home(arg: Option<PathBuf>) -> PathBuf {
    if let Some(p) = arg {
        return p;
    }
    for var in ["MYCAT3_PRO_HOME", "MYCAT_HOME"] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                return PathBuf::from(v);
            }
        }
    }
    PathBuf::from("./mycat-home")
}

fn load_or_init_config(home: &PathBuf) -> anyhow::Result<MycatRouterConfig> {
    let storage = FileConfigStorage::new(home);
    let server_json = home.join("server.json");
    if !server_json.exists() {
        info!(home = %home.display(), "no config found, writing default config (等价 ConfigUpdater.writeDefaultConfigToFile)");
        let cfg = default_config();
        storage.save_all(&cfg).context("write default config")?;
        return Ok(cfg);
    }
    storage.load_all().context("load config")
}

fn validate_config(cfg: &MycatRouterConfig) -> anyhow::Result<()> {
    // 运行模式合法性与代理模式约束
    let mode = cfg.server.mode.to_ascii_lowercase();
    if !matches!(mode.as_str(), "local" | "cluster" | "proxy") {
        anyhow::bail!(
            "server.mode '{}' invalid (supported: local | cluster | proxy)",
            cfg.server.mode
        );
    }
    if cfg.is_proxy_mode() {
        let target = cfg.proxy_target();
        if !cfg.clusters.contains_key(&target)
            && !cfg.datasources.contains_key(&target)
            && target != cfg.prototype
        {
            anyhow::bail!(
                "proxy.target '{}' not found (must be a cluster, datasource or prototype)",
                target
            );
        }
        mycat3_server::intercept::compile(&cfg.server.proxy.intercepts)
            .map_err(|e| anyhow::anyhow!("proxy.intercepts invalid: {}", e))?;
    }
    // 集群引用的数据源必须存在
    for (cname, cluster) in &cfg.clusters {
        for ds in cluster.masters.iter().chain(cluster.replicas.iter()) {
            if !cfg.datasources.contains_key(ds) {
                anyhow::bail!("cluster '{}' references unknown datasource '{}'", cname, ds);
            }
        }
    }
    // 数据源 URL 可解析（按 dbType 分派：mysql 用 JDBC 解析；oracle 用驱动连接串解析）
    for (name, ds) in &cfg.datasources {
        if ds.db_type.eq_ignore_ascii_case("oracle") {
            mycat3_oracle::config_from_jdbc_url(&ds.url, &ds.user, &ds.password)
                .with_context(|| format!("datasource '{}' oracle url invalid", name))?;
        } else if ds.db_type.eq_ignore_ascii_case("postgresql")
            || ds.db_type.eq_ignore_ascii_case("postgres")
            || ds.db_type.eq_ignore_ascii_case("pgsql")
        {
            mycat3_pgclient::parse_pg_jdbc_url(&ds.url, &ds.user, &ds.password)
                .with_context(|| format!("datasource '{}' postgres url invalid", name))?;
        } else if ds.db_type.eq_ignore_ascii_case("mssql")
            || ds.db_type.eq_ignore_ascii_case("sqlserver")
            || ds.db_type.eq_ignore_ascii_case("mssqlserver")
        {
            mycat3_mssql::parse_mssql_jdbc_url(&ds.url, &ds.user, &ds.password)
                .with_context(|| format!("datasource '{}' SQL Server url invalid", name))?;
        } else {
            ds.parse_url()
                .with_context(|| format!("datasource '{}' url invalid", name))?;
        }
    }
    // schema targetName 必须是已知集群或数据源
    for (sname, schema) in &cfg.schemas {
        let t = &schema.target_name;
        if !cfg.clusters.contains_key(t) && !cfg.datasources.contains_key(t) {
            anyhow::bail!("schema '{}' references unknown target '{}'", sname, t);
        }
        // 元数据可编译（分片函数合法）
        let _ = mycat3_sql::Metadata::build(cfg)
            .with_context(|| format!("schema '{}' table config invalid", sname))?;
    }
    // 用户默认 schema 存在性仅告警
    for (uname, user) in &cfg.users {
        for s in &user.schemas {
            if !cfg.schemas.contains_key(s) {
                eprintln!(
                    "warning: user '{}' references unknown schema '{}'",
                    uname, s
                );
            }
        }
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Start 命令：先同步读取配置以确定 reactor 线程数（对应 VertxOptions.setEventLoopPoolSize）
    if let Some(Commands::Start { ref home }) = cli.command {
        let home = resolve_home(home.clone());
        init_tracing();
        let cfg = load_or_init_config(&home)?;
        let reactor = cfg.server.server.reactor_number.max(1);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(reactor)
            // 深层异步调用链 + SQL 解析器需要较大栈（debug 构建尤其）
            .thread_stack_size(8 * 1024 * 1024)
            .enable_all()
            .build()
            .context("build tokio runtime")?;
        return runtime.block_on(run_start(home, cfg));
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(run_other(cli))
}

async fn run_start(home: PathBuf, cfg: MycatRouterConfig) -> anyhow::Result<()> {
    {
        let _ = &home;
        info!(version = VERSION, home = %home.display(), "Mycat3 Pro starting");
        validate_config(&cfg)?;

        let reactor = cfg.server.server.reactor_number.max(1);
        let server_ip = cfg.server.server.ip.clone();
        let server_port = cfg.server.server.port;
        let monitor_open = cfg.server.monitor.open;
        let monitor_ip = cfg.server.monitor.ip.clone();
        let monitor_port = cfg.server.monitor.port;

        let state =
            ServerState::with_home(cfg, Some(home.clone())).context("build server state")?;

        // 心跳定时任务（对应 ReplicaSelectorRuntime.start()，周期 5s）
        let hb = state.heartbeat();
        // spawn_loop 内部已派生常驻任务，句柄可安全丢弃（分离运行）
        let _heartbeat_loop = hb.spawn_loop(std::time::Duration::from_secs(5));

        info!(
            "Mycat3 Pro ready — mysql://{}:{} (monitor: {}:{}), reactors={}",
            server_ip, server_port, monitor_ip, monitor_port, reactor
        );

        // 监控 HTTP（对应 PrometheusExporter().run()）
        if monitor_open {
            let st = state.clone();
            tokio::spawn(async move {
                if let Err(e) = start_monitor(st, monitor_ip, monitor_port).await {
                    tracing::error!("monitor server error: {}", e);
                }
            });
        }

        // 优雅停机：SIGINT（Ctrl-C）与 SIGTERM（docker stop / k8s）
        let server = start_server(state.clone());
        let shutdown = async {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                match signal(SignalKind::terminate()) {
                    Ok(mut term) => {
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => {}
                            _ = term.recv() => {}
                        }
                    }
                    Err(_) => {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
        };
        tokio::select! {
            r = server => { r?; }
            _ = shutdown => {
                info!("shutdown signal received, shutting down");
            }
        }
        Ok(())
    }
}

async fn run_other(cli: Cli) -> anyhow::Result<()> {
    match cli.command.unwrap_or(Commands::Start { home: None }) {
        Commands::Version => {
            println!(
                "Mycat3 Pro {} (rustc target: {})",
                VERSION,
                std::env::consts::ARCH
            );
            Ok(())
        }
        Commands::Init { home } => {
            let home = resolve_home(home);
            let storage = FileConfigStorage::new(&home);
            storage.save_all(&default_config())?;
            println!("default config written to {}", home.display());
            Ok(())
        }
        Commands::Check { home } => {
            let home = resolve_home(home);
            let cfg = load_or_init_config(&home)?;
            validate_config(&cfg)?;
            println!(
                "config OK: {} datasources, {} clusters, {} schemas, {} users (home={})",
                cfg.datasources.len(),
                cfg.clusters.len(),
                cfg.schemas.len(),
                cfg.users.len(),
                home.display()
            );
            Ok(())
        }
        Commands::Start { .. } => unreachable!("handled in main"),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
