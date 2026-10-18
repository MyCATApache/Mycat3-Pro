# Oracle/TNS 前端可行性评估报告

> 评估日期：2026-10-16 · 结论：**技术上可行（判断修正），工程量约为 PG+TDS 前端总和的 3–5 倍，
> 建议按三阶段推进，MVP 以 12c–23ai 明文连接 + thin 驱动/JDBC/sqlplus 简单查询为目标**

本报告回答："Mycat3 Pro 能否像 PostgreSQL/TDS 一样实现 Oracle(TNS) 前端，让真实
Oracle 客户端（sqlplus、JDBC thin、python-oracledb 等）连入同一 8066 端口？"

## 0. 结论摘要

| 问题 | 结论 |
|---|---|
| 协议知识是否公开 | **是（2022–2026 发生质变）**。Oracle 官方开源了 Python/Node/Go/**Rust** 四种 thin 驱动（UPL/Apache 双许可），客户端侧协议全部以源码形式公开；其中 rust-oracledb（~22–25K 行 Rust）与 Pro 同语言 |
| 认证能否从明文密码做服务端 | **能**。O5L(11g)/O7L(12c+) 的 verifier 推导与 AUTH_SESSKEY 交换算法完全公开（官方驱动源码 + JtR/hashcat verifier 配方），服务端可按需从配置的明文密码实时推导 |
| ANO 网络加密是否必须 | **默认配置下不必须**。19c/21c/26ai 的 SQLNET.ENCRYPTION_* 默认均为 `accepted`（可协商明文）；且 Oracle 官方四种 thin 驱动**都不支持 ANO**（只支持 TLS），面向 thin 客户端可完全不实现。仅 OCI/sqlplus 且服务端 REQUIRED 的加固场景需要 ANO —— 其算法也已被 go-ora (MIT)/oracle-nio (Apache-2.0) 公开实现 |
| 服务端先例是否存在 | **存在**。oratofusionproxy（2026，闭源单二进制）实现了 TNS/TTC 服务端，经 sqlplus、SQL Developer、DBeaver、ojdbc、python-oracledb thin、go-ora、ODP.NET、Power BI 乃至**真实 Oracle 数据库 DBLINK**（19c 与 23ai/26ai 两代 TTC）验证 |
| 主要难点 | 不是算法保密，而是 **TTC 层的消息矩阵与版本长尾**：两代 TTC field version、OALL8 执行流、describe/fetch、Oracle NUMBER 二进制编码、charset 协商、piggyback、marker/break、sqlplus 的会话初始化 RPC 长尾 |
| Java 版 Mycat3 为何失败 | 其 TNS 是 PoC：无认证（固定 root）、把 DATA 包当 UTF-8 文本 SQL、响应是自定义字节块而非 TTC —— 真实客户端在 CONNECT/认证阶段就无法对话 |
| 建议 | 分三阶段（见 §6）。Phase 1+2 约 6–10K 行 Rust 即可让 python-oracledb thin / ojdbc thin / sqlplus 执行简单查询与 DML；Phase 3（LOB/游标/ANO/sqlplus 长尾）按需推进 |

## 1. Oracle 网络协议栈解剖

```
┌──────────────────────────────────────────────────────┐
│ OPI/TTC (Two-Task Common)    SQL 执行/取数/描述的 RPC 协议 │
│   OALL8(执行) TTIFETCH TTIDCB(describe) TTIRXH/RXD(行)   │
│   TTIDTY(类型协商) TTIOER(错误) piggyback                │
├──────────────────────────────────────────────────────┤
│ ANO/SNS (Advanced Security)  可选：DH 密钥交换 +          │
│   AES-CBC(128/192/256) 加密 + keyed SHA/MD5 校验和       │
├──────────────────────────────────────────────────────┤
│ 认证层  O3L(≤10g DES) / O4L(10g) / O5L(11g SHA1+AES192)  │
│         / O7L(12c+ PBKDF2-SHA512+AES256) / 23ai fast-auth│
├──────────────────────────────────────────────────────┤
│ TNS (传输衬底)  CONNECT/ACCEPT/RESEND/DATA/MARKER/       │
│   REFUSE/REDIRECT/CONTROL 包，8 字节头，SDU 协商          │
└──────────────────────────────────────────────────────┘
```

与 PostgreSQL（单层文本/二进制消息）和 TDS（单层 token 流）不同，Oracle 是**四层协议栈**，
每层都有独立的版本协商。这是工程量差异的本质来源。

## 2. 公开文献与开源参考盘点（全部经 2026-10 在线核验）

### 2.1 官方 thin 驱动（客户端侧协议的事实标准参考）

| 项目 | 许可 | 规模 | 关键内容 |
|---|---|---|---|
| [oracle/rust-oracledb](https://github.com/oracle/rust-oracledb)（2026-07，crates.io `oracledb` 26.0.0-beta） | UPL-1.0 OR Apache-2.0 | ~22–25K 行 Rust / 78 文件 | **与 Pro 同语言**。`src/messages/auth.rs`（552 行，O7L 全套 PBKDF2-SHA512+AES-256）、`src/ora_type/number.rs`（Oracle NUMBER 二进制编解码）、data_types.rs（38.6KB 类型 marshaling） |
| [oracle/python-oracledb](https://github.com/oracle/python-oracledb) thin（2022-05 起） | UPL-1.0 OR Apache-2.0 | ~15.5K 行 Cython | 最完整的消息目录：`packet.pyx`(TNS 923 行)、`protocol.pyx`(TTC)、`messages/base.pyx`(1808 行，全部消息类型/函数码/能力位 `TNS_CCAP_*`)、`messages/auth.pyx`+`crypto.pyx`（O5L+O7L 完整密码学）、`fast_auth.pyx`(23ai) |
| [oracle/node-oracledb](https://github.com/oracle/node-oracledb/tree/main/lib/thin) thin | UPL/Apache | ~45 文件 >500KB JS | `sqlnet/ANO.js`（ANO 协商报文结构，虽不加密） |
| [oracle/go-oracledb](https://github.com/oracle/go-oracledb)（2026-04） | UPL-1.0 | — | 同上生态 |
| [sijms/go-ora](https://github.com/sijms/go-ora) v3（第三方，MIT） | MIT | 680 文件 | **ANO 加密完整实现**：`advanced_nego/`（SNS 协商+DH 交换）、`network/security/`（AES-CBC 非标准填充、RC4/AES 派生 keyed checksum），实测 23ai `ENCRYPTION_SERVER=required` + AES256 |

### 2.2 认证算法（服务端方向）公开程度

- **O7L (12c+) verifier**：`PBKDF2-HMAC-SHA512(password, salt ‖ "AUTH_PBKDF2_SPEEDY_KEY", 4096, 64)` →
  `SHA512(key ‖ salt)[:32]` → AES-256 会话密钥。配方见 [hashcat module 12300](https://github.com/hashcat/hashcat/blob/master/src/modules/module_12300.c)
  （源码注释含完整公式）与 [JtR oracle12c_fmt_plug.c](https://github.com/openwall/john/tree/bleeding-jumbo/src)（Dhiru Kholia 2015）。
- **O5L (11g) verifier**：`SHA1(UTF16BE(upper(user) ‖ password)) ‖ salt`，见 [JtR oracle11_fmt_plug.c](https://github.com/openwall/john)（Alexandre Hamelin 2008）。
- **AUTH_SESSKEY 交换全流程**（服务端要发的 KV 集：`AUTH_VFR_DATA`、`AUTH_SESSKEY`、
  `AUTH_PBKDF2_VGEN_COUNT`、`AUTH_PBKDF2_CSK_SALT`、`AUTH_PBKDF2_SDER_COUNT`、`AUTH_SVR_RESPONSE`…，
  含组合密钥推导 `MD5(A⊕B)` / PBKDF2(hex(B‖A)) 与 `SERVER_TO_CLIENT` 证明串）：
  python-oracledb `messages/auth.pyx` + `crypto.pyx` 逐行可读；rust-oracledb `auth.rs` 同。
- **服务端从明文密码认证的存在性证明**：[oratofusionproxy](https://github.com/krokozyab/oracle-fusion-tns-proxy/blob/master/doc/auth.md)
  （"The password … must be the shared value you set with --oracle-password"）——即从共享明文
  实时推导 verifier 完成 O5L/O7L 服务端认证，被 sqlplus/ojdbc/dblink 接受。
- 历史文献：nmap [nselib/tns.lua](https://github.com/nmap/nmap/blob/master/nselib/tns.lua)（2010，O3L/O4L/O5L 客户端，
  引用 Montoro(oxid.it)/Tóth(soonerorlater.hu) 的早期 AES 分析）；[CVE-2012-3137](https://nvd.nist.gov/vuln/detail/CVE-2012-3137)
  （O5L 会话密钥泄露，Esteban Martinez Fayo，exploit-db 22069）。

### 2.3 TTC 报文布局文档

- [Wireshark packet-tns.c](https://gitlab.com/wireshark/wireshark/-/blob/master/epan/dissectors/packet-tns.c)
  （~10K 行，GPL-2+）：2026 年 9–10 月由 Peter Lemenkov 大幅重写，现已解码 TTI_ALL8（含绑定变量）、
  TTI_FETCH、TTI_DCB、TTI_RXH/RXD（含 ROWID/LOB）、TTI_DTY（12c/23ai 类型表）、TTI_OER、
  piggyback、AQ/TPC、23ai fast-auth、ANO 算法表——**当前最完整的 TTC 叙述性文档**（GPL：只读学习，不可抄码）。
- [Redwood Wire Protocol 非官方规范](https://github.com/redwood-wire-protocol)（CC BY-SA 4.0 文档 + Apache-2.0 示例码）：
  TNS 字段规则、TTC/TTI 消息目录、OPI 函数码全表（OAUTH 0x73 / OSESSKEY 0x76 / O3LOGON / TTISNS 0xDE）。
  注意：是"目录"性质，字段级规则稀疏；其配套的 libredwoodwpsrv（服务端库）等仓库**均为空壳未交付**。

### 2.4 服务端先例与成本数据点

- **oratofusionproxy**（2026-04，单人开发，闭源二进制）：自称"from-scratch 真实线协议实现"，
  兼容矩阵覆盖 sqlplus/SQL Developer/DBeaver/SQLcl/ojdbc/python-thin/go-ora/ODP.NET(managed+unmanaged)/
  Excel Power Query/Power BI/**19c 与 23ai/26ai 数据库经 DBLINK 连入（两代 TTC）**；无 ANO。
  —— 证明小型团队可在今天做出被真实客户端接受的服务端。
- **oracle-nio OracleMockServer**（Apache-2.0，Swift）：fixture 回放式 TNS/TTC mock 服务端（测试用途）。
- **Babelfish for Aurora PostgreSQL**（AWS，SQL Server 协议服务端仿真）：2020-12 宣布 → 2021-10 开源
  （Apache-2.0），此后持续多年多版本 —— 全协议+方言+语义的服务端仿真是**多人年工程**；但其难点在
  T-SQL 语义，纯协议代理层（本项目场景）小一个数量级。
- Metasploit tnscmd/oracle 模块、fapro 蜜罐：仅 listener 级 TNS（version/status/ping），无 TTC。

## 3. Java 版 Mycat3 TNS PoC 失败原因对照

| 层次 | Java PoC 行为 | 真实客户端要求 |
|---|---|---|
| TNS | CONNECT→ACCEPT+RESEND（无版本/SDU/服务协商） | 版本 312+ 协商、SDU、ANO 服务数组（至少协商"无服务"） |
| 认证 | **无**（固定 root 用户，接受任何存活连接） | O5L/O7L 挑战-响应（AUTH_SESSKEY 交换 + AUTH_PASSWORD 证明） |
| 数据 | DATA 包按 UTF-8 文本 SQL 解析 | TTC 二进制 RPC（OALL8/PI 协商/类型 marshaling） |
| 响应 | 自定义字节块 | TTIRXH/RXD/OER 标准行/错误消息 |

即：Java PoC 在**每一层**都与真实协议不同，故只能与自写的假客户端闭环。

## 4. 分层可行性与工程量估算（Rust，服务端方向）

| 层 | 可行性 | 估算规模 | 参考实现 |
|---|---|---|---|
| TNS 传输（CONNECT/ACCEPT/RESEND/DATA/MARKER/REFUSE、SDU 分包） | 确定可行 | ~600–900 行 | python-oracledb packet.pyx（923 行，客户端侧等价物） |
| ANO 协商"关闭"路径（声明无加密服务，thin 客户端/JDBC 默认接受） | 确定可行 | ~300 行 | node-oracledb ANO.js、go-ora advanced_nego |
| O7L 服务端认证（明文密码→PBKDF2 verifier→SESSKEY 交换→PASSWORD 验证→SVR_RESPONSE） | 确定可行（算法全公开） | ~800–1200 行 | rust-oracledb auth.rs（552 行客户端）+ hashcat 12300 + oratofusionproxy 存在性证明 |
| O5L 服务端认证（兼容 11g verifier 客户端） | 可行 | ~400 行 | python-oracledb auth.pyx/crypto.pyx |
| TTC PI/能力协商（固定 1–2 个 field version，其余拒绝——与官方 thin 驱动同策略） | 可行 | ~500 行 | protocol.pyx + Wireshark 解析器 |
| OALL8 执行 + TTIDCB describe + TTIFETCH + 行 marshaling（NUMBER/VARCHAR2/CHAR/DATE/TIMESTAMP/NULL） | 可行（NUMBER 编解码有双向参考） | ~2000–3000 行 | messages/base.pyx、rust-oracledb number.rs/data_types.rs、Wireshark TTIRXD |
| 错误路径（TTIOER）、LOGOFF、PING、SESSION_RELEASE、marker/break | 可行 | ~400 行 | 同上 |
| sqlplus 兼容长尾（会话初始化 RPC 序列、NLS 参数、ARRAYSIZE 等） | **中风险**（试错型工作，无完整清单） | ~500–1500 行 + 大量联调 | oratofusionproxy 兼容矩阵作为行为基准 |
| LOB/游标/复杂类型（CLOB/BLOB prefetch、REF CURSOR） | 可延后 | ~1000+ 行 | python-oracledb LOB 消息 |
| ANO 加密（DH+AES-CBC+keyed checksum；仅 OCI 客户端 + REQUIRED 场景需要） | 可行（算法公开） | ~800 行 | go-ora security/、oracle-nio AdvancedNegotiation |
| 23ai fast-auth、26ai Deep Data Security | **不建议**（服务端内部细节未公开） | — | — |

**MVP（Phase 1+2）合计约 6–10K 行**：TNS + ANO-off + O7L(+O5L) + PI 协商 + OALL8 简单查询/DML +
基本类型 + 错误/LOGOFF。对照：PG 前端 ~1000 行、TDS 前端 ~900 行（Pro 实测），Oracle 约为两者之和的 3–5 倍。

## 5. 风险清单

1. **版本矩阵长尾**（最大风险）：TTC field version 按服务器/客户端版本组合变化；官方 thin 驱动的
   策略是钉住能力协商（12.1+），Pro 应同样明确支持窗口（建议 12.1–23ai，26ai best-effort），
   窗口外客户端明确拒绝。oratofusionproxy 的"两代 TTC"表述说明至少要处理 19c 代与 23ai 代两套字段。
2. **无权威规范**：所有细节来自客户端源码反推 + 抓包比对，服务端行为（如 piggyback 时机、
   break 协议）需要以真实 Oracle XE/Free 为金标准反复差分调试。**测试策略必须包含
   `gvenzl/oracle-free` 容器做真服务器对照 + python-oracledb thin 做可控测试客户端**（其源码
   即预期行为清单）。
3. **sqlplus/OCI 客户端**比 thin 驱动苛刻得多（初始化 RPC 多、依赖 NLS/ARRAYSIZE 行为、
   可能要求 ANO）。MVP 应声明支持 python-oracledb thin / node thin / go-ora / ojdbc thin /
   DBeaver 等 JDBC 工具；sqlplus 列为 Phase 3 目标（oratofusionproxy 证明可达，但需长尾联调）。
4. **加密加固部署**（ENCRYPTION_SERVER=REQUIRED，OCI 云默认）：需 ANO 才能服务 OCI 客户端；
   好在算法公开（go-ora/oracle-nio），列为 Phase 3 可选项。
5. **法律/许可**：协议事实（报文布局、KV 名、密码学配方）不受版权保护；UPL/Apache/MIT 参考码
   可移植（保留声明）；Wireshark(GPL)/nmap 代码只读不抄。Oracle 从未起诉协议实现者，且自己
   以宽松许可发布参考实现——法律风险低（非法律意见）。
6. **维护成本**：Oracle 每年发版（23ai→26ai），协议演进（fast-auth、Deep Data Security）需要
   持续跟进官方驱动源码变更。

## 6. 建议路线图

**Phase 0 — 差分实验台（1–2 天）**
`docker run gvenzl/oracle-free` 起真实 23ai/Free；用 python-oracledb thin 连接并抓包
（Wireshark TNS 解析器）；把 CONNECT→认证→首次查询的完整字节流存为 golden fixtures。

**Phase 1 — TNS + O7L 认证（~2–3K 行，1–2 周）**
CONNECT/ACCEPT/RESEND/DATA/MARKER、SDU 协商、ANO"无服务"应答、O7L 服务端
（明文密码→verifier→SESSKEY→PASSWORD 验证→SERVER_TO_CLIENT 证明）。
验收：python-oracledb thin / go-ora 报"认证通过"（随后在 PI 协商处失败属预期）。

**Phase 2 — TTC 查询通路（~3–5K 行，2–4 周）**
PI/能力协商（钉住 12.1+ 两代 field version）、OALL8 简单 SELECT/DML、TTIDCB describe、
TTIRXH/RXD 行编解码（NUMBER/VARCHAR2/CHAR/DATE/TIMESTAMP/NULL）、TTIOER 错误、LOGOFF/PING。
接入点：`sniff.rs` 已能识别 TNS CONNECT（byte[4]==1），bootstrap 换掉现在的"明确拒绝"分支；
SQL 执行复用 `engine::execute_sql`（三协议共享层天然支持第四个前端）。
验收：python-oracledb thin / ojdbc thin / DBeaver 对 Mycat Pro 执行
`select * from orders where id=:1`、`insert`、事务 begin/commit，结果与 MySQL 前端一致。

**Phase 3 — 兼容长尾（按需）**
sqlplus 初始化 RPC 序列、LOB、REF CURSOR、piggyback、O5L(11g)、ANO 加密（OCI 场景）、
23ai fast-auth 评估。以 oratofusionproxy 兼容矩阵为行为基准逐项点亮。

## 7. 最终建议

- **若目标用户存在 Oracle 工具链诉求**（用 SQL Developer/DBeaver/ojdbc 管理 Mycat 分片集群，
  或从 Oracle DBLINK 联邦查询 Mycat）：值得投入，Phase 1+2 即可交付有感知价值的 MVP，
  且全部关键算法有官方开源参考，技术风险已从"未知"降为"工程量与长尾调试"。
- **若仅是协议覆盖面上的完备性诉求**：维持现状（嗅探识别 + 明确拒绝）是诚实且零维护成本的选择；
  Java 版 Mycat3 的 TNS PoC 反而是不该模仿的反面教材（给假成功的错觉）。
- Mycat3 Pro 当前决策：**保持明确拒绝**，本报告作为后续立项依据。若立项，先做 Phase 0
  差分实验台（成本一天），用 golden fixtures 把不确定性压到最低后再进入 Phase 1。

## 附：本报告与旧结论的差异说明

早期评估（见 compatibility.md 初版）认为"真实 Oracle 客户端需要 TTC/O5LOGON 私有协议，
不可行"。该判断在 2022 年前是成立的（当时仅有 nmap/Metasploit 的 listener 级实现和已失效的
博客文献）。2022–2026 年 Oracle 官方相继开源四种语言的 thin 驱动（含 Rust）、go-ora 公开 ANO
加密实现、Wireshark TTC 解析器完成重写、oratofusionproxy 证明独立服务端可行——协议知识已
系统性公开，故修正为"有条件可行"。
