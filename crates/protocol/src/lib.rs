//! Mycat3 Pro 协议层：MySQL 客户端/服务端协议编解码。
//!
//! 移植自 Mycat3 `common`/`proxy` 模块的协议实现：
//! - [`consts`]：能力标志、状态标志、命令类型、字段类型、错误码
//! - [`codec`]：lenenc 整数/字符串、载荷读写器
//! - [`frame`]：包分帧与流式解码（含 >16MB 分包重组）
//! - [`auth`]：mysql_native_password / caching_sha2_password scramble
//! - [`packets`]：握手、认证、OK/ERR/EOF、列定义、文本/二进制行等
//!
//! # 与 Java 版的差异（有意为之的修复）
//! 1. ERR 包恢复标准 `'#' + sqlstate` 格式（Java 版被注释掉）。
//! 2. 认证种子使用可打印 ASCII 字节（Java 版可能产生 NUL）。
//! 3. 帧解码器正确重组 >16MB 多分包（Java Vert.x 版会覆盖中间分包）。

pub mod auth;
pub mod codec;
pub mod consts;
pub mod error;
pub mod frame;
pub mod packets;

pub use codec::{PacketReader, PacketWriter};
pub use error::{ProtocolError, ProtocolResult};
pub use frame::{encode_frames, encode_packet, FrameDecoder, Packet};
