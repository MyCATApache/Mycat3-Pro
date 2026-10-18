//! MySQL 包分帧（framing）：3 字节小端长度 + 1 字节序号 + 载荷。
//!
//! - `encode_frames` 移植自 `MySQLPacketUtil.generateMySQLPacket` + `MySQLPacketSplitter`：
//!   载荷超过 0xFFFFFF 时自动分包，序号递增，最后一包恰好为 0xFFFFFF 时追加空包。
//! - `FrameDecoder` 移植自 `VertxMySQLPacketResolver` 的 HEAD/PAYLOAD 两状态流式解码器，
//!   并修复了 Java 版对 >16MB 多包重组不完整的问题（正确拼接连续 0xFFFFFF 分包）。

use crate::consts::MAX_PACKET_SIZE;
use crate::error::{ProtocolError, ProtocolResult};

/// 将逻辑载荷编码为一个或多个物理包，写入 `out`。
///
/// `seq` 为起始序号的引用，函数内会按写出的物理包数量递增（回绕 255）。
pub fn encode_frames(seq: &mut u8, payload: &[u8], out: &mut Vec<u8>) {
    let mut offset = 0usize;
    loop {
        let chunk = &payload[offset..std::cmp::min(offset + MAX_PACKET_SIZE, payload.len())];
        out.extend_from_slice(&(chunk.len() as u32).to_le_bytes()[..3]);
        out.push(*seq);
        *seq = seq.wrapping_add(1);
        out.extend_from_slice(chunk);
        offset += chunk.len();
        if chunk.len() < MAX_PACKET_SIZE {
            break;
        }
        if offset >= payload.len() {
            // 最后一包恰好满 0xFFFFFF，追加零长包结束
            out.extend_from_slice(&[0, 0, 0]);
            out.push(*seq);
            *seq = seq.wrapping_add(1);
            break;
        }
    }
}

/// 编码单个包并返回字节。
pub fn encode_packet(seq: u8, payload: &[u8]) -> Vec<u8> {
    let mut seq = seq;
    let mut out = Vec::with_capacity(payload.len() + 4);
    encode_frames(&mut seq, payload, &mut out);
    out
}

/// 一个完整的逻辑包（已重组多分包）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub seq: u8,
    pub payload: Vec<u8>,
}

/// 流式包解码器。
#[derive(Debug, Default)]
pub struct FrameDecoder {
    /// 已读取的头部字节（<4 时处于 HEAD 状态）
    head: Vec<u8>,
    /// 当前物理包载荷缓冲
    cur: Vec<u8>,
    /// 多包重组缓冲（跨物理包拼接）
    acc: Vec<u8>,
    /// 当前物理包期望长度
    expect: usize,
    /// 是否处于多包重组中（上一个物理包长度 == 0xFFFFFF）
    multi: bool,
    /// 多包重组时的起始序号
    multi_seq: u8,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// 喂入字节，返回其中包含的所有完整逻辑包。
    pub fn feed(&mut self, data: &[u8]) -> ProtocolResult<Vec<Packet>> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        // 注意：head 已满且期望长度为 0 时（空包），即使输入耗尽也需再迭代一次以完成该包
        while offset < data.len() || (self.head.len() == 4 && self.cur.len() == self.expect) {
            if self.head.len() < 4 {
                let need = 4 - self.head.len();
                let take = std::cmp::min(need, data.len() - offset);
                self.head.extend_from_slice(&data[offset..offset + take]);
                offset += take;
                if self.head.len() == 4 {
                    self.expect =
                        u32::from_le_bytes([self.head[0], self.head[1], self.head[2], 0]) as usize;
                    if self.expect > MAX_PACKET_SIZE {
                        return Err(ProtocolError::PacketTooLarge(self.expect));
                    }
                    self.cur.reserve(self.expect);
                }
                continue;
            }
            // PAYLOAD 状态
            let take = std::cmp::min(self.expect - self.cur.len(), data.len() - offset);
            self.cur.extend_from_slice(&data[offset..offset + take]);
            offset += take;
            if self.cur.len() == self.expect {
                let seq = self.head[3];
                let full = self.expect == MAX_PACKET_SIZE;
                self.head.clear();
                self.expect = 0;
                if full {
                    // 分包未结束，进入/继续多包重组
                    if !self.multi {
                        self.multi = true;
                        self.multi_seq = seq;
                    }
                    self.acc.extend(std::mem::take(&mut self.cur));
                } else if self.multi {
                    self.multi = false;
                    self.acc.extend(std::mem::take(&mut self.cur));
                    out.push(Packet {
                        seq: self.multi_seq,
                        payload: std::mem::take(&mut self.acc),
                    });
                } else {
                    out.push(Packet {
                        seq,
                        payload: std::mem::take(&mut self.cur),
                    });
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_small_packet() {
        let bytes = encode_packet(0, b"hello");
        assert_eq!(bytes[..4], [5, 0, 0, 0]);
        assert_eq!(&bytes[4..], b"hello");
    }

    #[test]
    fn test_encode_split_16m() {
        let payload = vec![7u8; MAX_PACKET_SIZE + 100];
        let mut seq = 0u8;
        let mut out = Vec::new();
        encode_frames(&mut seq, &payload, &mut out);
        // 第一包 0xFFFFFF + 4，第二包 100 + 4
        assert_eq!(out.len(), MAX_PACKET_SIZE + 4 + 100 + 4);
        assert_eq!(seq, 2);
    }

    #[test]
    fn test_encode_exact_16m_appends_empty() {
        let payload = vec![7u8; MAX_PACKET_SIZE];
        let mut seq = 0u8;
        let mut out = Vec::new();
        encode_frames(&mut seq, &payload, &mut out);
        assert_eq!(out.len(), MAX_PACKET_SIZE + 4 + 4);
        assert_eq!(&out[MAX_PACKET_SIZE + 4..MAX_PACKET_SIZE + 7], &[0, 0, 0]);
        assert_eq!(seq, 2);
    }

    #[test]
    fn test_decoder_simple() {
        let mut d = FrameDecoder::new();
        let mut input = encode_packet(0, b"select 1");
        input.extend_from_slice(&encode_packet(1, b"select 2"));
        let packets = d.feed(&input).unwrap();
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].seq, 0);
        assert_eq!(packets[0].payload, b"select 1");
        assert_eq!(packets[1].seq, 1);
        assert_eq!(packets[1].payload, b"select 2");
    }

    #[test]
    fn test_decoder_byte_by_byte() {
        let mut d = FrameDecoder::new();
        let input = encode_packet(3, b"abc");
        let mut got = Vec::new();
        for b in input {
            got.extend(d.feed(&[b]).unwrap());
        }
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].payload, b"abc");
        assert_eq!(got[0].seq, 3);
    }

    #[test]
    fn test_decoder_multi_packet_reassembly() {
        // 构造 >16MB 逻辑包：0xFFFFFF + 100 字节
        let payload = vec![9u8; MAX_PACKET_SIZE + 100];
        let mut seq = 0u8;
        let mut wire = Vec::new();
        encode_frames(&mut seq, &payload, &mut wire);

        let mut d = FrameDecoder::new();
        let packets = d.feed(&wire).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].seq, 0);
        assert_eq!(packets[0].payload.len(), payload.len());
    }

    #[test]
    fn test_decoder_exact_16m_with_trailing_empty() {
        let payload = vec![1u8; MAX_PACKET_SIZE];
        let mut seq = 0u8;
        let mut wire = Vec::new();
        encode_frames(&mut seq, &payload, &mut wire);

        let mut d = FrameDecoder::new();
        let packets = d.feed(&wire).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].payload.len(), MAX_PACKET_SIZE);
    }
}
