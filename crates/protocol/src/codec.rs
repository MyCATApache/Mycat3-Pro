//! 载荷读写工具：length-encoded integer / string、定长整数等。
//!
//! 移植自 Mycat3 `MySQLPayloadWriter` / `MySQLPacket` / `ReadView` 的编解码逻辑。

use crate::error::{ProtocolError, ProtocolResult};

/// 载荷读取器（大端无关，MySQL 全部小端）。
#[derive(Debug, Clone)]
pub struct PacketReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> PacketReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        PacketReader { buf, pos: 0 }
    }

    #[inline]
    pub fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn require(&self, n: usize) -> ProtocolResult<()> {
        if self.remaining() < n {
            Err(ProtocolError::need_got(n, self.remaining()))
        } else {
            Ok(())
        }
    }

    pub fn read_u8(&mut self) -> ProtocolResult<u8> {
        self.require(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn peek_u8(&self) -> ProtocolResult<u8> {
        self.require(1)?;
        Ok(self.buf[self.pos])
    }

    pub fn read_int2(&mut self) -> ProtocolResult<u16> {
        self.require(2)?;
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn read_int3(&mut self) -> ProtocolResult<u32> {
        self.require(3)?;
        let v = u32::from_le_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            0,
        ]);
        self.pos += 3;
        Ok(v)
    }

    pub fn read_int4(&mut self) -> ProtocolResult<u32> {
        self.require(4)?;
        let v = u32::from_le_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    pub fn read_int8(&mut self) -> ProtocolResult<u64> {
        self.require(8)?;
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.buf[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_le_bytes(b))
    }

    pub fn read_fixed(&mut self, n: usize) -> ProtocolResult<&'a [u8]> {
        self.require(n)?;
        let v = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(v)
    }

    /// 读取 length-encoded integer。
    pub fn read_lenenc_int(&mut self) -> ProtocolResult<u64> {
        let first = self.read_u8()?;
        match first {
            0xfb => Ok(u64::MAX), // NULL 标记（部分实现用作 NULL）
            0xfc => Ok(self.read_int2()? as u64),
            0xfd => Ok(self.read_int3()? as u64),
            0xfe => self.read_int8(),
            n => Ok(n as u64),
        }
    }

    /// 读取 length-encoded 字节串。返回 None 表示 NULL (0xfb 前缀)。
    pub fn read_lenenc_bytes(&mut self) -> ProtocolResult<Option<&'a [u8]>> {
        self.require(1)?;
        if self.buf[self.pos] == 0xfb && self.remaining() >= 1 {
            // 0xfb 单独出现表示 NULL
            self.pos += 1;
            return Ok(None);
        }
        let len = self.read_lenenc_int()? as usize;
        Ok(Some(self.read_fixed(len)?))
    }

    /// 读取以 NUL 结尾的字符串（C-string）。
    pub fn read_null_str(&mut self) -> ProtocolResult<&'a str> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        self.require(1)?; // NUL
        let s = std::str::from_utf8(&self.buf[start..self.pos])
            .map_err(|_| ProtocolError::InvalidUtf8)?;
        self.pos += 1;
        Ok(s)
    }

    /// 读取到载荷末尾的字节（EOF-string bytes）。
    pub fn read_eof_bytes(&mut self) -> &'a [u8] {
        let v = &self.buf[self.pos..];
        self.pos = self.buf.len();
        v
    }

    /// 读取到载荷末尾的 UTF-8 字符串。
    pub fn read_eof_str(&mut self) -> ProtocolResult<&'a str> {
        let bytes = self.read_eof_bytes();
        // 兼容尾部可能存在的 NUL
        let bytes = if bytes.last() == Some(&0) {
            &bytes[..bytes.len() - 1]
        } else {
            bytes
        };
        std::str::from_utf8(bytes).map_err(|_| ProtocolError::InvalidUtf8)
    }

    pub fn skip(&mut self, n: usize) -> ProtocolResult<()> {
        self.require(n)?;
        self.pos += n;
        Ok(())
    }
}

/// 载荷写入器。移植自 `MySQLPayloadWriter`。
#[derive(Debug, Default, Clone)]
pub struct PacketWriter {
    buf: Vec<u8>,
}

impl PacketWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        PacketWriter {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn write_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn write_int2(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_int3(&mut self, v: u32) {
        self.buf
            .extend_from_slice(&(v & 0x00FF_FFFF).to_le_bytes()[..3]);
    }

    pub fn write_int4(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_int8(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn write_fixed(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }

    pub fn write_zeros(&mut self, n: usize) {
        self.buf.resize(self.buf.len() + n, 0);
    }

    /// 写 length-encoded integer。等价 `MySQLPayloadWriter.writeLenencInt`。
    pub fn write_lenenc_int(&mut self, v: u64) {
        if v < 251 {
            self.buf.push(v as u8);
        } else if v < (1 << 16) {
            self.buf.push(0xfc);
            self.write_int2(v as u16);
        } else if v < (1 << 24) {
            self.buf.push(0xfd);
            self.write_int3(v as u32);
        } else {
            self.buf.push(0xfe);
            self.write_int8(v);
        }
    }

    /// 写 length-encoded 字节串。
    pub fn write_lenenc_bytes(&mut self, v: &[u8]) {
        self.write_lenenc_int(v.len() as u64);
        self.buf.extend_from_slice(v);
    }

    /// 写 NUL 结尾字符串。
    pub fn write_null_str(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
        self.buf.push(0);
    }

    /// 写剩余字节（EOF-string，不带长度前缀）。
    pub fn write_eof_str(&mut self, s: &str) {
        self.buf.extend_from_slice(s.as_bytes());
    }

    /// 在指定偏移写入 u16（用于回填长度等）。
    pub fn write_int2_at(&mut self, pos: usize, v: u16) {
        let b = v.to_le_bytes();
        self.buf[pos] = b[0];
        self.buf[pos + 1] = b[1];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lenenc_int_roundtrip() {
        for v in [
            0u64,
            1,
            250,
            251,
            65535,
            65536,
            1 << 24,
            (1 << 24) + 1,
            u64::MAX - 1,
        ] {
            let mut w = PacketWriter::new();
            w.write_lenenc_int(v);
            let bytes = w.into_bytes();
            let mut r = PacketReader::new(&bytes);
            assert_eq!(r.read_lenenc_int().unwrap(), v, "value {v}");
            assert!(r.is_empty());
        }
    }

    #[test]
    fn test_lenenc_int_encoding_sizes() {
        let mut w = PacketWriter::new();
        w.write_lenenc_int(250);
        assert_eq!(w.len(), 1);
        let mut w = PacketWriter::new();
        w.write_lenenc_int(0xfc00);
        assert_eq!(w.len(), 3);
        let mut w = PacketWriter::new();
        w.write_lenenc_int(0xfd_0000);
        assert_eq!(w.len(), 4);
        let mut w = PacketWriter::new();
        w.write_lenenc_int(0xfe_0000_0000);
        assert_eq!(w.len(), 9);
    }

    #[test]
    fn test_null_str_and_eof_str() {
        let mut w = PacketWriter::new();
        w.write_null_str("root");
        w.write_eof_str("select 1");
        let bytes = w.into_bytes();
        let mut r = PacketReader::new(&bytes);
        assert_eq!(r.read_null_str().unwrap(), "root");
        assert_eq!(r.read_eof_str().unwrap(), "select 1");
    }

    #[test]
    fn test_lenenc_bytes_null() {
        let mut w = PacketWriter::new();
        w.write_lenenc_bytes(b"abc");
        let bytes = w.into_bytes();
        let mut r = PacketReader::new(&bytes);
        assert_eq!(r.read_lenenc_bytes().unwrap(), Some(&b"abc"[..]));

        let mut r = PacketReader::new(&[0xfb]);
        assert_eq!(r.read_lenenc_bytes().unwrap(), None);
    }

    #[test]
    fn test_int24() {
        let mut w = PacketWriter::new();
        w.write_int3(0x00FF_FFFF);
        let bytes = w.into_bytes();
        assert_eq!(bytes.len(), 3);
        let mut r = PacketReader::new(&bytes);
        assert_eq!(r.read_int3().unwrap(), 0x00FF_FFFF);
    }

    #[test]
    fn test_unexpected_eof() {
        let mut r = PacketReader::new(&[1, 2]);
        assert!(r.read_int4().is_err());
    }
}
