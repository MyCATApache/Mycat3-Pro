//! 哈希工具：与 Java 版逐位一致的哈希实现。
//!
//! - `java_string_hash`：`String.hashCode()`（h*31+c，UTF-16 码元，i32 回绕）
//! - `wang_hash`：Thomas Wang 整数哈希（`PartitionByHashMod.hash`）
//! - `murmur3_32`：Guava `Hashing.murmur3_32(seed)`，`hash_unencoded_chars`
//!   对应 `hashUnencodedChars`（每字符按 2 字节小端喂入）
//! - `jump_consistent_hash`：Google Jump Consistent Hash
//!   （`PartitionByJumpConsistentHash` 的精确移植）
//! - `crc32`：IEEE CRC-32（`PureJavaCrc32` 等价）

/// Java `String.hashCode()`：对 UTF-16 码元计算 `h = h*31 + c`（i32 回绕）。
pub fn java_string_hash(s: &str) -> i32 {
    let mut h: i32 = 0;
    for c in s.encode_utf16() {
        h = h.wrapping_mul(31).wrapping_add(c as i32);
    }
    h
}

/// 带自定义种子的字符串哈希（`AutoFunctionFactory.hashCode(value, randSeed)`）。
pub fn java_string_hash_seed(s: &str, seed: i32) -> i32 {
    let mut h: i32 = 0;
    for c in s.encode_utf16() {
        h = h.wrapping_mul(seed).wrapping_add(c as i32);
    }
    h
}

/// 子串哈希 `StringUtil.hash(key, start, end)`：对 s[start..end) 的字符做 31 进制哈希。
pub fn string_util_hash(s: &str, start: usize, end: usize) -> i64 {
    let chars: Vec<char> = s.chars().collect();
    let end = end.min(chars.len());
    let mut h: i64 = 0;
    for c in &chars[start.min(end)..end] {
        h = h.wrapping_mul(31).wrapping_add(*c as i64);
    }
    h
}

/// Thomas Wang / Jenkins 整数哈希。移植自 `PartitionByHashMod.hash(int)`。
pub fn wang_hash(key: i32) -> i32 {
    let mut k = key;
    k = (!k).wrapping_add(k << 21); // key = (key << 21) - key - 1
    k ^= k >> 24; // Java >> 为算术右移
    k = k.wrapping_add(k << 3).wrapping_add(k << 8); // key * 265
    k ^= k >> 14;
    k = k.wrapping_add(k << 2).wrapping_add(k << 4); // key * 21
    k ^= k >> 28;
    k = k.wrapping_add(k << 31);
    k
}

const MURMUR_C1: u32 = 0xcc9e_2d51;
const MURMUR_C2: u32 = 0x1b87_3593;

/// Guava 兼容 murmur3_32（little-endian 字节流）。
pub fn murmur3_32(seed: u32, data: &[u8]) -> u32 {
    let mut h1 = seed;
    let mut chunks = data.chunks_exact(4);
    for c in &mut chunks {
        let mut k1 = u32::from_le_bytes([c[0], c[1], c[2], c[3]]);
        k1 = k1.wrapping_mul(MURMUR_C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(MURMUR_C2);
        h1 ^= k1;
        h1 = h1.rotate_left(13);
        h1 = h1.wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    // tail
    let tail = chunks.remainder();
    if !tail.is_empty() {
        let mut k1 = 0u32;
        for (i, b) in tail.iter().enumerate() {
            k1 |= (*b as u32) << (8 * i);
        }
        k1 = k1.wrapping_mul(MURMUR_C1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(MURMUR_C2);
        h1 ^= k1;
    }
    // finalization
    h1 ^= data.len() as u32;
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;
    h1
}

/// Guava `murmur3_32(seed).hashUnencodedChars(s).asInt()`：
/// 字符串按 UTF-16 码元、每码元 2 字节小端参与哈希。
pub fn murmur3_32_unencoded_chars(seed: u32, s: &str) -> i32 {
    let mut bytes = Vec::with_capacity(s.len() * 2);
    for c in s.encode_utf16() {
        bytes.extend_from_slice(&c.to_le_bytes());
    }
    murmur3_32(seed, &bytes) as i32
}

/// Google Jump Consistent Hash。
/// 精确移植 `PartitionByJumpConsistentHash.jumpConsistentHash(long key, int buckets)`。
pub fn jump_consistent_hash(key: i64, buckets: i32) -> i32 {
    const CONSTANT: i64 = 2862933555777941757u64 as i64; // wrapping
    const JUMP: f64 = 2147483648.0; // 1L << 31
    let mut k = key;
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < buckets as i64 {
        b = j;
        k = k.wrapping_mul(CONSTANT).wrapping_add(1);
        // Java: (k >>> 33) + 1L，toDouble 做无符号转 double
        let x = ((k as u64) >> 33).wrapping_add(1) as f64;
        j = ((b + 1) as f64 * (JUMP / x)) as i64;
    }
    b as i32
}

/// CRC-32 (IEEE)，等价 `PureJavaCrc32HashFunction.hash(String.getBytes())`。
pub fn crc32(s: &str) -> u64 {
    crc32fast::hash(s.as_bytes()) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_java_string_hash() {
        // 已知向量："root".hashCode() = 3499364，"hello".hashCode() = 99162322
        assert_eq!(java_string_hash("root"), 3506402);
        assert_eq!(java_string_hash("hello"), 99162322);
        assert_eq!(java_string_hash(""), 0);
        assert_eq!(java_string_hash("a"), 97);
    }

    #[test]
    fn test_wang_hash_deterministic() {
        assert_eq!(wang_hash(12345), wang_hash(12345));
        assert_ne!(wang_hash(1), wang_hash(2));
    }

    #[test]
    fn test_murmur3_known_vector() {
        // murmur3_32(seed=0, "") = 0
        assert_eq!(murmur3_32(0, b""), 0);
        // murmur3_32(seed=0, "test") 的 Guava 已知值 = 0xba6bd213
        assert_eq!(murmur3_32(0, b"test"), 0xba6b_d213);
        // murmur3_32(seed=0, "Hello") = 0x12da77c8（Python 独立实现验证）
        assert_eq!(murmur3_32(0, b"Hello"), 0x12da_77c8);
        assert_eq!(murmur3_32(0, b"foo"), 0xf6a5_c420);
        assert_eq!(murmur3_32(0, b"bar"), 0x450e_998d);
    }

    #[test]
    fn test_murmur3_unencoded_chars_ascii() {
        // ASCII 字符串 hashUnencodedChars 每字符后跟 0 字节
        let h = murmur3_32_unencoded_chars(0, "ab");
        let manual = murmur3_32(0, &[b'a', 0, b'b', 0]);
        assert_eq!(h as u32, manual);
    }

    #[test]
    fn test_jump_consistent_hash() {
        // Google 论文/常见实现的已知向量
        assert_eq!(jump_consistent_hash(1, 1), 0);
        assert_eq!(jump_consistent_hash(42, 1), 0);
        // key=-1? 分布性：多桶下结果在界内且确定
        for buckets in [2, 3, 8, 100] {
            for key in [0i64, 1, -1, 123456789, i64::MAX] {
                let r = jump_consistent_hash(key, buckets);
                assert!(
                    (0..buckets).contains(&r),
                    "key={} buckets={} -> {}",
                    key,
                    buckets,
                    r
                );
            }
        }
        // 一致性：桶数增加时大部分 key 不迁移
        let mut stay = 0;
        for k in 0..1000i64 {
            if jump_consistent_hash(k, 10) == jump_consistent_hash(k, 11) {
                stay += 1;
            }
        }
        assert!(stay > 800, "only {} of 1000 keys stayed", stay);
    }

    #[test]
    fn test_crc32() {
        // CRC-32("123456789") = 0xCBF43926
        assert_eq!(crc32("123456789"), 0xCBF4_3926);
    }

    #[test]
    fn test_string_util_hash() {
        // "abc": 97*31^2 + 98*31 + 99 = 96354
        assert_eq!(string_util_hash("abc", 0, 3), 96354);
        // Java String.hashCode("abc") 相同
        assert_eq!(java_string_hash("abc") as i64, 96354);
    }
}
