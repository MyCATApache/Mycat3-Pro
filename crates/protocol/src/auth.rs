//! 认证算法：mysql_native_password (scramble411) 与 caching_sha2_password。
//!
//! 移植自 Mycat3 `MysqlNativePasswordPluginUtil` / `SecurityUtil` / `CachingSha2PasswordPlugin`。

use rand::Rng;
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::Sha256;

/// 生成 20 字节认证种子（8 + 12）。
///
/// 与 Java 版 `nextSeedBuild()` 等价，但改进：Java 版使用原始随机字节，可能包含 NUL
/// 导致握手包截断；此处生成可打印 ASCII 范围 (0x21..=0x7e)，与官方 MySQL 服务端一致。
pub fn generate_seed() -> [u8; 20] {
    let mut rng = rand::thread_rng();
    let mut seed = [0u8; 20];
    for b in seed.iter_mut() {
        *b = rng.gen_range(0x21..=0x7e);
    }
    seed
}

fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(data);
    h.finalize().into()
}

/// mysql_native_password scramble：
/// `SHA1(password) XOR SHA1(seed || SHA1(SHA1(password)))`
pub fn scramble_native(password: &[u8], seed: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let pass1 = sha1(password);
    let pass2 = sha1(&pass1);
    let mut hasher = Sha1::new();
    hasher.update(seed);
    hasher.update(pass2);
    let seed_hash: [u8; 20] = hasher.finalize().into();
    pass1
        .iter()
        .zip(seed_hash.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// caching_sha2_password scramble：
/// `XOR(SHA256(password), SHA256(SHA256(SHA256(password)) || seed))`
pub fn scramble_caching_sha2(password: &[u8], seed: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let mut h = Sha256::new();
    h.update(password);
    let pass1: [u8; 32] = h.finalize().into();
    let pass2 = sha256(&pass1);
    let mut h = Sha256::new();
    h.update(pass2);
    h.update(seed);
    let seed_hash: [u8; 32] = h.finalize().into();
    pass1
        .iter()
        .zip(seed_hash.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize().into()
}

/// 校验客户端提交的 scramble 是否与配置密码匹配。
pub fn verify_native(client_response: &[u8], password: &str, seed: &[u8]) -> bool {
    if password.is_empty() {
        return client_response.is_empty();
    }
    constant_time_eq(client_response, &scramble_native(password.as_bytes(), seed))
}

pub fn verify_caching_sha2(client_response: &[u8], password: &str, seed: &[u8]) -> bool {
    if password.is_empty() {
        return client_response.is_empty();
    }
    constant_time_eq(
        client_response,
        &scramble_caching_sha2(password.as_bytes(), seed),
    )
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scramble411_known_vector() {
        // 与 Java SecurityUtil.scramble411 的算法一致性验证：
        // SHA1("root") = dc76e9f0c0006e8f919e0c515c66dbba3982f785
        let seed = b"12345678901234567890";
        let s = scramble_native(b"root", seed);
        assert_eq!(s.len(), 20);
        // 手工重算
        let pass1 = sha1(b"root");
        let pass2 = sha1(&pass1);
        let mut h = Sha1::new();
        h.update(seed);
        h.update(pass2);
        let sh: [u8; 20] = h.finalize().into();
        let expect: Vec<u8> = pass1.iter().zip(sh.iter()).map(|(a, b)| a ^ b).collect();
        assert_eq!(s, expect);
    }

    #[test]
    fn test_empty_password() {
        assert!(scramble_native(b"", b"seed").is_empty());
        assert!(scramble_caching_sha2(b"", b"seed").is_empty());
        assert!(verify_native(&[], "", b"seed"));
        assert!(!verify_native(&[1], "", b"seed"));
    }

    #[test]
    fn test_verify_roundtrip() {
        let seed = generate_seed();
        let resp = scramble_native(b"123456", &seed);
        assert!(verify_native(&resp, "123456", &seed));
        assert!(!verify_native(&resp, "654321", &seed));
    }

    #[test]
    fn test_caching_sha2_roundtrip() {
        let seed = generate_seed();
        let resp = scramble_caching_sha2(b"pass", &seed);
        assert!(verify_caching_sha2(&resp, "pass", &seed));
        assert!(!verify_caching_sha2(&resp, "Pass", &seed));
    }

    #[test]
    fn test_seed_printable() {
        for _ in 0..100 {
            let seed = generate_seed();
            assert!(seed.iter().all(|b| (0x21..=0x7e).contains(b)));
        }
    }
}
