//! Java `DateTimeFormatter`/`SimpleDateFormat` 模式到 chrono 格式的转换与日期解析。
//!
//! 支持 Mycat3 分片函数常用的模式：yyyy MM dd HH mm ss SSS。

use chrono::{NaiveDate, NaiveDateTime};

/// 将 Java 日期模式（如 "yyyy-MM-dd HH:mm:ss"）转换为 chrono 格式串（"%Y-%m-%d %H:%M:%S"）。
pub fn java_pattern_to_chrono(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() * 2);
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // 统计连续重复字母
        let mut run = 1;
        while i + run < chars.len() && chars[i + run] == c {
            run += 1;
        }
        match (c, run) {
            ('y', 2) => out.push_str("%y"),
            ('y', _) => out.push_str("%Y"),
            ('M', 3..) => out.push_str("%b"), // MMM → 缩写月名
            ('M', 2) => out.push_str("%m"),
            ('M', 1) => out.push_str("%m"),
            ('d', _) => out.push_str("%d"),
            ('H', _) => out.push_str("%H"),
            ('h', _) => out.push_str("%I"),
            ('m', _) => out.push_str("%M"),
            ('s', _) => out.push_str("%S"),
            ('S', _) => out.push_str("%f"),
            ('E', _) => out.push_str("%a"),
            _ => {
                for _ in 0..run {
                    out.push(c);
                }
            }
        }
        i += run;
    }
    out
}

/// 按一组 Java 模式解析日期时间（依次尝试）。
/// 兼容仅日期（补 00:00:00）与含时间两种输入。
pub fn parse_datetime(value: &str, patterns: &[String]) -> Option<NaiveDateTime> {
    let value = value.trim();
    for p in patterns {
        let fmt = java_pattern_to_chrono(p);
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, &fmt) {
            return Some(dt);
        }
        if let Ok(d) = NaiveDate::parse_from_str(value, &fmt) {
            return Some(d.and_hms_opt(0, 0, 0).unwrap());
        }
    }
    // Timestamp.valueOf 风格回退："yyyy-[m]m-[d]d hh:mm:ss[.f...]"
    let fallbacks = [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d",
    ];
    for f in fallbacks {
        if let Ok(dt) = NaiveDateTime::parse_from_str(value, f) {
            return Some(dt);
        }
        if let Ok(d) = NaiveDate::parse_from_str(value, f) {
            return Some(d.and_hms_opt(0, 0, 0).unwrap());
        }
    }
    None
}

/// 按一组 Java 模式解析日期。
pub fn parse_date(value: &str, patterns: &[String]) -> Option<NaiveDate> {
    parse_datetime(value, patterns).map(|dt| dt.date())
}

/// 将 csv 模式串（"yyyy-MM-dd,yyyyMMdd"）拆成模式列表。
pub fn split_patterns(date_format: &str) -> Vec<String> {
    date_format
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_convert() {
        assert_eq!(java_pattern_to_chrono("yyyy-MM-dd"), "%Y-%m-%d");
        assert_eq!(
            java_pattern_to_chrono("yyyy-MM-dd HH:mm:ss"),
            "%Y-%m-%d %H:%M:%S"
        );
        assert_eq!(java_pattern_to_chrono("yyyyMMdd"), "%Y%m%d");
        assert_eq!(java_pattern_to_chrono("yyyy/MM/dd"), "%Y/%m/%d");
    }

    #[test]
    fn test_parse() {
        let pats = split_patterns("yyyy-MM-dd");
        let dt = parse_datetime("2021-05-06", &pats).unwrap();
        assert_eq!(dt.year(), 2021);
        assert_eq!(dt.month(), 5);
        assert_eq!(dt.day(), 6);

        let pats = split_patterns("yyyy-MM-dd HH:mm:ss");
        let dt = parse_datetime("2021-05-06 07:08:09", &pats).unwrap();
        assert_eq!(dt.hour(), 7);

        // fallback 无模式匹配
        let dt = parse_datetime("20210506", &[]).unwrap();
        assert_eq!(dt.day(), 6);
    }

    use chrono::{Datelike, Timelike};
}
