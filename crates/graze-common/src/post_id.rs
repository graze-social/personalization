//! Date-sharded post identifiers for URI interning.
//!
//! New IDs are `{YYYYMMDD}{seq:010}` (18 ASCII digits, no separators) so they are
//! safe inside Redis keys like `pl:{post_id}:{YYYYMMDD}`.

/// Length of the date prefix (`YYYYMMDD`).
pub const DATE_LEN: usize = 8;
/// Length of the daily sequence suffix.
pub const SEQ_LEN: usize = 10;
/// Total length of a dated post ID.
pub const DATED_ID_LEN: usize = DATE_LEN + SEQ_LEN;

/// Build a dated post ID from intern date and daily sequence.
#[inline]
pub fn format_post_id(date: &str, seq: u64) -> String {
    format!("{}{:0width$}", date, seq, width = SEQ_LEN)
}

/// True if `id` is a dated post ID (`YYYYMMDD` + 10-digit seq).
#[inline]
pub fn is_dated(id: &str) -> bool {
    if id.len() != DATED_ID_LEN {
        return false;
    }
    id.chars().all(|c| c.is_ascii_digit())
        && parse_date_prefix(id).is_some()
}

/// True if `id` is a legacy global monotonic integer string.
#[inline]
pub fn is_legacy_numeric(id: &str) -> bool {
    !id.is_empty()
        && id.len() < DATED_ID_LEN
        && id.chars().all(|c| c.is_ascii_digit())
}

/// Extract `YYYYMMDD` from a dated post ID.
#[inline]
pub fn intern_date_from_post_id(id: &str) -> Option<&str> {
    if !is_dated(id) {
        return None;
    }
    Some(&id[..DATE_LEN])
}

fn parse_date_prefix(id: &str) -> Option<()> {
    if id.len() < DATE_LEN {
        return None;
    }
    let y: u32 = id[0..4].parse().ok()?;
    let m: u32 = id[4..6].parse().ok()?;
    let d: u32 = id[6..8].parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || y < 1970 {
        return None;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_and_parse_dated_id() {
        let id = format_post_id("20260513", 42);
        assert_eq!(id, "202605130000000042");
        assert!(is_dated(&id));
        assert_eq!(intern_date_from_post_id(&id), Some("20260513"));
    }

    #[test]
    fn legacy_numeric() {
        assert!(is_legacy_numeric("184729301"));
        assert!(!is_legacy_numeric("202605130000000042"));
    }
}
