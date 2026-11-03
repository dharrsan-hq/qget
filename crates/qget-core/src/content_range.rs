//! Strict `Content-Range` parsing (RFC 9110 §14.4).

/// A parsed `Content-Range: bytes first-last/complete` header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContentRange {
    /// First byte position (inclusive).
    pub first: u64,
    /// Last byte position (inclusive).
    pub last: u64,
    /// Complete length, if the server sent one (`*` means unknown).
    pub complete: Option<u64>,
}

/// A `Content-Range: bytes */complete` header, as sent with 416 responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unsatisfied {
    pub complete: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parsed {
    Range(ContentRange),
    Unsatisfied(Unsatisfied),
}

/// Parse a `Content-Range` header value. Returns `None` on any malformed input.
pub fn parse(value: &str) -> Option<Parsed> {
    let rest = value.trim().strip_prefix("bytes")?;
    let rest = rest.strip_prefix(' ')?.trim_start();
    let (spec, complete) = rest.split_once('/')?;
    let complete = complete.trim();
    let complete = if complete == "*" {
        None
    } else {
        Some(parse_u64(complete)?)
    };
    if spec.trim() == "*" {
        return Some(Parsed::Unsatisfied(Unsatisfied {
            complete: complete?,
        }));
    }
    let (first, last) = spec.trim().split_once('-')?;
    let first = parse_u64(first)?;
    let last = parse_u64(last)?;
    if last < first {
        return None;
    }
    if let Some(c) = complete {
        if last >= c {
            return None;
        }
    }
    Some(Parsed::Range(ContentRange {
        first,
        last,
        complete,
    }))
}

fn parse_u64(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

impl ContentRange {
    /// Whether this response covers exactly `[start, end)` of a resource of `size` bytes.
    pub fn matches(&self, start: u64, end: u64, size: u64) -> bool {
        self.first == start && self.last + 1 == end && self.complete.is_none_or(|c| c == size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parses_valid_headers() {
        assert_eq!(
            parse("bytes 0-99/1000"),
            Some(Parsed::Range(ContentRange {
                first: 0,
                last: 99,
                complete: Some(1000)
            }))
        );
        assert_eq!(
            parse("bytes 5-5/*"),
            Some(Parsed::Range(ContentRange {
                first: 5,
                last: 5,
                complete: None
            }))
        );
        assert_eq!(
            parse("bytes */1000"),
            Some(Parsed::Unsatisfied(Unsatisfied { complete: 1000 }))
        );
    }

    #[test]
    fn rejects_malformed_headers() {
        for bad in [
            "",
            "bytes",
            "bytes 0-99",
            "bytes 99-0/1000",
            "bytes 0-1000/1000",
            "bytes -5-10/100",
            "bytes 0-+9/100",
            "items 0-9/100",
            "bytes 0-9/abc",
            "bytes */*",
        ] {
            assert_eq!(parse(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn matches_requested_range() {
        let Some(Parsed::Range(cr)) = parse("bytes 100-199/1000") else {
            panic!()
        };
        assert!(cr.matches(100, 200, 1000));
        assert!(!cr.matches(100, 201, 1000));
        assert!(!cr.matches(100, 200, 999));
    }

    proptest! {
        #[test]
        fn never_panics(s in ".{0,40}") {
            let _ = parse(&s);
        }

        #[test]
        fn roundtrip(first in 0u64..1<<40, len in 1u64..1<<20, extra in 0u64..1<<20) {
            let last = first + len - 1;
            let complete = last + 1 + extra;
            let header = format!("bytes {first}-{last}/{complete}");
            prop_assert_eq!(parse(&header), Some(Parsed::Range(ContentRange { first, last, complete: Some(complete) })));
        }
    }
}
