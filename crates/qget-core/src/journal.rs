//! Resume journal encoding (DUR-01..05).
//!
//! Layout: an 8-byte magic followed by records of
//! `[len: u32 LE][crc32c(payload): u32 LE][payload]`.
//! Decoding stops at the first truncated or corrupt record; everything before
//! it is trusted and the writer truncates the file there before appending.
//! A record only ever describes data that was made durable (fdatasync) before
//! the record itself was written.

use crate::{ExpectedHash, IntervalSet, Validator};

pub const MAGIC: &[u8; 8] = b"QGETJNL\x01";
/// Upper bound on a single record, to reject garbage lengths quickly.
const MAX_RECORD: u32 = 64 << 20;

const TAG_HEADER: u8 = 1;
const TAG_RANGES: u8 = 2;

/// Identity of the download the journal belongs to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Header {
    pub size: u64,
    pub validator: Validator,
    pub url: String,
    pub expected: Option<ExpectedHash>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Header(Header),
    /// Byte ranges whose data is durable on disk.
    Ranges(Vec<(u64, u64)>),
}

/// Result of decoding a journal file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Decoded {
    pub header: Option<Header>,
    pub coverage: IntervalSet,
    /// Number of leading bytes that decoded cleanly; anything after is torn.
    pub valid_len: u64,
    /// Number of records successfully decoded.
    pub records: usize,
}

pub fn encode_record(rec: &Record) -> Vec<u8> {
    let mut payload = Vec::new();
    match rec {
        Record::Header(h) => {
            payload.push(TAG_HEADER);
            payload.extend_from_slice(&h.size.to_le_bytes());
            put_opt_str(&mut payload, h.validator.etag.as_deref());
            put_opt_str(&mut payload, h.validator.last_modified.as_deref());
            put_str(&mut payload, &h.url);
            put_opt_str(
                &mut payload,
                h.expected.as_ref().map(|e| e.to_string()).as_deref(),
            );
        }
        Record::Ranges(spans) => {
            payload.push(TAG_RANGES);
            payload.extend_from_slice(&(spans.len() as u32).to_le_bytes());
            for &(s, e) in spans {
                payload.extend_from_slice(&s.to_le_bytes());
                payload.extend_from_slice(&e.to_le_bytes());
            }
        }
    }
    let mut out = Vec::with_capacity(payload.len() + 8);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Encode a complete journal file (magic + records), used for compaction.
pub fn encode_file(header: &Header, coverage: &IntervalSet) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    out.extend(encode_record(&Record::Header(header.clone())));
    if !coverage.is_empty() {
        out.extend(encode_record(&Record::Ranges(coverage.spans().to_vec())));
    }
    out
}

pub fn decode(bytes: &[u8]) -> Decoded {
    let mut d = Decoded::default();
    if bytes.len() < MAGIC.len() || &bytes[..MAGIC.len()] != MAGIC {
        return d;
    }
    let mut pos = MAGIC.len();
    d.valid_len = pos as u64;
    while let Some((rec, next)) = decode_one(bytes, pos) {
        match rec {
            Record::Header(h) => {
                if d.header.is_some() {
                    // A second header is never written; treat as corruption.
                    break;
                }
                d.header = Some(h);
            }
            Record::Ranges(spans) => {
                if d.header.is_none() {
                    break;
                }
                let size = d.header.as_ref().map_or(0, |h| h.size);
                for (s, e) in spans {
                    if s < e && e <= size {
                        d.coverage.insert(s, e);
                    }
                }
            }
        }
        d.records += 1;
        pos = next;
        d.valid_len = pos as u64;
    }
    d
}

fn decode_one(bytes: &[u8], pos: usize) -> Option<(Record, usize)> {
    let len = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?);
    if len == 0 || len > MAX_RECORD {
        return None;
    }
    let crc = u32::from_le_bytes(bytes.get(pos + 4..pos + 8)?.try_into().ok()?);
    let payload = bytes.get(pos + 8..pos + 8 + len as usize)?;
    if crc32c::crc32c(payload) != crc {
        return None;
    }
    let mut r = Reader { buf: payload };
    let rec = match r.u8()? {
        TAG_HEADER => {
            let size = r.u64()?;
            let etag = r.opt_str()?;
            let last_modified = r.opt_str()?;
            let url = r.str()?;
            let expected = match r.opt_str()? {
                Some(s) => Some(ExpectedHash::parse(&s)?),
                None => None,
            };
            Record::Header(Header {
                size,
                validator: Validator {
                    etag,
                    last_modified,
                },
                url,
                expected,
            })
        }
        TAG_RANGES => {
            let n = r.u32()?;
            let mut spans = Vec::with_capacity(n.min(1 << 16) as usize);
            for _ in 0..n {
                spans.push((r.u64()?, r.u64()?));
            }
            Record::Ranges(spans)
        }
        _ => return None,
    };
    if !r.buf.is_empty() {
        return None;
    }
    Some((rec, pos + 8 + len as usize))
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn put_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        Some(s) => {
            out.push(1);
            put_str(out, s);
        }
        None => out.push(0),
    }
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.buf.len() < n {
            return None;
        }
        let (a, b) = self.buf.split_at(n);
        self.buf = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }
    fn str(&mut self) -> Option<String> {
        let n = self.u32()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }
    fn opt_str(&mut self) -> Option<Option<String>> {
        match self.u8()? {
            0 => Some(None),
            1 => Some(Some(self.str()?)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn header() -> Header {
        Header {
            size: 1000,
            validator: Validator {
                etag: Some("\"abc\"".into()),
                last_modified: None,
            },
            url: "https://example.com/f".into(),
            expected: ExpectedHash::parse(&"a".repeat(64)),
        }
    }

    #[test]
    fn roundtrip() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(encode_record(&Record::Header(header())));
        bytes.extend(encode_record(&Record::Ranges(vec![(0, 100), (200, 300)])));
        bytes.extend(encode_record(&Record::Ranges(vec![(100, 200)])));
        let d = decode(&bytes);
        assert_eq!(d.header, Some(header()));
        assert_eq!(d.coverage.spans(), &[(0, 300)]);
        assert_eq!(d.valid_len, bytes.len() as u64);
        assert_eq!(d.records, 3);
    }

    #[test]
    fn torn_tail_is_ignored() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(encode_record(&Record::Header(header())));
        let good = bytes.len();
        bytes.extend(encode_record(&Record::Ranges(vec![(0, 100)])));
        for cut in good..bytes.len() {
            let d = decode(&bytes[..cut]);
            assert_eq!(d.valid_len, good as u64);
            assert!(d.coverage.is_empty());
        }
        // Flip a payload bit: CRC must reject it.
        let mut corrupt = bytes.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0x40;
        assert!(decode(&corrupt).coverage.is_empty());
    }

    #[test]
    fn out_of_bounds_ranges_are_dropped() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend(encode_record(&Record::Header(header())));
        bytes.extend(encode_record(&Record::Ranges(vec![
            (0, 10),
            (900, 2000),
            (50, 40),
        ])));
        assert_eq!(decode(&bytes).coverage.spans(), &[(0, 10)]);
    }

    #[test]
    fn compaction_roundtrip() {
        let cov = IntervalSet::from_spans([(0, 10), (20, 30)]);
        let d = decode(&encode_file(&header(), &cov));
        assert_eq!(d.coverage, cov);
        assert_eq!(d.header, Some(header()));
    }

    proptest! {
        #[test]
        fn decoder_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let mut input = MAGIC.to_vec();
            input.extend(bytes);
            let d = decode(&input);
            prop_assert!(d.valid_len as usize <= input.len());
        }
    }
}
