//! Typed property values and their binary encoding.
//!
//! Property documents are `Record`s: ordered, named fields whose values carry explicit types
//! (REQ-0009). Integers are 64-bit and decimals are decimal128, so nothing round-trips through
//! a lossy float the way plain JSON numbers do. Duplicate field names are rejected wherever a
//! record is built or decoded (REQ-0011).

// reqforge: implements REQ-0009
// reqforge: implements REQ-0011

use std::fmt;

/// Deepest nesting of lists and records the codec accepts. REQ-0009 AC2 requires at least 100.
pub const MAX_DEPTH: usize = 128;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Double(f64),
    Decimal(Decimal128),
    Timestamp(Timestamp),
    String(String),
    Binary(Vec<u8>),
    List(Vec<Value>),
    Record(Record),
}

/// A decimal128 value: `coefficient × 10^exponent` with at most 34 significant digits and the
/// IEEE 754 decimal128 exponent range. Stored exactly; equality is representation equality, so
/// `1.0` and `1.00` are distinct values, as in IEEE 754.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128 {
    coefficient: i128,
    exponent: i16,
}

impl Decimal128 {
    pub const MAX_COEFFICIENT: i128 = 10i128.pow(34) - 1;
    pub const MIN_EXPONENT: i16 = -6176;
    pub const MAX_EXPONENT: i16 = 6111;

    pub fn new(coefficient: i128, exponent: i16) -> Result<Self, ValueError> {
        if coefficient.unsigned_abs() > Self::MAX_COEFFICIENT as u128 {
            return Err(ValueError::DecimalOutOfRange);
        }
        if !(Self::MIN_EXPONENT..=Self::MAX_EXPONENT).contains(&exponent) {
            return Err(ValueError::DecimalOutOfRange);
        }
        Ok(Self {
            coefficient,
            exponent,
        })
    }

    pub fn coefficient(&self) -> i128 {
        self.coefficient
    }

    pub fn exponent(&self) -> i16 {
        self.exponent
    }

    /// Parse plain or scientific notation, e.g. `-12.340`, `1E-6176`, `9.99e10`.
    pub fn parse(s: &str) -> Result<Self, ValueError> {
        let bad = || ValueError::InvalidDecimal(s.to_string());
        let (mantissa, exp) = match s.find(['e', 'E']) {
            Some(i) => (&s[..i], s[i + 1..].parse::<i32>().map_err(|_| bad())?),
            None => (s, 0),
        };
        let (negative, digits) = match mantissa.as_bytes().first() {
            Some(b'-') => (true, &mantissa[1..]),
            Some(b'+') => (false, &mantissa[1..]),
            _ => (false, mantissa),
        };
        let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
        let all: String = [int_part, frac_part].concat();
        if all.is_empty() || !all.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        let trimmed = all.trim_start_matches('0');
        if trimmed.len() > 34 {
            return Err(ValueError::DecimalOutOfRange);
        }
        let magnitude: i128 = if trimmed.is_empty() {
            0
        } else {
            trimmed.parse().map_err(|_| bad())?
        };
        let exponent = exp - frac_part.len() as i32;
        let exponent = i16::try_from(exponent).map_err(|_| ValueError::DecimalOutOfRange)?;
        Self::new(if negative { -magnitude } else { magnitude }, exponent)
    }
}

impl fmt::Display for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}E{}", self.coefficient, self.exponent)
    }
}

/// An instant with its UTC offset (GQL ZONED DATETIME): microseconds since the Unix epoch in
/// UTC, plus the offset in minutes that the value was written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Timestamp {
    pub micros_utc: i64,
    pub offset_minutes: i16,
}

impl Timestamp {
    pub fn new(micros_utc: i64, offset_minutes: i16) -> Result<Self, ValueError> {
        if !(-18 * 60..=18 * 60).contains(&offset_minutes) {
            return Err(ValueError::InvalidTimestampOffset(offset_minutes));
        }
        Ok(Self {
            micros_utc,
            offset_minutes,
        })
    }
}

/// Ordered named fields with unique names (REQ-0011).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Record {
    fields: Vec<(String, Value)>,
}

impl Record {
    pub fn new(fields: Vec<(String, Value)>) -> Result<Self, ValueError> {
        let mut seen = std::collections::HashSet::with_capacity(fields.len());
        for (name, _) in &fields {
            if !seen.insert(name.as_str()) {
                return Err(ValueError::DuplicateKey(name.clone()));
            }
        }
        Ok(Self { fields })
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub fn fields(&self) -> &[(String, Value)] {
        &self.fields
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueError {
    DuplicateKey(String),
    DecimalOutOfRange,
    InvalidDecimal(String),
    InvalidTimestampOffset(i16),
    TooDeep,
    Truncated,
    TrailingBytes,
    UnknownTag(u8),
    InvalidUtf8,
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKey(k) => write!(f, "duplicate property name `{k}`"),
            Self::DecimalOutOfRange => f.write_str("decimal outside the decimal128 range"),
            Self::InvalidDecimal(s) => write!(f, "invalid decimal literal `{s}`"),
            Self::InvalidTimestampOffset(m) => {
                write!(f, "UTC offset {m} minutes is outside ±18 hours")
            }
            Self::TooDeep => write!(f, "value nested deeper than {MAX_DEPTH} levels"),
            Self::Truncated => f.write_str("encoded value is truncated"),
            Self::TrailingBytes => f.write_str("encoded value has trailing bytes"),
            Self::UnknownTag(t) => write!(f, "unknown value tag {t:#04x}"),
            Self::InvalidUtf8 => f.write_str("string is not valid UTF-8"),
        }
    }
}

impl std::error::Error for ValueError {}

// ---------------------------------------------------------------------------------------------
// Binary encoding: one tag byte, then a fixed-width or length-prefixed payload. Lengths and
// counts are LEB128 varints. Fixed-width integers are little-endian.

const T_NULL: u8 = 0x00;
const T_FALSE: u8 = 0x01;
const T_TRUE: u8 = 0x02;
const T_INT64: u8 = 0x03;
const T_DOUBLE: u8 = 0x04;
const T_DECIMAL: u8 = 0x05;
const T_TIMESTAMP: u8 = 0x06;
const T_STRING: u8 = 0x07;
const T_BINARY: u8 = 0x08;
const T_LIST: u8 = 0x09;
const T_RECORD: u8 = 0x0a;

fn varint_len(mut n: u64) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

impl Value {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut out);
        out
    }

    /// Exact size of `encode()`, without encoding.
    pub fn encoded_len(&self) -> usize {
        1 + match self {
            Value::Null | Value::Bool(_) => 0,
            Value::Int64(_) | Value::Double(_) => 8,
            Value::Decimal(_) => 16 + 2,
            Value::Timestamp(_) => 8 + 2,
            Value::String(s) => varint_len(s.len() as u64) + s.len(),
            Value::Binary(b) => varint_len(b.len() as u64) + b.len(),
            Value::List(items) => {
                varint_len(items.len() as u64) + items.iter().map(Value::encoded_len).sum::<usize>()
            }
            Value::Record(r) => r.encoded_body_len(),
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            Value::Null => out.push(T_NULL),
            Value::Bool(false) => out.push(T_FALSE),
            Value::Bool(true) => out.push(T_TRUE),
            Value::Int64(n) => {
                out.push(T_INT64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Value::Double(x) => {
                out.push(T_DOUBLE);
                out.extend_from_slice(&x.to_bits().to_le_bytes());
            }
            Value::Decimal(d) => {
                out.push(T_DECIMAL);
                out.extend_from_slice(&d.coefficient.to_le_bytes());
                out.extend_from_slice(&d.exponent.to_le_bytes());
            }
            Value::Timestamp(t) => {
                out.push(T_TIMESTAMP);
                out.extend_from_slice(&t.micros_utc.to_le_bytes());
                out.extend_from_slice(&t.offset_minutes.to_le_bytes());
            }
            Value::String(s) => {
                out.push(T_STRING);
                put_varint(out, s.len() as u64);
                out.extend_from_slice(s.as_bytes());
            }
            Value::Binary(b) => {
                out.push(T_BINARY);
                put_varint(out, b.len() as u64);
                out.extend_from_slice(b);
            }
            Value::List(items) => {
                out.push(T_LIST);
                put_varint(out, items.len() as u64);
                for item in items {
                    item.encode_into(out);
                }
            }
            Value::Record(r) => {
                out.push(T_RECORD);
                r.encode_body(out);
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Value, ValueError> {
        let mut r = Reader { bytes, pos: 0 };
        let v = r.value(0)?;
        if r.pos != bytes.len() {
            return Err(ValueError::TrailingBytes);
        }
        Ok(v)
    }
}

impl Record {
    pub fn encode(&self) -> Vec<u8> {
        Value::Record(self.clone()).encode()
    }

    /// Size of this record encoded as a property document.
    pub fn encoded_len(&self) -> usize {
        1 + self.encoded_body_len()
    }

    fn encoded_body_len(&self) -> usize {
        varint_len(self.fields.len() as u64)
            + self
                .fields
                .iter()
                .map(|(k, v)| varint_len(k.len() as u64) + k.len() + v.encoded_len())
                .sum::<usize>()
    }

    fn encode_body(&self, out: &mut Vec<u8>) {
        put_varint(out, self.fields.len() as u64);
        for (k, v) in &self.fields {
            put_varint(out, k.len() as u64);
            out.extend_from_slice(k.as_bytes());
            v.encode_into(out);
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Record, ValueError> {
        match Value::decode(bytes)? {
            Value::Record(r) => Ok(r),
            _ => Err(ValueError::UnknownTag(bytes.first().copied().unwrap_or(0))),
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], ValueError> {
        let end = self.pos.checked_add(n).ok_or(ValueError::Truncated)?;
        let s = self.bytes.get(self.pos..end).ok_or(ValueError::Truncated)?;
        self.pos = end;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ValueError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }

    fn varint(&mut self) -> Result<u64, ValueError> {
        let mut n = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.take(1)?[0];
            n |= u64::from(b & 0x7f) << shift;
            if b < 0x80 {
                return Ok(n);
            }
        }
        Err(ValueError::Truncated)
    }

    fn len(&mut self) -> Result<usize, ValueError> {
        let n = self.varint()?;
        // A length can never exceed the bytes that remain; reject before allocating.
        if n > (self.bytes.len() - self.pos) as u64 {
            return Err(ValueError::Truncated);
        }
        Ok(n as usize)
    }

    fn string(&mut self) -> Result<String, ValueError> {
        let n = self.len()?;
        String::from_utf8(self.take(n)?.to_vec()).map_err(|_| ValueError::InvalidUtf8)
    }

    fn value(&mut self, depth: usize) -> Result<Value, ValueError> {
        let tag = self.take(1)?[0];
        Ok(match tag {
            T_NULL => Value::Null,
            T_FALSE => Value::Bool(false),
            T_TRUE => Value::Bool(true),
            T_INT64 => Value::Int64(i64::from_le_bytes(self.array()?)),
            T_DOUBLE => Value::Double(f64::from_bits(u64::from_le_bytes(self.array()?))),
            T_DECIMAL => {
                let c = i128::from_le_bytes(self.array()?);
                let e = i16::from_le_bytes(self.array()?);
                Value::Decimal(Decimal128::new(c, e)?)
            }
            T_TIMESTAMP => {
                let micros = i64::from_le_bytes(self.array()?);
                let offset = i16::from_le_bytes(self.array()?);
                Value::Timestamp(Timestamp::new(micros, offset)?)
            }
            T_STRING => Value::String(self.string()?),
            T_BINARY => {
                let n = self.len()?;
                Value::Binary(self.take(n)?.to_vec())
            }
            T_LIST | T_RECORD => {
                if depth >= MAX_DEPTH {
                    return Err(ValueError::TooDeep);
                }
                let n = self.len()?;
                if tag == T_LIST {
                    let mut items = Vec::with_capacity(n.min(1024));
                    for _ in 0..n {
                        items.push(self.value(depth + 1)?);
                    }
                    Value::List(items)
                } else {
                    let mut fields = Vec::with_capacity(n.min(1024));
                    for _ in 0..n {
                        let k = self.string()?;
                        fields.push((k, self.value(depth + 1)?));
                    }
                    Value::Record(Record::new(fields)?)
                }
            }
            other => return Err(ValueError::UnknownTag(other)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_len_matches_encoding() {
        let v = Value::Record(
            Record::new(vec![
                ("a".into(), Value::Int64(-1)),
                (
                    "b".into(),
                    Value::List(vec![Value::String("x".repeat(300)), Value::Null]),
                ),
                (
                    "c".into(),
                    Value::Decimal(Decimal128::parse("-12.340").unwrap()),
                ),
            ])
            .unwrap(),
        );
        assert_eq!(v.encode().len(), v.encoded_len());
    }

    #[test]
    fn decimal_parse() {
        let d = Decimal128::parse("-12.340").unwrap();
        assert_eq!((d.coefficient(), d.exponent()), (-12340, -3));
        assert_eq!(Decimal128::parse("1e-6176").unwrap().exponent(), -6176);
        assert!(Decimal128::parse("1e6112").is_err());
        assert!(Decimal128::parse("1".repeat(35).as_str()).is_err());
        assert!(Decimal128::parse("1.2.3").is_err());
    }

    #[test]
    fn decode_rejects_garbage() {
        assert_eq!(Value::decode(&[0xff]), Err(ValueError::UnknownTag(0xff)));
        assert_eq!(Value::decode(&[T_INT64, 1, 2]), Err(ValueError::Truncated));
        assert_eq!(
            Value::decode(&[T_NULL, T_NULL]),
            Err(ValueError::TrailingBytes)
        );
        // A huge length prefix must not allocate.
        assert_eq!(
            Value::decode(&[T_BINARY, 0xff, 0xff, 0xff, 0xff, 0x0f]),
            Err(ValueError::Truncated)
        );
    }
}
