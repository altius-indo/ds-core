//! GQL lexer (ISO/IEC 39075 §21 lexical elements, the v1 subset).
//!
//! Keywords are case-insensitive identifiers; the parser compares them upper-cased. Comments
//! `//`, `--` and `/* */` are skipped (GB02, GB03). Integer literals may be decimal, hex `0x`,
//! octal `0o` or binary `0b` (GL01–GL03). A numeric literal immediately followed by letters is
//! lexed with that suffix kept, so the parser can name the unsupported suffix features
//! (GL05–GL10) instead of reporting a generic syntax error.

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    /// Identifier or keyword, as written.
    Ident(String),
    /// `` `quoted identifier` ``: never a keyword.
    Quoted(String),
    Int(i128),
    /// Exact decimal text (e.g. `12.50`, `1E-3`), parsed later as decimal128.
    Decimal(String),
    /// A number with a type suffix (`1.5f`, `2d`): `(digits, suffix)`.
    Suffixed(String, String),
    Str(String),
    Param(String),
    Punct(&'static str),
    Eof,
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tok::Ident(s) => write!(f, "{s}"),
            Tok::Quoted(s) => write!(f, "`{s}`"),
            Tok::Int(n) => write!(f, "{n}"),
            Tok::Decimal(s) => write!(f, "{s}"),
            Tok::Suffixed(n, s) => write!(f, "{n}{s}"),
            Tok::Str(s) => write!(f, "'{s}'"),
            Tok::Param(p) => write!(f, "${p}"),
            Tok::Punct(p) => write!(f, "{p}"),
            Tok::Eof => f.write_str("end of input"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub tok: Tok,
    /// Byte offset of the token's first character.
    pub at: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexError {
    pub at: usize,
    pub message: String,
}

/// Longest first, so `<-[` lexes as `<-` then `[`, and `->` beats `-`.
const PUNCT: &[&str] = &[
    "<->", "<-", "->", "<~", "~>", "<>", "<=", ">=", "||", "::", "(", ")", "[", "]", "{", "}", ",",
    ".", ":", ";", "|", "&", "!", "%", "=", "<", ">", "+", "-", "*", "/", "~", "?",
];

pub fn lex(src: &str) -> Result<Vec<Token>, LexError> {
    let b = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Comments.
        if src[i..].starts_with("//") || src[i..].starts_with("--") {
            while i < b.len() && b[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if src[i..].starts_with("/*") {
            let end = src[i + 2..].find("*/").ok_or(LexError {
                at: i,
                message: "unterminated comment".into(),
            })?;
            i += 2 + end + 2;
            continue;
        }
        let at = i;
        // Identifiers and keywords.
        if c.is_ascii_alphabetic() || c == b'_' {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            out.push(Token {
                tok: Tok::Ident(src[at..i].to_string()),
                at,
            });
            continue;
        }
        if c == b'`' {
            let end = src[i + 1..].find('`').ok_or(LexError {
                at,
                message: "unterminated quoted identifier".into(),
            })?;
            out.push(Token {
                tok: Tok::Quoted(src[i + 1..i + 1 + end].to_string()),
                at,
            });
            i += end + 2;
            continue;
        }
        if c == b'$' {
            i += 1;
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            if start == i {
                return Err(LexError {
                    at,
                    message: "expected a parameter name after $".into(),
                });
            }
            out.push(Token {
                tok: Tok::Param(src[start..i].to_string()),
                at,
            });
            continue;
        }
        if c == b'\'' || c == b'"' {
            let (s, next) = string(src, i)?;
            out.push(Token {
                tok: Tok::Str(s),
                at,
            });
            i = next;
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            let (tok, next) = number(src, i)?;
            out.push(Token { tok, at });
            i = next;
            continue;
        }
        match PUNCT.iter().find(|p| src[i..].starts_with(**p)) {
            Some(p) => {
                out.push(Token {
                    tok: Tok::Punct(p),
                    at,
                });
                i += p.len();
            }
            None => {
                return Err(LexError {
                    at,
                    message: format!(
                        "unexpected character {:?}",
                        &src[i..].chars().next().unwrap_or('?')
                    ),
                });
            }
        }
    }
    out.push(Token {
        tok: Tok::Eof,
        at: src.len(),
    });
    Ok(out)
}

fn string(src: &str, start: usize) -> Result<(String, usize), LexError> {
    let quote = src.as_bytes()[start];
    let mut out = String::new();
    let mut chars = src[start + 1..].char_indices();
    while let Some((off, ch)) = chars.next() {
        let abs = start + 1 + off;
        if ch as u32 == quote as u32 {
            // A doubled quote is an escaped quote.
            if src[abs + 1..].starts_with(quote as char) {
                out.push(ch);
                chars.next();
                continue;
            }
            return Ok((out, abs + 1));
        }
        if ch == '\\' {
            let esc = chars.next().ok_or(LexError {
                at: abs,
                message: "unterminated escape".into(),
            })?;
            out.push(match esc.1 {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                '\\' => '\\',
                '\'' => '\'',
                '"' => '"',
                other => {
                    return Err(LexError {
                        at: abs,
                        message: format!("unknown escape \\{other}"),
                    });
                }
            });
            continue;
        }
        out.push(ch);
    }
    Err(LexError {
        at: start,
        message: "unterminated string".into(),
    })
}

fn number(src: &str, start: usize) -> Result<(Tok, usize), LexError> {
    let b = src.as_bytes();
    let mut i = start;
    let radix = match (b[i], b.get(i + 1).map(|c| c.to_ascii_lowercase())) {
        (b'0', Some(b'x')) => 16,
        (b'0', Some(b'o')) => 8,
        (b'0', Some(b'b')) => 2,
        _ => 10,
    };
    if radix != 10 {
        i += 2;
        let digits_start = i;
        while i < b.len() && (b[i].is_ascii_hexdigit() || b[i] == b'_') {
            i += 1;
        }
        let digits: String = src[digits_start..i].chars().filter(|c| *c != '_').collect();
        let n = i128::from_str_radix(&digits, radix).map_err(|e| LexError {
            at: start,
            message: format!("invalid integer literal: {e}"),
        })?;
        return Ok((Tok::Int(n), i));
    }
    let mut decimal = false;
    while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
        i += 1;
    }
    if i < b.len() && b[i] == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit) {
        decimal = true;
        i += 1;
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'_') {
            i += 1;
        }
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        let mut j = i + 1;
        if j < b.len() && (b[j] == b'+' || b[j] == b'-') {
            j += 1;
        }
        if j < b.len() && b[j].is_ascii_digit() {
            decimal = true;
            i = j;
            while i < b.len() && b[i].is_ascii_digit() {
                i += 1;
            }
        }
    }
    let text: String = src[start..i].chars().filter(|c| *c != '_').collect();
    // A suffix such as `f`, `d`, `m` (GL05–GL10).
    if i < b.len() && b[i].is_ascii_alphabetic() {
        let s = i;
        while i < b.len() && b[i].is_ascii_alphanumeric() {
            i += 1;
        }
        return Ok((Tok::Suffixed(text, src[s..i].to_string()), i));
    }
    if decimal {
        return Ok((Tok::Decimal(text), i));
    }
    let n = text.parse::<i128>().map_err(|e| LexError {
        at: start,
        message: format!("invalid integer literal: {e}"),
    })?;
    Ok((Tok::Int(n), i))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Tok> {
        lex(s).unwrap().into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn edge_arrows_and_punctuation() {
        assert_eq!(
            toks("(a)-[e]->(b)<-[f]-(c)"),
            vec![
                Tok::Punct("("),
                Tok::Ident("a".into()),
                Tok::Punct(")"),
                Tok::Punct("-"),
                Tok::Punct("["),
                Tok::Ident("e".into()),
                Tok::Punct("]"),
                Tok::Punct("->"),
                Tok::Punct("("),
                Tok::Ident("b".into()),
                Tok::Punct(")"),
                Tok::Punct("<-"),
                Tok::Punct("["),
                Tok::Ident("f".into()),
                Tok::Punct("]"),
                Tok::Punct("-"),
                Tok::Punct("("),
                Tok::Ident("c".into()),
                Tok::Punct(")"),
                Tok::Eof
            ]
        );
    }

    #[test]
    fn literals_and_comments() {
        assert_eq!(
            toks("0x1F 0o17 0b101 42 12.50 1E3 'it''s' $p // c\n-- c\n/* c */ `my id`"),
            vec![
                Tok::Int(31),
                Tok::Int(15),
                Tok::Int(5),
                Tok::Int(42),
                Tok::Decimal("12.50".into()),
                Tok::Decimal("1E3".into()),
                Tok::Str("it's".into()),
                Tok::Param("p".into()),
                Tok::Quoted("my id".into()),
                Tok::Eof
            ]
        );
        assert_eq!(toks("1.5f")[0], Tok::Suffixed("1.5".into(), "f".into()));
    }

    #[test]
    fn errors_have_offsets() {
        assert_eq!(lex("'abc").unwrap_err().at, 0);
        assert_eq!(lex("a # b").unwrap_err().at, 2);
    }
}
