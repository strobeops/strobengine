//! STOMP 1.2 frame codec (parser + serializer).
//!
//! A STOMP frame is `COMMAND\n`, zero or more `name:value\n` headers, a blank
//! `\n`, an optional body, and a terminating NULL octet:
//!
//! ```text
//! SEND
//! destination:/queue/orders
//! content-length:5
//!
//! hello\0
//! ```
//!
//! Header names/values use byte escaping (`\\n`, `\\r`, `\\c` for `:`, `\\\\`).
//! When `content-length` is present the body is exactly that many bytes
//! (binary-safe, may contain NULL); otherwise the body ends at the first NULL
//! octet and a single trailing EOL before the NULL is treated as a delimiter.
//!
//! [`Decoder`] is an incremental parser: bytes are pushed as they arrive from
//! the socket and complete frames are pulled off the front, so partial frames
//! spanning TCP reads and multiple frames arriving in one read both work.

use std::str;

/// The NULL octet that terminates every STOMP frame.
pub const NULL: u8 = 0;

/// A single heartbeat is a lone EOL emitted between frames.
pub const HEARTBEAT: &[u8] = b"\n";

/// Errors raised when a frame cannot be decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// A command/header line was not valid UTF-8.
    Utf8,
    /// A header line was missing the `name:value` separator.
    MissingHeaderValue,
    /// A header value ended with a dangling escape (`\`).
    TrailingEscape,
    /// A header contained an unknown escape sequence.
    InvalidEscape,
    /// A `content-length` body was not followed by the terminating NULL.
    MissingNull,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            Self::Utf8 => "frame contained invalid UTF-8",
            Self::MissingHeaderValue => "header line is missing ':' separator",
            Self::TrailingEscape => "header value ends with a dangling escape",
            Self::InvalidEscape => "header value contains an unknown escape",
            Self::MissingNull => "content-length body is not NULL-terminated",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for CodecError {}

/// A decoded STOMP frame.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Frame {
    /// Create a frame with the given command and no headers/body.
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Builder-style header insert (overwrites an existing name).
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.set_header(name, value);
        self
    }

    /// Builder-style body.
    pub fn with_body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    /// Set (or replace) a header value.
    pub fn set_header(&mut self, name: impl Into<String>, value: impl Into<String>) {
        let name = name.into();
        let value = value.into();
        if let Some(slot) = self
            .headers
            .iter_mut()
            .find(|(k, _)| k.eq_ignore_ascii_case(&name))
        {
            slot.1 = value;
        } else {
            self.headers.push((name, value));
        }
    }

    /// Look up the first header value with the given name (case-insensitive).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Parse and return the `content-length` header, if present and numeric.
    pub fn content_length(&self) -> Option<usize> {
        self.header("content-length")
            .and_then(|v| v.trim().parse().ok())
    }

    /// Serialize this frame (command, headers, blank line, body, NULL).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32 + self.body.len());
        out.extend_from_slice(self.command.as_bytes());
        out.push(b'\n');
        for (name, value) in &self.headers {
            encode_header(name, &mut out);
            out.push(b':');
            encode_header(value, &mut out);
            out.push(b'\n');
        }
        out.push(b'\n');
        out.extend_from_slice(&self.body);
        out.push(NULL);
        out
    }
}

/// Append a header component with the STOMP escaping applied.
fn encode_header(value: &str, out: &mut Vec<u8>) {
    for b in value.bytes() {
        match b {
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b':' => out.extend_from_slice(b"\\c"),
            b'\\' => out.extend_from_slice(b"\\\\"),
            _ => out.push(b),
        }
    }
}

/// Decode a header component, reversing the STOMP escaping.
fn decode_header(bytes: &[u8]) -> Result<String, CodecError> {
    let s = str::from_utf8(bytes).map_err(|_| CodecError::Utf8)?;
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('c') => out.push(':'),
            Some('\\') => out.push('\\'),
            Some(_) => return Err(CodecError::InvalidEscape),
            None => return Err(CodecError::TrailingEscape),
        }
    }
    Ok(out.to_owned())
}

/// Strip a single trailing `\r`.
fn strip_cr(line: &[u8]) -> &[u8] {
    match line.split_last() {
        Some((b'\r', rest)) => rest,
        _ => line,
    }
}

/// Find the first occurrence of `needle` in `haystack`.
fn find(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

/// Incremental STOMP frame decoder.
///
/// Push bytes as they arrive with [`Decoder::push`]; pull complete frames with
/// [`Decoder::next_frame`]. Returns `Ok(None)` when the buffer holds only a
/// partial frame, and `Err` for a malformed frame (callers should reset the
/// connection).
#[derive(Debug, Default)]
pub struct Decoder {
    buf: Vec<u8>,
    pos: usize,
}

impl Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append freshly received bytes to the internal buffer.
    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// Number of unconsumed bytes currently buffered.
    pub fn pending(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Decode the next complete frame, if one is buffered.
    pub fn next_frame(&mut self) -> Result<Option<Frame>, CodecError> {
        self.skip_heartbeats();

        // Command line.
        let Some(nl) = find(&self.buf[self.pos..], b'\n') else {
            return Ok(None);
        };
        let cmd_line = strip_cr(&self.buf[self.pos..self.pos + nl]);
        if cmd_line.is_empty() {
            // Defensive: leading EOLs are normally consumed by skip_heartbeats.
            self.pos += nl + 1;
            return self.next_frame();
        }
        let command = str::from_utf8(cmd_line)
            .map_err(|_| CodecError::Utf8)?
            .trim()
            .to_owned();
        let mut cursor = self.pos + nl + 1;

        // Headers up to the blank line.
        let mut headers = Vec::new();
        loop {
            let Some(hline_nl) = find(&self.buf[cursor..], b'\n') else {
                return Ok(None);
            };
            let line = strip_cr(&self.buf[cursor..cursor + hline_nl]);
            cursor += hline_nl + 1;
            if line.is_empty() {
                break;
            }
            let Some(colon) = find(line, b':') else {
                return Err(CodecError::MissingHeaderValue);
            };
            let name = decode_header(&line[..colon])?;
            let value = decode_header(&line[colon + 1..])?;
            headers.push((name, value));
        }

        // Body.
        let content_length = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse::<usize>().ok());

        let (body, end) = if let Some(len) = content_length {
            let body_end = cursor + len;
            if self.buf.len() <= body_end {
                return Ok(None);
            }
            if self.buf[body_end] != NULL {
                return Err(CodecError::MissingNull);
            }
            (self.buf[cursor..body_end].to_vec(), body_end + 1)
        } else {
            let Some(null_off) = find(&self.buf[cursor..], NULL) else {
                return Ok(None);
            };
            let body_end = cursor + null_off;
            // A single trailing EOL before the NULL is a frame delimiter, not body.
            let mut trimmed = body_end;
            if trimmed > cursor && self.buf[trimmed - 1] == b'\n' {
                trimmed -= 1;
                if trimmed > cursor && self.buf[trimmed - 1] == b'\r' {
                    trimmed -= 1;
                }
            }
            (self.buf[cursor..trimmed].to_vec(), body_end + 1)
        };

        self.pos = end;
        self.compact();
        Ok(Some(Frame {
            command,
            headers,
            body,
        }))
    }

    /// Skip leading EOL bytes (inter-frame heartbeats).
    fn skip_heartbeats(&mut self) {
        while self.pos < self.buf.len() && matches!(self.buf[self.pos], b'\n' | b'\r') {
            self.pos += 1;
        }
        self.compact();
    }

    /// Reclaim consumed bytes once the cursor is non-trivial.
    fn compact(&mut self) {
        if self.pos > 0 && (self.pos == self.buf.len() || self.pos >= 4096) {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_all(bytes: &[u8]) -> Vec<Frame> {
        let mut decoder = Decoder::new();
        decoder.push(bytes);
        let mut frames = Vec::new();
        while let Some(frame) = decoder.next_frame().unwrap() {
            frames.push(frame);
        }
        frames
    }

    #[test]
    fn encodes_and_decodes_a_simple_frame() {
        let frame = Frame::new("SEND")
            .with_header("destination", "/queue/orders")
            .with_body("hello");
        let decoded = decode_all(&frame.encode());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].command, "SEND");
        assert_eq!(decoded[0].header("destination"), Some("/queue/orders"));
        assert_eq!(decoded[0].body, b"hello");
    }

    #[test]
    fn content_length_preserves_embedded_nulls() {
        let body = b"a\0b\0c";
        let frame = Frame::new("MESSAGE")
            .with_header("content-length", body.len().to_string())
            .with_body(body.to_vec());
        let decoded = decode_all(&frame.encode());
        assert_eq!(decoded[0].body, body);
    }

    #[test]
    fn without_content_length_body_stops_at_first_null() {
        // Trailing frame delimiter EOL before NULL is stripped, embedded NULL is not.
        let raw = b"SEND\ndestination:/q\n\nabc\0";
        let decoded = decode_all(raw);
        assert_eq!(decoded[0].body, b"abc");
    }

    #[test]
    fn trailing_eol_before_null_is_not_part_of_body() {
        let raw = b"SEND\n\npayload\n\0";
        let decoded = decode_all(raw);
        assert_eq!(decoded[0].body, b"payload");
    }

    #[test]
    fn header_escaping_roundtrips() {
        let frame = Frame::new("SEND").with_header("x", "a:b\nc\rd\\e");
        let decoded = decode_all(&frame.encode());
        assert_eq!(decoded[0].header("x"), Some("a:b\nc\rd\\e"));
    }

    #[test]
    fn partial_frame_yields_none_until_complete() {
        let bytes = Frame::new("SEND")
            .with_header("destination", "/q")
            .with_body("body")
            .encode();
        let mut decoder = Decoder::new();
        for &byte in &bytes[..bytes.len() - 1] {
            decoder.push(&[byte]);
            assert!(
                decoder.next_frame().unwrap().is_none(),
                "frame completed early"
            );
        }
        decoder.push(&bytes[bytes.len() - 1..]);
        let frame = decoder.next_frame().unwrap().unwrap();
        assert_eq!(frame.body, b"body");
    }

    #[test]
    fn multiple_frames_in_one_buffer() {
        let mut bytes = Frame::new("SEND").with_body("a").encode();
        bytes.extend_from_slice(&Frame::new("SEND").with_body("b").encode());
        let decoded = decode_all(&bytes);
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].body, b"a");
        assert_eq!(decoded[1].body, b"b");
    }

    #[test]
    fn leading_heartbeats_are_skipped() {
        let bytes = b"\n\n\r\nSEND\n\nyo\0";
        let decoded = decode_all(bytes);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].command, "SEND");
    }

    #[test]
    fn missing_null_terminator_returns_none() {
        let mut decoder = Decoder::new();
        decoder.push(b"SEND\n\npartial body without terminator");
        assert!(decoder.next_frame().unwrap().is_none());
    }

    #[test]
    fn malformed_header_without_colon_errors() {
        let mut decoder = Decoder::new();
        decoder.push(b"SEND\nbroken-header\n\n\0");
        assert_eq!(decoder.next_frame(), Err(CodecError::MissingHeaderValue));
    }

    #[test]
    fn dangling_escape_errors() {
        let mut decoder = Decoder::new();
        decoder.push(b"SEND\nx:value\\\n\n\0");
        assert_eq!(decoder.next_frame(), Err(CodecError::TrailingEscape));
    }

    #[test]
    fn unknown_escape_errors() {
        let mut decoder = Decoder::new();
        decoder.push(b"SEND\nx:va\\zlue\n\n\0");
        assert_eq!(decoder.next_frame(), Err(CodecError::InvalidEscape));
    }

    #[test]
    fn content_length_without_null_errors() {
        let mut decoder = Decoder::new();
        // Declares 5 bytes but the trailing byte is 'X' instead of NULL.
        decoder.push(b"SEND\ncontent-length:5\n\nhelloX");
        assert_eq!(decoder.next_frame(), Err(CodecError::MissingNull));
    }

    #[test]
    fn duplicate_headers_are_preserved_first_wins() {
        let raw = b"MESSAGE\nfoo:1\nfoo:2\n\n\0";
        let decoded = decode_all(raw);
        assert_eq!(decoded[0].headers.len(), 2);
        assert_eq!(decoded[0].header("foo"), Some("1"));
    }

    #[test]
    fn crlf_line_endings_are_tolerated() {
        let raw = b"SEND\r\ndestination:/q\r\n\r\nbody\0";
        let decoded = decode_all(raw);
        assert_eq!(decoded[0].command, "SEND");
        assert_eq!(decoded[0].header("destination"), Some("/q"));
        assert_eq!(decoded[0].body, b"body");
    }

    #[test]
    fn compact_reclaims_consumed_bytes() {
        let mut decoder = Decoder::new();
        for _ in 0..40 {
            decoder.push(&Frame::new("SEND").with_body(vec![b'x'; 200]).encode());
            let frame = decoder.next_frame().unwrap().unwrap();
            assert_eq!(frame.body.len(), 200);
        }
        // Buffer must not grow without bound across many frames.
        assert!(
            decoder.pending() < 512,
            "buffer grew to {}",
            decoder.pending()
        );
    }
}
