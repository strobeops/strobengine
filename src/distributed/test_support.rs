//! Shared test helpers for the distributed control-plane integration tests.
//!
//! Provides a minimal, deterministic HTTP/1.1 target on loopback that records
//! every request it serves, so worker and master tests can drive real traffic
//! without external dependencies.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Spawn a minimal HTTP/1.1 responder on loopback that records each request.
///
/// Returns the base URL, a request counter, a shutdown token and the task
/// handle. The caller must cancel the token and await the handle to stop it.
pub async fn spawn_http_target() -> (String, Arc<AtomicUsize>, CancellationToken, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();

    let hits_task = Arc::clone(&hits);
    let cancel_task = cancel.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel_task.cancelled() => break,
                accepted = listener.accept() => {
                    let Ok((mut socket, _)) = accepted else { break };
                    let hits = Arc::clone(&hits_task);
                    tokio::spawn(async move {
                        if serve_one(&mut socket).await.is_ok() {
                            hits.fetch_add(1, Ordering::SeqCst);
                        }
                    });
                }
            }
        }
    });

    (format!("http://{addr}"), hits, cancel, handle)
}

/// Serve exactly one HTTP/1.1 request on `socket` and close.
async fn serve_one(socket: &mut TcpStream) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 1024];

    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let headers = String::from_utf8_lossy(&buf[..header_end]);
    let content_length = parse_content_length(&headers);
    let mut consumed_body = buf.len() - header_end;
    while consumed_body < content_length {
        let n = socket.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        consumed_body += n;
    }

    let body = b"{\"ok\":true}";
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    socket.write_all(body).await?;
    socket.flush().await?;
    Ok(())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_length_parsing_is_case_insensitive() {
        assert_eq!(parse_content_length("Content-Length: 42\r\nHost: x"), 42);
        assert_eq!(parse_content_length("content-length:7\r\n"), 7);
        assert_eq!(parse_content_length("Host: x"), 0);
    }

    #[test]
    fn find_subslice_locates_separator() {
        assert_eq!(find_subslice(b"abc\r\n\r\nxyz", b"\r\n\r\n"), Some(3));
        assert_eq!(find_subslice(b"no separator", b"\r\n\r\n"), None);
    }
}
