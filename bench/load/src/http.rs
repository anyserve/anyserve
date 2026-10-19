use std::time::Duration;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::Result;

const MAX_BODY: usize = 8 * 1024 * 1024;

pub(super) struct HttpConnection {
    authority: String,
    stream: BufReader<TcpStream>,
}

struct Message {
    start: String,
    body: Vec<u8>,
    close: bool,
}

impl HttpConnection {
    pub(super) async fn connect(endpoint: &str) -> Result<Self> {
        let authority = endpoint
            .strip_prefix("http://")
            .ok_or("benchmark HTTP endpoint must use http://")?
            .trim_end_matches('/')
            .to_owned();
        if authority.contains('/') {
            return Err("benchmark HTTP endpoint must not contain a path".into());
        }
        let stream = TcpStream::connect(&authority).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            authority,
            stream: BufReader::new(stream),
        })
    }

    pub(super) async fn chat(&mut self, payload: &[u8]) -> Result {
        let body = serde_json::to_vec(&json!({
            "model": "bench", "stream": false,
            "messages": [{"role": "user", "content": String::from_utf8_lossy(payload)}]
        }))?;
        let header = format!(
            "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            self.authority,
            body.len()
        );
        self.stream.get_mut().write_all(header.as_bytes()).await?;
        self.stream.get_mut().write_all(&body).await?;
        let response = read_message(&mut self.stream)
            .await?
            .ok_or("HTTP peer closed before response")?;
        if response.start.split_whitespace().nth(1) != Some("200") {
            return Err(format!(
                "{}: {}",
                response.start,
                String::from_utf8_lossy(&response.body)
            )
            .into());
        }
        let value: serde_json::Value = serde_json::from_slice(&response.body)?;
        if value["choices"][0]["message"]["content"] != "bench response" {
            return Err("OpenAI gateway returned an unexpected response".into());
        }
        if response.close {
            *self = Self::connect(&format!("http://{}", self.authority)).await?;
        }
        Ok(())
    }
}

async fn read_message(stream: &mut BufReader<TcpStream>) -> Result<Option<Message>> {
    let mut start = String::new();
    if stream.read_line(&mut start).await? == 0 {
        return Ok(None);
    }
    let mut length = None;
    let mut chunked = false;
    let mut close = false;
    let mut total_header = start.len();
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await? == 0 {
            return Err("HTTP peer closed during headers".into());
        }
        total_header += line.len();
        if total_header > 64 * 1024 {
            return Err("HTTP headers exceed benchmark limit".into());
        }
        if line == "\r\n" {
            break;
        }
        let (name, value) = line.split_once(':').ok_or("invalid HTTP header")?;
        match name.to_ascii_lowercase().as_str() {
            "content-length" => length = Some(value.trim().parse::<usize>()?),
            "transfer-encoding" => chunked = value.trim().eq_ignore_ascii_case("chunked"),
            "connection" => close = value.trim().eq_ignore_ascii_case("close"),
            _ => {}
        }
    }
    let mut body = Vec::new();
    if chunked {
        loop {
            let mut line = String::new();
            stream.read_line(&mut line).await?;
            let count = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)?;
            if count == 0 {
                loop {
                    line.clear();
                    if stream.read_line(&mut line).await? == 0 {
                        return Err("HTTP peer closed during trailers".into());
                    }
                    if line == "\r\n" {
                        break;
                    }
                }
                break;
            }
            if count > MAX_BODY.saturating_sub(body.len()) {
                return Err("HTTP body exceeds benchmark limit".into());
            }
            let offset = body.len();
            body.resize(offset + count, 0);
            stream.read_exact(&mut body[offset..]).await?;
            let mut crlf = [0; 2];
            stream.read_exact(&mut crlf).await?;
            if crlf != *b"\r\n" {
                return Err("invalid HTTP chunk terminator".into());
            }
        }
    } else if let Some(count) = length {
        if count > MAX_BODY {
            return Err("HTTP body exceeds benchmark limit".into());
        }
        body.resize(count, 0);
        stream.read_exact(&mut body).await?;
    } else if start.starts_with("HTTP/") {
        return Err("HTTP response needs content-length or chunked framing".into());
    }
    Ok(Some(Message {
        start: start.trim_end().into(),
        body,
        close,
    }))
}

pub(super) async fn upstream(listen: &str) -> Result {
    let listener = TcpListener::bind(listen).await?;
    println!("ready upstream {}", listener.local_addr()?);
    let response = serde_json::to_vec(&json!({
        "id": "bench-fixed", "object": "chat.completion", "created": 0, "model": "bench",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "bench response"}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
    }))?;
    let mut tasks = JoinSet::new();
    let stop = crate::shutdown_signal();
    tokio::pin!(stop);
    loop {
        tokio::select! {
            result = &mut stop => { result?; return Ok(()); },
            joined = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = joined { eprintln!("upstream task: {error}"); }
            },
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                stream.set_nodelay(true)?;
                let response = response.clone();
                tasks.spawn(async move {
                    let mut stream = BufReader::new(stream);
                    loop {
                        let message = match tokio::time::timeout(Duration::from_secs(30), read_message(&mut stream)).await {
                            Ok(Ok(Some(message))) => message,
                            Ok(Ok(None)) | Err(_) => return,
                            Ok(Err(error)) => { eprintln!("upstream request: {error}"); return; },
                        };
                        let header = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", response.len());
                        if stream.get_mut().write_all(header.as_bytes()).await.is_err()
                            || stream.get_mut().write_all(&response).await.is_err()
                            || message.close { return; }
                    }
                });
            },
        }
    }
}
