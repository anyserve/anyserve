use std::collections::{BTreeSet, HashMap};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyserve_client::execution::{
    Attempt, AttemptClient, AttemptLink, ExecutionClient, WorkerSession, v2,
};
use serde_json::Value;
use tokio::sync::{Notify, oneshot};
use tonic::Code;

const OPERATOR_CLIENT_TOKEN: &str = "e2e-native-operator-tok";
const DEFAULT_WORKER_TOKEN: &str = "e2e-native-worker-tok";
const TENANT_A_CLIENT_TOKEN: &str = "e2e-tenant-a-client-tok";
const TENANT_A_WORKER_TOKEN: &str = "e2e-tenant-a-worker-tok";
const TENANT_B_CLIENT_TOKEN: &str = "e2e-tenant-b-client-tok";
const TENANT_B_WORKER_TOKEN: &str = "e2e-tenant-b-worker-tok";

// ---------------------------------------------------------------------------
// Pure-Rust FIPS 180-4 SHA-256 (avoids touching Cargo.lock under `--locked`)
// ---------------------------------------------------------------------------

fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64) * 8;
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0x00);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for block in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, &word) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes(word);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }

    let mut out = String::with_capacity(64);
    for word in state {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{word:08x}");
    }
    out
}

fn pattern_bytes(len: usize, seed: u8) -> Vec<u8> {
    let mut out = vec![0u8; len];
    let mut s = u64::from(seed)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(1);
    for chunk in out.chunks_mut(8) {
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        let bytes = s.wrapping_mul(0x2545_f491_4f6c_dd1d).to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    out
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("resolve workspace root")
}

// ---------------------------------------------------------------------------
// Node 24 `node-native-gateway.ts` Subprocess Harness
// ---------------------------------------------------------------------------

struct NativeGatewayProc {
    child: Child,
    http_base: String,
    ws_base: String,
}

impl NativeGatewayProc {
    async fn start() -> Self {
        let root = workspace_root();
        let script = root.join("deploy/cloudflare/src/node-native-gateway.ts");
        let scoped_tokens_json = serde_json::json!([
            {
                "token": TENANT_A_CLIENT_TOKEN,
                "role": "client",
                "namespace": "tenant-a",
                "subject": "tenant-a-client"
            },
            {
                "token": TENANT_A_WORKER_TOKEN,
                "role": "worker",
                "namespace": "tenant-a",
                "subject": "tenant-a-worker"
            },
            {
                "token": TENANT_B_CLIENT_TOKEN,
                "role": "client",
                "namespace": "tenant-b",
                "subject": "tenant-b-client"
            },
            {
                "token": TENANT_B_WORKER_TOKEN,
                "role": "worker",
                "namespace": "tenant-b",
                "subject": "tenant-b-worker"
            }
        ])
        .to_string();

        let mut child = Command::new("node")
            .arg("--experimental-strip-types")
            .arg(&script)
            .current_dir(&root)
            .env("HOST", "127.0.0.1")
            .env("PORT", "0")
            .env("ANYSERVE_TEST_HOOKS", "1")
            .env("ANYSERVE_CLIENT_TOKENS", OPERATOR_CLIENT_TOKEN)
            .env("ANYSERVE_WORKER_TOKENS", DEFAULT_WORKER_TOKEN)
            .env("ANYSERVE_SCOPED_TOKENS", scoped_tokens_json)
            .env(
                "ANYSERVE_PUBLIC_ENDPOINT",
                "wss://native.anyserve.example.com",
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn node --experimental-strip-types node-native-gateway.ts");

        let stdout = child.stdout.take().expect("gateway stdout");
        let stderr = child.stderr.take().expect("gateway stderr");

        // Drain stderr in background so pipe never blocks
        std::thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines() {
                if line.is_err() {
                    break;
                }
            }
        });

        let (listen_tx, listen_rx) = std::sync::mpsc::channel::<(String, String)>();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                let Ok(text) = line else { break };
                if let Some(rest) = text.strip_prefix("ANYSERVE_NATIVE_GATEWAY_LISTENING ") {
                    let mut parts = rest.split_whitespace();
                    if let (Some(http_url), Some(ws_url)) = (parts.next(), parts.next()) {
                        let _ = listen_tx.send((http_url.to_owned(), ws_url.to_owned()));
                    }
                }
            }
        });

        let (http_base, ws_base) = listen_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("node-native-gateway did not emit listening line within 10s");

        let http = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(resp) = http.get(format!("{http_base}/readyz")).send().await
                && resp.status() == reqwest::StatusCode::NO_CONTENT
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node-native-gateway did not pass /readyz at {http_base}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        Self {
            child,
            http_base,
            ws_base,
        }
    }

    #[allow(dead_code)]
    async fn hibernate(&self) -> Value {
        let http = reqwest::Client::new();
        let resp = http
            .post(format!("{}/__test/hibernate", self.http_base))
            .send()
            .await
            .expect("POST /__test/hibernate");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.json::<Value>().await.expect("decode hibernate json")
    }

    async fn stats(&self) -> Value {
        let http = reqwest::Client::new();
        let resp = http
            .get(format!("{}/__test/stats", self.http_base))
            .send()
            .await
            .expect("GET /__test/stats");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.json::<Value>().await.expect("decode stats json")
    }

    fn stop(mut self) {
        #[cfg(unix)]
        {
            let pid = self.child.id().to_string();
            let _ = Command::new("kill").args(["-TERM", &pid]).status();
            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if let Ok(Some(_)) = self.child.try_wait() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for NativeGatewayProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Shared Protobuf & Worker Helpers
// ---------------------------------------------------------------------------

fn make_spec(
    interface: &str,
    inputs: Vec<v2::Object>,
    durable: bool,
    max_attempts: u32,
) -> v2::JobSpec {
    v2::JobSpec {
        demand: Some(v2::Demand {
            interface: interface.to_owned(),
            ..Default::default()
        }),
        inputs,
        policy: Some(v2::ExecutionPolicy {
            durability: (if durable {
                v2::Durability::Durable
            } else {
                v2::Durability::Ephemeral
            }) as i32,
            max_attempts,
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn inline_obj(bytes: Vec<u8>) -> v2::Object {
    v2::Object {
        body: Some(v2::object::Body::Inline(bytes.into())),
        metadata: HashMap::new(),
    }
}

async fn connect_worker_with_token(
    ws_base: &str,
    token: &str,
    worker_id: &str,
    interfaces: &[&str],
) -> (WorkerSession, v2::Welcome) {
    let mut session = WorkerSession::connect(
        ws_base,
        Some(token),
        v2::Hello {
            worker_id: worker_id.to_owned(),
            interfaces: interfaces.iter().map(|s| (*s).to_owned()).collect(),
            max_concurrency: 4,
            total_capacity: HashMap::from([("slot".to_owned(), 4)]),
            ..Default::default()
        },
    )
    .await
    .expect("connect WorkerSession over ws://");

    let Some(v2::server_message::Msg::Welcome(welcome)) = session
        .messages
        .message()
        .await
        .expect("read Welcome")
        .expect("non-empty Welcome stream")
        .msg
    else {
        panic!("expected ServerMessage::Welcome");
    };

    session
        .control
        .send(v2::WorkerMessage {
            msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                free_slots: 4,
            })),
        })
        .await
        .expect("send initial Credit");

    (session, welcome)
}

// ---------------------------------------------------------------------------
// 1. 2 MiB Single-Message Invoke, Streaming Invoke, and Mid-Stream CancelOnDrop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_do_invoke_2mib_streaming_and_cancel_on_drop() {
    let gw = NativeGatewayProc::start().await;

    let (mut session, welcome) = connect_worker_with_token(
        &gw.ws_base,
        DEFAULT_WORKER_TOKEN,
        "rust-native-worker-1",
        &["native.echo", "native.stream", "native.hold"],
    )
    .await;

    // Verify Workers Free native defaults (Section 5.1 & 6.1)
    assert_eq!(welcome.heartbeat_interval_ms, 30_000);
    assert_eq!(welcome.heartbeat_timeout_ms, 90_000);
    let limits = welcome.limits.expect("Welcome.limits");
    assert_eq!(limits.max_chunk_bytes, 65_536);
    assert_eq!(limits.attempt_window_bytes, 4_194_304);
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));

    let cancel_notified = Arc::new(Notify::new());
    let cancel_notified_bg = Arc::clone(&cancel_notified);

    let worker_task = tokio::spawn(async move {
        let mut cancel_waiters: HashMap<String, oneshot::Sender<()>> = HashMap::new();
        while let Ok(Some(msg)) = session.messages.message().await {
            let Some(inner) = msg.msg else { continue };
            match inner {
                v2::server_message::Msg::Cancel(cancel) => {
                    if let Some(tx) = cancel_waiters.remove(&cancel.lease_id) {
                        let _ = tx.send(());
                    }
                    let _ = session
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::CancelAck(v2::CancelAck {
                                lease_id: cancel.lease_id,
                            })),
                        })
                        .await;
                    cancel_notified_bg.notify_waiters();
                }
                v2::server_message::Msg::Assign(assign) => {
                    session
                        .control
                        .send(v2::WorkerMessage {
                            msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                                lease_id: assign.lease_id.clone(),
                                accepted: true,
                                reject_reason: String::new(),
                            })),
                        })
                        .await
                        .unwrap();

                    let mut data = session.data.clone();
                    let control = session.control.clone();
                    let limits_copy = limits;
                    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
                    if assign.interface == "native.hold" {
                        cancel_waiters.insert(assign.lease_id.clone(), cancel_tx);
                    }

                    tokio::spawn(async move {
                        match assign.interface.as_str() {
                            "native.echo" => {
                                let mut attempt =
                                    Attempt::open(&mut data, &assign, &limits_copy, hb_timeout)
                                        .await
                                        .unwrap();
                                let out = attempt.inputs.clone();
                                attempt
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits_copy,
                                        &out,
                                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                    )
                                    .await
                                    .unwrap();
                            }
                            "native.stream" => {
                                let mut attempt = Attempt::open_stream(
                                    &mut data,
                                    &assign,
                                    &limits_copy,
                                    hb_timeout,
                                )
                                .await
                                .unwrap();
                                let mut combined = Vec::new();
                                let mut chunk_idx = 0usize;
                                while let Some(chunk) = attempt.input_chunk().await.unwrap() {
                                    combined.extend_from_slice(&chunk.data);
                                    chunk_idx += 1;
                                    // Emit progress event first, then non-progress streamed Output
                                    attempt
                                        .frame(
                                            format!("progress-{chunk_idx}").into_bytes().into(),
                                            HashMap::new(),
                                            true,
                                        )
                                        .await
                                        .unwrap();
                                    attempt
                                        .frame(chunk.data, HashMap::new(), false)
                                        .await
                                        .unwrap();
                                }
                                attempt
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits_copy,
                                        &[inline_obj(combined)],
                                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                    )
                                    .await
                                    .unwrap();
                            }
                            "native.hold" => {
                                let mut attempt = Attempt::open_stream(
                                    &mut data,
                                    &assign,
                                    &limits_copy,
                                    hb_timeout,
                                )
                                .await
                                .unwrap();
                                attempt
                                    .frame(b"ready-to-drop".to_vec().into(), HashMap::new(), true)
                                    .await
                                    .unwrap();
                                let _ = cancel_rx.await;
                                let _ = attempt
                                    .commit(
                                        &mut data,
                                        &assign,
                                        &limits_copy,
                                        &[],
                                        v2::finish::Outcome::Cancelled(v2::Cancelled {}),
                                    )
                                    .await;
                            }
                            other => panic!("unexpected interface: {other}"),
                        }
                        let _ = control
                            .send(v2::WorkerMessage {
                                msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                                    free_slots: 1,
                                })),
                            })
                            .await;
                    });
                }
                _ => {}
            }
        }
    });

    let mut client = ExecutionClient::connect_with_token(&gw.ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();

    // (a) 2 MiB single-message Invoke
    let two_mib = pattern_bytes(2 * 1024 * 1024, 0x37);
    let two_mib_sha = sha256_hex(&two_mib);
    let mut inv_2m = client
        .invoke(make_spec(
            "native.echo",
            vec![inline_obj(two_mib.clone())],
            false,
            1,
        ))
        .await
        .unwrap();

    let mut saw_accepted = false;
    let mut finished_2m = None;
    while let Some(ev) = inv_2m.message().await.unwrap() {
        match ev.event {
            Some(v2::execution_event::Event::Accepted(_)) => saw_accepted = true,
            Some(v2::execution_event::Event::Finished(fin)) => {
                finished_2m = Some(fin);
            }
            _ => {}
        }
    }
    assert!(saw_accepted, "2 MiB Invoke must emit Accepted");
    let fin_2m = finished_2m.expect("2 MiB Invoke must emit Finished before EOF");
    assert_eq!(fin_2m.state, v2::JobState::Succeeded as i32);
    let Some(v2::object::Body::Inline(out_2m)) = &fin_2m.outputs[0].body else {
        panic!("expected inline 2 MiB output");
    };
    assert_eq!(out_2m.len(), two_mib.len());
    assert_eq!(sha256_hex(out_2m), two_mib_sha);

    // (b) Streaming Invoke (InputChunks + InputEnd + progress + Output + Finished)
    let part1 = pattern_bytes(100 * 1024, 0x01);
    let part2 = pattern_bytes(180 * 1024, 0x02);
    let part3 = pattern_bytes(44 * 1024, 0x03);
    let mut expected_stream = Vec::with_capacity(part1.len() + part2.len() + part3.len());
    expected_stream.extend_from_slice(&part1);
    expected_stream.extend_from_slice(&part2);
    expected_stream.extend_from_slice(&part3);

    let mut inv_stream = client
        .invoke_stream(make_spec("native.stream", Vec::new(), false, 1))
        .await
        .unwrap();
    inv_stream.send(part1).await.unwrap();
    inv_stream.send(part2).await.unwrap();
    inv_stream.send(part3).await.unwrap();
    inv_stream.finish_input().await.unwrap();

    let mut progress_msgs = Vec::new();
    let mut output_bytes = Vec::new();
    let mut stream_fin = None;
    while let Some(ev) = inv_stream.events.message().await.unwrap() {
        match ev.event {
            Some(v2::execution_event::Event::Output(out)) => {
                if out.progress {
                    progress_msgs.push(String::from_utf8_lossy(&out.data).into_owned());
                } else {
                    output_bytes.extend_from_slice(&out.data);
                }
            }
            Some(v2::execution_event::Event::Finished(fin)) => {
                stream_fin = Some(fin);
            }
            _ => {}
        }
    }
    assert!(!progress_msgs.is_empty(), "must receive progress frames");
    assert_eq!(output_bytes, expected_stream);
    let fin_s = stream_fin.expect("streaming Invoke must emit Finished");
    assert_eq!(fin_s.state, v2::JobState::Succeeded as i32);
    assert_eq!(
        fin_s.outputs[0].body,
        Some(v2::object::Body::Inline(expected_stream.into()))
    );

    // (c) Mid-stream caller drop (CancelOnDrop)
    let mut hold_inv = client
        .invoke_stream(make_spec("native.hold", Vec::new(), false, 1))
        .await
        .unwrap();
    let mut hold_job_id = None;
    while let Some(ev) = hold_inv.events.message().await.unwrap() {
        hold_job_id = Some(ev.job_id.clone());
        if matches!(ev.event, Some(v2::execution_event::Event::Output(_))) {
            break;
        }
    }
    let hold_job_id = hold_job_id.expect("hold job_id");
    drop(hold_inv);

    tokio::time::timeout(Duration::from_secs(5), cancel_notified.notified())
        .await
        .expect("dropping Invoke stream must send Cancel to worker");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let job = client.get(&hold_job_id, false).await.unwrap();
            if job.state == v2::JobState::Cancelled as i32 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("dropped Invoke job must transition to Cancelled");

    worker_task.abort();
    gw.stop();
}

// ---------------------------------------------------------------------------
// 2. Exposed Non-Progress Output + Retryable Worker Failure -> output_interrupted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_do_exposed_output_retryable_failure_becomes_output_interrupted() {
    let gw = NativeGatewayProc::start().await;

    let (mut session, welcome) = connect_worker_with_token(
        &gw.ws_base,
        DEFAULT_WORKER_TOKEN,
        "rust-native-worker-expose",
        &["native.progress_retry", "native.expose_fail"],
    )
    .await;
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));

    let progress_attempts = Arc::new(AtomicUsize::new(0));
    let expose_attempts = Arc::new(AtomicUsize::new(0));
    let caller_saw_exposed = Arc::new(Notify::new());

    let progress_attempts_bg = Arc::clone(&progress_attempts);
    let expose_attempts_bg = Arc::clone(&expose_attempts);
    let caller_saw_exposed_bg = Arc::clone(&caller_saw_exposed);

    let worker_task = tokio::spawn(async move {
        while let Ok(Some(msg)) = session.messages.message().await {
            let Some(v2::server_message::Msg::Assign(assign)) = msg.msg else {
                continue;
            };
            session
                .control
                .send(v2::WorkerMessage {
                    msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                        lease_id: assign.lease_id.clone(),
                        accepted: true,
                        reject_reason: String::new(),
                    })),
                })
                .await
                .unwrap();

            let mut data = session.data.clone();
            let control = session.control.clone();
            let limits_copy = limits;
            let pa = Arc::clone(&progress_attempts_bg);
            let ea = Arc::clone(&expose_attempts_bg);
            let saw_exposed = Arc::clone(&caller_saw_exposed_bg);

            tokio::spawn(async move {
                let mut attempt = Attempt::open(&mut data, &assign, &limits_copy, hb_timeout)
                    .await
                    .unwrap();

                match assign.interface.as_str() {
                    "native.progress_retry" => {
                        let n = pa.fetch_add(1, Ordering::SeqCst) + 1;
                        attempt
                            .frame(format!("p-{n}").into_bytes().into(), HashMap::new(), true)
                            .await
                            .unwrap();
                        if n == 1 {
                            attempt
                                .commit(
                                    &mut data,
                                    &assign,
                                    &limits_copy,
                                    &[],
                                    v2::finish::Outcome::Failed(v2::ErrorInfo {
                                        code: "transient".into(),
                                        message: "first attempt transient failure".into(),
                                        retryable: true,
                                    }),
                                )
                                .await
                                .unwrap();
                        } else {
                            attempt
                                .commit(
                                    &mut data,
                                    &assign,
                                    &limits_copy,
                                    &[inline_obj(b"recovered-after-progress".to_vec())],
                                    v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    "native.expose_fail" => {
                        ea.fetch_add(1, Ordering::SeqCst);
                        // Non-progress Output (`progress = false`) marks attempt as EXPOSED!
                        attempt
                            .frame(
                                b"non-progress-exposed-chunk".to_vec().into(),
                                HashMap::new(),
                                false,
                            )
                            .await
                            .unwrap();
                        saw_exposed.notified().await;
                        attempt
                            .commit(
                                &mut data,
                                &assign,
                                &limits_copy,
                                &[],
                                v2::finish::Outcome::Failed(v2::ErrorInfo {
                                    code: "transient_after_output".into(),
                                    message: "worker crashed after exposing output".into(),
                                    retryable: true,
                                }),
                            )
                            .await
                            .unwrap();
                    }
                    other => panic!("unexpected interface: {other}"),
                }

                let _ = control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                            free_slots: 1,
                        })),
                    })
                    .await;
            });
        }
    });

    let mut client = ExecutionClient::connect_with_token(&gw.ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();

    // (a) Progress-only output + retryable failure -> retries automatically (200ms exact alarm) and succeeds!
    let mut inv_retry = client
        .invoke(make_spec(
            "native.progress_retry",
            vec![inline_obj(b"go".to_vec())],
            false,
            3,
        ))
        .await
        .unwrap();
    let mut saw_restarted = false;
    let mut retry_fin = None;
    while let Some(ev) = inv_retry.message().await.unwrap() {
        match ev.event {
            Some(v2::execution_event::Event::AttemptRestarted(_)) => saw_restarted = true,
            Some(v2::execution_event::Event::Finished(fin)) => retry_fin = Some(fin),
            _ => {}
        }
    }
    assert!(
        saw_restarted,
        "progress-only retryable failure must emit AttemptRestarted"
    );
    let retry_fin = retry_fin.unwrap();
    assert_eq!(retry_fin.state, v2::JobState::Succeeded as i32);
    assert_eq!(progress_attempts.load(Ordering::SeqCst), 2);

    // (b) Exposed non-progress Output + retryable failure -> fails immediately with `output_interrupted`, NEVER re-run!
    let mut inv_expose = client
        .invoke(make_spec(
            "native.expose_fail",
            vec![inline_obj(b"go".to_vec())],
            false,
            3,
        ))
        .await
        .unwrap();

    let mut exposed_chunks = Vec::new();
    let mut expose_fin = None;
    while let Some(ev) = inv_expose.message().await.unwrap() {
        match ev.event {
            Some(v2::execution_event::Event::Output(out)) if !out.progress => {
                exposed_chunks.extend_from_slice(&out.data);
                caller_saw_exposed.notify_waiters();
            }
            Some(v2::execution_event::Event::AttemptRestarted(_)) => {
                panic!("exposed Invoke must NEVER emit AttemptRestarted");
            }
            Some(v2::execution_event::Event::Finished(fin)) => {
                expose_fin = Some(fin);
            }
            _ => {}
        }
    }
    assert_eq!(exposed_chunks, b"non-progress-exposed-chunk");
    let expose_fin = expose_fin.expect("exposed Invoke must emit Finished");
    assert_eq!(expose_fin.state, v2::JobState::Failed as i32);
    let err = expose_fin.error.expect("Finished.error");
    assert_eq!(err.code, "output_interrupted");
    assert!(!err.retryable);
    assert_eq!(
        expose_attempts.load(Ordering::SeqCst),
        1,
        "exposed Invoke must never execute a second attempt"
    );

    worker_task.abort();
    gw.stop();
}

// ---------------------------------------------------------------------------
// 3. Durable Submit/Watch/GetJob/ListJobs/Cancel across 24x1B Bundle, 64 KiB
//    Inline, 2.5 MiB R2 Spill, and 8 MiB Multi-Window Result + Mid-Upload Hibernate
// ---------------------------------------------------------------------------

async fn upload_8mib_with_mid_hibernate(
    mut link: AttemptLink,
    inputs_via_attempt: bool,
    payload_8m: &[u8],
    chunk_bytes: usize,
    window_bytes: usize,
    gw_http_base: &str,
) -> (u64, String, String) {
    // 1. Wait for AttemptOpened (and input trailer only when `inputs_via_attempt` is true, per Section 5.3)
    let mut opened = false;
    loop {
        let down = link
            .events
            .message()
            .await
            .unwrap()
            .expect("attempt stream open");
        match down.msg.unwrap() {
            v2::attempt_down::Msg::Opened(_) => {
                opened = true;
                if !inputs_via_attempt {
                    break;
                }
            }
            v2::attempt_down::Msg::Input(inp) if inp.last => {
                break;
            }
            v2::attempt_down::Msg::Input(_) => {}
            other => panic!("unexpected pre-upload AttemptDown: {other:?}"),
        }
    }
    assert!(opened, "AttemptOpened must precede InputData");

    let mut sent_bytes: u64 = 0;
    let mut acked_bytes: u64 = 0;
    let total_chunks = payload_8m.len().div_ceil(chunk_bytes);
    assert_eq!(total_chunks, 128, "8 MiB at 64 KiB chunks is 128 chunks");

    let mut hibernate_runs = 0u64;
    let mut prev_boot_id = String::new();
    let mut new_boot_id = String::new();

    for chunk_idx in 0..total_chunks {
        let start = chunk_idx * chunk_bytes;
        let end = (start + chunk_bytes).min(payload_8m.len());
        let slice = &payload_8m[start..end];

        while (sent_bytes.saturating_sub(acked_bytes) as usize) + slice.len() > window_bytes {
            let down = link
                .events
                .message()
                .await
                .unwrap()
                .expect("expected Ack while window full");
            if let Some(v2::attempt_down::Msg::Ack(ack)) = down.msg {
                acked_bytes = acked_bytes.max(ack.result_bytes.min(sent_bytes));
            }
        }

        link.send
            .send(v2::AttemptUp {
                msg: Some(v2::attempt_up::Msg::Result(v2::ResultChunk {
                    index: 0,
                    data: slice.to_vec().into(),
                    last: end == payload_8m.len(),
                    metadata: if chunk_idx == 0 {
                        HashMap::from([("x-test".to_owned(), "8mib-hibernate".to_owned())])
                    } else {
                        HashMap::new()
                    },
                    uri: String::new(),
                })),
            })
            .await
            .unwrap();
        sent_bytes += slice.len() as u64;

        // Right after chunk 67 (68 chunks = 4.25 MiB: two 2 MiB segments sealed in R2 +
        // 4 unsealed chunks in `native_upload_tail`, across the first 4 MiB window!),
        // force a real DO hibernation WITHOUT closing any TCP/WebSocket connections!
        if chunk_idx == 67 {
            // Give the gateway socket pump a moment to commit chunk 67 into SQLite before hibernating
            tokio::time::sleep(Duration::from_millis(30)).await;
            let http = reqwest::Client::new();
            let hib: Value = http
                .post(format!("{gw_http_base}/__test/hibernate"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            hibernate_runs = hib["constructor_runs"].as_u64().unwrap();
            prev_boot_id = hib["prev_boot_id"].as_str().unwrap().to_owned();
            new_boot_id = hib["boot_id"].as_str().unwrap().to_owned();
            assert!(
                hib["open_websockets"].as_u64().unwrap() >= 3,
                "session + attempt + watch TCP WebSockets must remain open across hibernation"
            );
        }
    }

    link.send
        .send(v2::AttemptUp {
            msg: Some(v2::attempt_up::Msg::Finish(v2::Finish {
                outcome: Some(v2::finish::Outcome::Succeeded(v2::Succeeded {})),
                handler_duration_ms: None,
            })),
        })
        .await
        .unwrap();

    loop {
        let down = link
            .events
            .message()
            .await
            .unwrap()
            .expect("expected AttemptFinished::Committed");
        match down.msg.unwrap() {
            v2::attempt_down::Msg::Ack(_) => {}
            v2::attempt_down::Msg::Finished(fin) => {
                assert_eq!(
                    fin.status,
                    v2::attempt_finished::Status::Committed as i32,
                    "post-hibernation 8 MiB upload must commit cleanly: {}",
                    fin.reason
                );
                break;
            }
            other => panic!("unexpected post-finish AttemptDown: {other:?}"),
        }
    }

    (hibernate_runs, prev_boot_id, new_boot_id)
}

#[tokio::test]
async fn native_do_durable_bundles_r2_spill_and_8mib_mid_upload_hibernation() {
    let gw = NativeGatewayProc::start().await;

    let (mut session, welcome) = connect_worker_with_token(
        &gw.ws_base,
        DEFAULT_WORKER_TOKEN,
        "rust-native-worker-durable",
        &["native.echo", "native.gen8m"],
    )
    .await;
    let limits = welcome.limits.unwrap();
    let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));

    let payload_8m = Arc::new(pattern_bytes(8 * 1024 * 1024, 0x9c));
    let payload_8m_bg = Arc::clone(&payload_8m);
    let gw_http_bg = gw.http_base.clone();
    let (hib_info_tx, hib_info_rx) = oneshot::channel::<(u64, String, String)>();
    let hib_info_tx = Arc::new(tokio::sync::Mutex::new(Some(hib_info_tx)));

    let worker_task = tokio::spawn(async move {
        while let Ok(Some(msg)) = session.messages.message().await {
            let Some(v2::server_message::Msg::Assign(assign)) = msg.msg else {
                continue;
            };
            session
                .control
                .send(v2::WorkerMessage {
                    msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                        lease_id: assign.lease_id.clone(),
                        accepted: true,
                        reject_reason: String::new(),
                    })),
                })
                .await
                .unwrap();

            let mut data = session.data.clone();
            let control = session.control.clone();
            let limits_copy = limits;
            let p8m = Arc::clone(&payload_8m_bg);
            let http_url = gw_http_bg.clone();
            let hib_tx_slot = Arc::clone(&hib_info_tx);

            tokio::spawn(async move {
                match assign.interface.as_str() {
                    "native.echo" => {
                        let mut attempt =
                            Attempt::open(&mut data, &assign, &limits_copy, hb_timeout)
                                .await
                                .unwrap();
                        let out = attempt.inputs.clone();
                        attempt
                            .commit(
                                &mut data,
                                &assign,
                                &limits_copy,
                                &out,
                                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                            )
                            .await
                            .unwrap();
                    }
                    "native.gen8m" => {
                        let link = data
                            .open(assign.lease_id.clone(), assign.generation, 0)
                            .await
                            .unwrap();
                        let info = upload_8mib_with_mid_hibernate(
                            link,
                            assign.inputs_via_attempt,
                            &p8m,
                            limits_copy.max_chunk_bytes as usize,
                            limits_copy.attempt_window_bytes as usize,
                            &http_url,
                        )
                        .await;
                        if let Some(tx) = hib_tx_slot.lock().await.take() {
                            let _ = tx.send(info);
                        }
                    }
                    other => panic!("unexpected interface: {other}"),
                }

                let _ = control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                            free_slots: 1,
                        })),
                    })
                    .await;
            });
        }
    });

    let mut client = ExecutionClient::connect_with_token(&gw.ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();

    // (a) 24 x 1 B small-object bundle
    let objects_24: Vec<v2::Object> = (0u8..24)
        .map(|i| v2::Object {
            body: Some(v2::object::Body::Inline(vec![i].into())),
            metadata: HashMap::from([("idx".to_owned(), i.to_string())]),
        })
        .collect();
    let sub_24 = client
        .submit(
            Some("durable-bundle-24x1b".into()),
            make_spec("native.echo", objects_24.clone(), true, 3),
        )
        .await
        .unwrap();
    assert!(sub_24.created);

    let mut watch_24 = client.watch("durable-bundle-24x1b", 0).await.unwrap();
    let mut fin_24 = None;
    while let Some(ev) = watch_24.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            fin_24 = Some(fin);
        }
    }
    let fin_24 = fin_24.unwrap();
    assert_eq!(fin_24.state, v2::JobState::Succeeded as i32);
    assert_eq!(fin_24.outputs.len(), 24);
    for (i, obj) in fin_24.outputs.iter().enumerate() {
        assert_eq!(
            obj.body,
            Some(v2::object::Body::Inline(vec![i as u8].into()))
        );
        assert_eq!(
            obj.metadata.get("idx").map(String::as_str),
            Some(i.to_string().as_str())
        );
    }
    let got_24 = client.get("durable-bundle-24x1b", true).await.unwrap();
    assert_eq!(got_24.outputs.len(), 24);

    // (b) 64 KiB inline payload (must NOT spill to R2)
    let stats_before_64k = gw.stats().await;
    let r2_puts_before_64k = stats_before_64k["r2_put_count"].as_u64().unwrap();
    let payload_64k = pattern_bytes(64 * 1024, 0x44);
    let sub_64k = client
        .submit(
            Some("durable-inline-64k".into()),
            make_spec(
                "native.echo",
                vec![inline_obj(payload_64k.clone())],
                true,
                3,
            ),
        )
        .await
        .unwrap();
    assert!(sub_64k.created);
    let mut watch_64k = client.watch("durable-inline-64k", 0).await.unwrap();
    while let Some(ev) = watch_64k.message().await.unwrap() {
        if matches!(ev.event, Some(v2::execution_event::Event::Finished(_))) {
            break;
        }
    }
    let got_64k = client.get("durable-inline-64k", true).await.unwrap();
    assert_eq!(
        got_64k.outputs[0].body,
        Some(v2::object::Body::Inline(payload_64k.into()))
    );
    let stats_after_64k = gw.stats().await;
    assert_eq!(
        stats_after_64k["r2_put_count"].as_u64().unwrap(),
        r2_puts_before_64k,
        "64 KiB inline bundle must stay in SQLite without R2 PUTs"
    );

    // (c) 2.5 MiB R2-spilled payload (> 512 KiB bundle threshold -> R2 PUTs for input & output)
    let payload_2_5m = pattern_bytes(2 * 1024 * 1024 + 512 * 1024, 0x55);
    let sha_2_5m = sha256_hex(&payload_2_5m);
    let sub_2_5m = client
        .submit(
            Some("durable-r2-2-5m".into()),
            make_spec(
                "native.echo",
                vec![inline_obj(payload_2_5m.clone())],
                true,
                3,
            ),
        )
        .await
        .unwrap();
    assert!(sub_2_5m.created);
    let mut watch_2_5m = client.watch("durable-r2-2-5m", 0).await.unwrap();
    let mut fin_2_5m = None;
    while let Some(ev) = watch_2_5m.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            fin_2_5m = Some(fin);
        }
    }
    let Some(v2::object::Body::Inline(watch_out_2_5m)) = &fin_2_5m.unwrap().outputs[0].body else {
        panic!("expected inline 2.5 MiB output");
    };
    assert_eq!(sha256_hex(watch_out_2_5m), sha_2_5m);

    let got_2_5m = client.get("durable-r2-2-5m", true).await.unwrap();
    let Some(v2::object::Body::Inline(get_out_2_5m)) = &got_2_5m.outputs[0].body else {
        panic!("expected inline 2.5 MiB output on GetJob");
    };
    assert_eq!(sha256_hex(get_out_2_5m), sha_2_5m);

    let stats_after_2_5m = gw.stats().await;
    assert!(
        stats_after_2_5m["r2_put_count"].as_u64().unwrap() >= r2_puts_before_64k + 2,
        "2.5 MiB input and output must spill to R2"
    );

    // (d) 8 MiB multi-window (> 4 MiB window) result WITH real POST /__test/hibernate mid-upload!
    let expected_8m_sha = sha256_hex(&payload_8m);
    let initial_stats = gw.stats().await;
    let runs_before = initial_stats["constructor_runs"].as_u64().unwrap();
    assert_eq!(runs_before, 1);

    let sub_8m = client
        .submit(
            Some("durable-8m-hibernate".into()),
            make_spec("native.gen8m", vec![inline_obj(b"start".to_vec())], true, 1),
        )
        .await
        .unwrap();
    assert!(sub_8m.created);

    // Open live Watch BEFORE hibernation so the Watch TCP/WS connection also survives hibernation!
    let mut watch_8m = client.watch("durable-8m-hibernate", 0).await.unwrap();
    let mut fin_8m = None;
    while let Some(ev) = watch_8m.message().await.unwrap() {
        if let Some(v2::execution_event::Event::Finished(fin)) = ev.event {
            fin_8m = Some(fin);
        }
    }
    let (hib_runs, prev_boot, new_boot) = hib_info_rx
        .await
        .expect("mid-upload hibernate hook must have executed");
    assert_eq!(hib_runs, runs_before + 1);
    assert_ne!(prev_boot, new_boot);

    let fin_8m = fin_8m.expect("pre-hibernation Watch socket must receive 8 MiB Finished");
    assert_eq!(fin_8m.state, v2::JobState::Succeeded as i32);
    let Some(v2::object::Body::Inline(watch_8m_bytes)) = &fin_8m.outputs[0].body else {
        panic!("expected 8 MiB inline output on Watch");
    };
    assert_eq!(watch_8m_bytes.len(), 8 * 1024 * 1024);
    assert_eq!(sha256_hex(watch_8m_bytes), expected_8m_sha);

    let got_8m = client.get("durable-8m-hibernate", true).await.unwrap();
    assert_eq!(got_8m.state, v2::JobState::Succeeded as i32);
    assert_eq!(got_8m.attempt_count, 1);
    let Some(v2::object::Body::Inline(get_8m_bytes)) = &got_8m.outputs[0].body else {
        panic!("expected 8 MiB inline output on GetJob");
    };
    assert_eq!(get_8m_bytes.len(), 8 * 1024 * 1024);
    assert_eq!(sha256_hex(get_8m_bytes), expected_8m_sha);

    // (e) Cancel + ListJobs
    let cancel_sub = client
        .submit(
            Some("durable-cancel-me".into()),
            make_spec("native.unclaimed", vec![inline_obj(b"x".to_vec())], true, 1),
        )
        .await
        .unwrap();
    assert!(cancel_sub.created);
    let cancelled = client
        .cancel("durable-cancel-me", "operator test cancel")
        .await
        .unwrap();
    assert_eq!(cancelled.state, v2::JobState::Cancelled as i32);

    let listed = client
        .list(v2::ListJobsRequest {
            limit: 20,
            ..Default::default()
        })
        .await
        .unwrap();
    let listed_ids: BTreeSet<&str> = listed.jobs.iter().map(|j| j.job_id.as_str()).collect();
    for expected_id in [
        "durable-bundle-24x1b",
        "durable-inline-64k",
        "durable-r2-2-5m",
        "durable-8m-hibernate",
        "durable-cancel-me",
    ] {
        assert!(
            listed_ids.contains(expected_id),
            "ListJobs missing {expected_id}"
        );
    }

    worker_task.abort();
    gw.stop();
}

// ---------------------------------------------------------------------------
// 4. Role & Namespace Auth (401, 403, Scoped Tenant Isolation on Same Raw ID)
//    + Console /api/overview, /api/jobs/:id (4096B preview), /api/analytics/jobs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn native_do_auth_scoping_namespace_isolation_and_console_apis() {
    let gw = NativeGatewayProc::start().await;
    let http = reqwest::Client::new();

    // (a) Unauthenticated (401 / Code::Unauthenticated) and wrong-role (403 / Code::PermissionDenied)
    let mut unauth_client = ExecutionClient::connect_with_token(&gw.ws_base, None)
        .await
        .unwrap();
    assert_eq!(
        unauth_client
            .list(v2::ListJobsRequest::default())
            .await
            .unwrap_err()
            .code(),
        Code::Unauthenticated
    );
    assert_eq!(
        unauth_client
            .invoke(make_spec("ns.echo", Vec::new(), false, 1))
            .await
            .err()
            .unwrap()
            .code(),
        Code::Unauthenticated
    );

    let mut worker_as_client =
        ExecutionClient::connect_with_token(&gw.ws_base, Some(DEFAULT_WORKER_TOKEN))
            .await
            .unwrap();
    assert_eq!(
        worker_as_client
            .submit(None, make_spec("ns.echo", Vec::new(), false, 1))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
    assert_eq!(
        worker_as_client.watch("any", 0).await.err().unwrap().code(),
        Code::PermissionDenied
    );

    let client_as_worker_err = WorkerSession::connect(
        &gw.ws_base,
        Some(OPERATOR_CLIENT_TOKEN),
        v2::Hello {
            worker_id: "wrong-role-worker".into(),
            interfaces: vec!["ns.echo".into()],
            max_concurrency: 1,
            ..Default::default()
        },
    )
    .await
    .err()
    .expect("client token must fail on worker session");
    assert_eq!(
        client_as_worker_err
            .downcast_ref::<tonic::Status>()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );

    let mut client_as_attempt = AttemptClient::connect(&gw.ws_base, Some(OPERATOR_CLIENT_TOKEN))
        .await
        .unwrap();
    assert_eq!(
        client_as_attempt
            .open("lease-x".into(), 1, 0)
            .await
            .err()
            .unwrap()
            .code(),
        Code::PermissionDenied
    );

    // Console auth: missing token -> 401; scoped tenant -> 403 on /metrics, 200 scoped on /api/overview
    let ov_unauth = http
        .get(format!("{}/api/overview", gw.http_base))
        .send()
        .await
        .unwrap();
    assert_eq!(ov_unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    let metrics_scoped = http
        .get(format!("{}/metrics", gw.http_base))
        .bearer_auth(TENANT_A_CLIENT_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(metrics_scoped.status(), reqwest::StatusCode::FORBIDDEN);

    let ov_scoped = http
        .get(format!("{}/api/overview", gw.http_base))
        .bearer_auth(TENANT_A_CLIENT_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(ov_scoped.status(), reqwest::StatusCode::OK);
    let ov_scoped_json: Value = ov_scoped.json().await.unwrap();
    assert_eq!(ov_scoped_json["scope"]["is_operator"], false);
    assert_eq!(ov_scoped_json["scope"]["namespace"], "tenant-a");

    let cross_ns = http
        .get(format!("{}/api/jobs?namespace=tenant-b", gw.http_base))
        .bearer_auth(TENANT_A_CLIENT_TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(cross_ns.status(), reqwest::StatusCode::FORBIDDEN);

    // (b) Scoped worker + client isolation: `tenant-a` vs `tenant-b` on the SAME raw `job_id`!
    let spawn_echo_worker = |token: &'static str, wid: &'static str| {
        let ws_url = gw.ws_base.clone();
        tokio::spawn(async move {
            let (mut session, welcome) =
                connect_worker_with_token(&ws_url, token, wid, &["ns.echo"]).await;
            let limits = welcome.limits.unwrap();
            let hb_timeout = Duration::from_millis(u64::from(welcome.heartbeat_timeout_ms));
            while let Ok(Some(msg)) = session.messages.message().await {
                let Some(v2::server_message::Msg::Assign(assign)) = msg.msg else {
                    continue;
                };
                session
                    .control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::AssignReply(v2::AssignReply {
                            lease_id: assign.lease_id.clone(),
                            accepted: true,
                            reject_reason: String::new(),
                        })),
                    })
                    .await
                    .unwrap();
                let mut data = session.data.clone();
                let mut attempt = Attempt::open(&mut data, &assign, &limits, hb_timeout)
                    .await
                    .unwrap();
                let out = attempt.inputs.clone();
                attempt
                    .commit(
                        &mut data,
                        &assign,
                        &limits,
                        &out,
                        v2::finish::Outcome::Succeeded(v2::Succeeded {}),
                    )
                    .await
                    .unwrap();
                let _ = session
                    .control
                    .send(v2::WorkerMessage {
                        msg: Some(v2::worker_message::Msg::Credit(v2::Credit {
                            free_slots: 1,
                        })),
                    })
                    .await;
            }
        })
    };

    let worker_a = spawn_echo_worker(TENANT_A_WORKER_TOKEN, "worker-tenant-a");
    let worker_b = spawn_echo_worker(TENANT_B_WORKER_TOKEN, "worker-tenant-b");

    let mut client_a =
        ExecutionClient::connect_with_token(&gw.ws_base, Some(TENANT_A_CLIENT_TOKEN))
            .await
            .unwrap();
    let mut client_b =
        ExecutionClient::connect_with_token(&gw.ws_base, Some(TENANT_B_CLIENT_TOKEN))
            .await
            .unwrap();

    // Both tenants submit the EXACT SAME raw `job_id` ("shared-raw-job-1") with distinct 6000-byte ASCII payloads (> 4096 B)
    let payload_a = vec![b'A'; 6_000];
    let payload_b = vec![b'B'; 6_000];

    let sub_a = client_a
        .submit(
            Some("shared-raw-job-1".into()),
            make_spec("ns.echo", vec![inline_obj(payload_a.clone())], true, 2),
        )
        .await
        .unwrap();
    let sub_b = client_b
        .submit(
            Some("shared-raw-job-1".into()),
            make_spec("ns.echo", vec![inline_obj(payload_b.clone())], true, 2),
        )
        .await
        .unwrap();
    assert!(sub_a.created);
    assert!(sub_b.created);

    // Scoped token passing a spoofed `x-anyserve-namespace: tenant-b` header MUST ignore the header
    // and stay strictly locked to `tenant-a` (parity with `crates/anyserve-core/src/auth.rs`)!
    let mut spoof_a_as_b = ExecutionClient::connect_with_options(
        &gw.ws_base,
        Some(TENANT_A_CLIENT_TOKEN),
        Some("tenant-b"),
    )
    .await
    .unwrap();
    let spoof_listed = spoof_a_as_b
        .list(v2::ListJobsRequest::default())
        .await
        .unwrap();
    assert!(
        spoof_listed.jobs.iter().all(|j| j.namespace == "tenant-a"),
        "scoped token must ignore spoofed x-anyserve-namespace header and remain isolated in tenant-a"
    );

    let mut watch_a = client_a.watch("shared-raw-job-1", 0).await.unwrap();
    while let Some(ev) = watch_a.message().await.unwrap() {
        if matches!(ev.event, Some(v2::execution_event::Event::Finished(_))) {
            break;
        }
    }
    let mut watch_b = client_b.watch("shared-raw-job-1", 0).await.unwrap();
    while let Some(ev) = watch_b.message().await.unwrap() {
        if matches!(ev.event, Some(v2::execution_event::Event::Finished(_))) {
            break;
        }
    }

    let got_a = client_a.get("shared-raw-job-1", true).await.unwrap();
    let got_b = client_b.get("shared-raw-job-1", true).await.unwrap();
    assert_eq!(got_a.namespace, "tenant-a");
    assert_eq!(got_b.namespace, "tenant-b");
    assert_eq!(
        got_a.outputs[0].body,
        Some(v2::object::Body::Inline(payload_a.into()))
    );
    assert_eq!(
        got_b.outputs[0].body,
        Some(v2::object::Body::Inline(payload_b.into()))
    );

    // Submit a non-existent ID check: tenant-a cannot read a job submitted only in tenant-b
    client_b
        .submit(
            Some("only-in-b".into()),
            make_spec("ns.echo", vec![inline_obj(b"secret-b".to_vec())], true, 1),
        )
        .await
        .unwrap();
    assert_eq!(
        client_a.get("only-in-b", true).await.unwrap_err().code(),
        Code::NotFound
    );

    // (c) Console APIs with operator token:
    //     - /api/overview (`recommended_poll_ms: 10000`, `memory_plus_cloudflare`, `public_endpoint`)
    //     - /api/jobs/:id (`4096` B truncated preview)
    //     - /api/analytics/jobs (`retained` & `archive`)
    let overview: Value = http
        .get(format!("{}/api/overview", gw.http_base))
        .bearer_auth(OPERATOR_CLIENT_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(overview["runtime"]["code"], "memory_plus_cloudflare");
    assert_eq!(overview["control_plane"]["recommended_poll_ms"], 10_000);
    assert_eq!(
        overview["control_plane"]["public_endpoint"],
        "wss://native.anyserve.example.com"
    );

    let detail_a: Value = http
        .get(format!(
            "{}/api/jobs/shared-raw-job-1?namespace=tenant-a",
            gw.http_base
        ))
        .bearer_auth(OPERATOR_CLIENT_TOKEN)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(detail_a["job"]["job_id"], "shared-raw-job-1");
    assert_eq!(detail_a["job"]["namespace"], "tenant-a");
    assert_eq!(detail_a["job"]["outputs"][0]["size_bytes"], 6_000);
    assert_eq!(detail_a["job"]["outputs"][0]["truncated"], true);
    let preview_str = detail_a["job"]["outputs"][0]["preview"]
        .as_str()
        .expect("4096-byte preview text");
    assert_eq!(preview_str.len(), 4_096);
    assert!(preview_str.bytes().all(|b| b == b'A'));

    let now_upper = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let from_lower = now_upper.saturating_sub(3_600_000);

    for source in ["retained", "archive"] {
        let analytics: Value = http
            .get(format!(
                "{}/api/analytics/jobs?source={source}&from_ms={from_lower}&to_ms={now_upper}",
                gw.http_base
            ))
            .bearer_auth(OPERATOR_CLIENT_TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let jobs = analytics["jobs"].as_array().expect("analytics jobs array");
        let namespaces_seen: BTreeSet<&str> = jobs
            .iter()
            .filter(|j| j["job_id"] == "shared-raw-job-1")
            .filter_map(|j| j["namespace"].as_str())
            .collect();
        assert!(
            namespaces_seen.contains("tenant-a") && namespaces_seen.contains("tenant-b"),
            "analytics ({source}) must include both tenant-a and tenant-b jobs"
        );
    }

    worker_a.abort();
    worker_b.abort();
    gw.stop();
}
