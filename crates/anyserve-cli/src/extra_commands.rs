use std::collections::HashMap;
use std::io::{self, Read as _, Write as _};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use anyserve_client::execution::{ExecutionClient, v2};
use anyserve_client::{NAMESPACE_ENV, TOKEN_ENV};
use anyserve_core::auth::{
    SIGNING_KEYS_ENV, SignedTokenClaims, generate_token, inspect_signed_token, parse_signing_keys,
    sign_token,
};
use anyserve_core::now_ms;
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as GrpcServingStatus;
use tonic_health::pb::health_client::HealthClient;

use crate::{arg_or_env, process_env};

const LOCAL_TIMEOUT_GRACE: Duration = Duration::from_secs(2);

#[derive(Subcommand, Debug)]
pub(crate) enum TokenCommands {
    /// Mint a stateless HMAC-signed bearer token (`asv1.`).
    Mint(TokenMintArgs),
    /// Inspect an `asv1.` bearer token's claims and optionally verify its signature.
    Inspect(TokenInspectArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum TokenRoleArg {
    Client,
    Worker,
}

impl TokenRoleArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Worker => "worker",
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct TokenMintArgs {
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long, value_enum)]
    role: TokenRoleArg,
    #[arg(long)]
    subject: String,
    #[arg(long)]
    ttl_secs: Option<u64>,
    #[arg(long)]
    forced_pool: Option<String>,
    #[arg(long)]
    jti: Option<String>,
    #[arg(long)]
    signing_key: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Args, Debug)]
pub(crate) struct TokenInspectArgs {
    token: String,
    #[arg(long)]
    signing_key: Option<String>,
}

#[derive(Args, Debug)]
pub(crate) struct InvokeArgs {
    interface: String,
    #[arg(long)]
    input: Option<String>,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    namespace: Option<String>,
    #[arg(long)]
    stream: bool,
    #[arg(long)]
    timeout_secs: Option<u64>,
}

#[derive(Args, Debug)]
pub(crate) struct DoctorArgs {
    #[arg(long)]
    token: Option<String>,
    #[arg(long)]
    json: bool,
}

/// `--signing-key`, else `ANYSERVE_TOKEN_SIGNING_KEY`, parsed like the server does.
fn signing_keys(cli: Option<String>) -> Vec<Vec<u8>> {
    parse_signing_keys(&arg_or_env(cli, SIGNING_KEYS_ENV, process_env).unwrap_or_default())
}

fn mint_token(args: TokenMintArgs) -> Result<(String, SignedTokenClaims, bool)> {
    if args.ttl_secs == Some(0) {
        bail!("--ttl-secs must be positive; omit for a non-expiring token");
    }
    let key = signing_keys(args.signing_key)
        .into_iter()
        .next()
        .with_context(|| format!("signing key requires --signing-key or {SIGNING_KEYS_ENV}"))?;
    let subject = args.subject.trim().to_owned();
    if subject.is_empty() {
        bail!("--subject must not be empty");
    }
    let iat = now_ms() / 1000;
    let claims = SignedTokenClaims {
        jti: crate::serve_config::nonempty_trimmed(args.jti)
            .unwrap_or_else(|| generate_token("jti_")),
        ns: args.namespace.trim().to_owned(),
        role: args.role.as_str().to_owned(),
        sub: subject,
        iat,
        exp: args.ttl_secs.map(|ttl| iat.saturating_add(ttl)),
        fp: crate::serve_config::nonempty_trimmed(args.forced_pool),
    };
    let token = sign_token(&key, &claims)?;
    Ok((token, claims, args.json))
}

fn inspect_token(args: TokenInspectArgs) -> Result<(Value, Option<&'static str>)> {
    let keys = signing_keys(args.signing_key);
    let inspected = inspect_signed_token(args.token.trim(), &keys)?;
    let now_secs = now_ms() / 1000;
    let expired = inspected.claims.exp.is_some_and(|exp| exp <= now_secs);
    let body = json!({
        "claims": inspected.claims,
        "signature_valid": inspected.signature_valid,
        "expired": expired,
    });
    let verify_err = if !keys.is_empty() {
        if inspected.signature_valid == Some(false) {
            Some("token signature verification failed")
        } else if expired {
            Some("token has expired")
        } else {
            None
        }
    } else {
        None
    };
    Ok((body, verify_err))
}

pub(crate) fn run_token_command(command: TokenCommands) -> Result<()> {
    match command {
        TokenCommands::Mint(args) => {
            let (token, claims, as_json) = mint_token(args)?;
            if as_json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({ "token": token, "claims": claims }))?
                );
            } else {
                println!("{token}");
            }
            Ok(())
        }
        TokenCommands::Inspect(args) => {
            let (body, verify_err) = inspect_token(args)?;
            println!("{}", serde_json::to_string_pretty(&body)?);
            if let Some(msg) = verify_err {
                bail!("{msg}");
            }
            Ok(())
        }
    }
}

fn read_invoke_input(raw: Option<&str>, is_json: bool) -> Result<Vec<u8>> {
    let bytes = match raw {
        None => {
            if is_json {
                b"{}".to_vec()
            } else {
                Vec::new()
            }
        }
        Some("-") => {
            let mut buf = Vec::new();
            io::stdin()
                .read_to_end(&mut buf)
                .context("read invoke input from stdin")?;
            buf
        }
        Some(value) if value.starts_with('@') => {
            let path = &value[1..];
            std::fs::read(path).with_context(|| format!("read invoke input file '{path}'"))?
        }
        Some(value) => value.as_bytes().to_vec(),
    };
    if is_json {
        serde_json::from_slice::<serde::de::IgnoredAny>(&bytes)
            .context("--json input must be valid JSON")?;
    }
    Ok(bytes)
}

fn emit_outputs_to(outputs: &[v2::Object], out: &mut impl io::Write) -> Result<()> {
    for obj in outputs {
        match &obj.body {
            Some(v2::object::Body::Inline(bytes)) => {
                out.write_all(bytes)?;
            }
            Some(v2::object::Body::Uri(uri)) => {
                writeln!(out, "{uri}")?;
            }
            None => {}
        }
    }
    out.flush()?;
    Ok(())
}

struct StdoutWriter;

impl io::Write for StdoutWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        io::stdout().lock().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stdout().lock().flush()
    }
}

async fn run_invoke_to_writer<W: io::Write>(
    endpoint: &str,
    args: InvokeArgs,
    out_writer: &mut W,
) -> Result<()> {
    if args.timeout_secs == Some(0) {
        bail!("--timeout-secs must be positive");
    }
    let payload = read_invoke_input(args.input.as_deref(), args.json)?;
    let token = arg_or_env(args.token, TOKEN_ENV, process_env);
    let namespace = arg_or_env(args.namespace, NAMESPACE_ENV, process_env);

    let mut metadata = HashMap::new();
    if args.json {
        metadata.insert("content_type".to_owned(), "application/json".to_owned());
    }
    let timeout_ms = args
        .timeout_secs
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(0);
    let spec = v2::JobSpec {
        demand: Some(v2::Demand {
            interface: args.interface,
            ..Default::default()
        }),
        inputs: vec![v2::Object {
            body: Some(v2::object::Body::Inline(payload.into())),
            metadata,
        }],
        policy: Some(v2::ExecutionPolicy {
            durability: v2::Durability::Ephemeral as i32,
            execution_timeout_ms: timeout_ms,
            total_timeout_ms: timeout_ms,
            ..Default::default()
        }),
        ..Default::default()
    };

    let fut = async {
        let mut client =
            ExecutionClient::connect_with_options(endpoint, token.as_deref(), namespace.as_deref())
                .await?;
        let mut events = client.invoke(spec).await?;
        let mut saw_output_frames = false;
        let mut buffered = Vec::new();
        while let Some(ev) = events.message().await? {
            match ev.event {
                Some(v2::execution_event::Event::Output(out)) => {
                    if out.progress {
                        if tracing::enabled!(tracing::Level::DEBUG) && !out.data.is_empty() {
                            let mut stderr = io::stderr().lock();
                            let _ = stderr.write_all(&out.data);
                            let _ = stderr.flush();
                        }
                        continue;
                    }
                    saw_output_frames = true;
                    if args.stream {
                        out_writer.write_all(&out.data)?;
                        out_writer.flush()?;
                    } else {
                        buffered.extend_from_slice(&out.data);
                    }
                }
                Some(v2::execution_event::Event::AttemptRestarted(_)) if !args.stream => {
                    buffered.clear();
                    saw_output_frames = false;
                }
                Some(v2::execution_event::Event::Finished(fin)) => {
                    if fin.state != v2::JobState::Succeeded as i32 {
                        let msg = fin
                            .error
                            .as_ref()
                            .map(|e| format!("{}: {}", e.code, e.message))
                            .unwrap_or_else(|| format!("execution ended in state {}", fin.state));
                        bail!("{msg}");
                    }
                    if saw_output_frames {
                        if !args.stream {
                            out_writer.write_all(&buffered)?;
                            out_writer.flush()?;
                        }
                    } else {
                        emit_outputs_to(&fin.outputs, out_writer)?;
                    }
                    return Ok(());
                }
                _ => {}
            }
        }
        bail!("execution ended without a terminal event");
    };

    if let Some(secs) = args.timeout_secs {
        let local_budget = Duration::from_secs(secs).saturating_add(LOCAL_TIMEOUT_GRACE);
        tokio::time::timeout(local_budget, fut)
            .await
            .context("invoke timed out")?
    } else {
        fut.await
    }
}

pub(crate) async fn run_invoke_command(endpoint: &str, args: InvokeArgs) -> Result<()> {
    run_invoke_to_writer(endpoint, args, &mut StdoutWriter).await
}

fn normalize_http_base(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("wss://") {
        format!("https://{}", &trimmed[6..])
    } else if lower.starts_with("ws://") {
        format!("http://{}", &trimmed[5..])
    } else if lower.starts_with("https://") {
        format!("https://{}", &trimmed[8..])
    } else if lower.starts_with("http://") {
        format!("http://{}", &trimmed[7..])
    } else {
        trimmed.to_owned()
    }
}

fn is_ws_scheme(endpoint: &str) -> bool {
    let lower = endpoint.trim().to_ascii_lowercase();
    lower.starts_with("ws://") || lower.starts_with("wss://")
}

/// Sends one probe: the response on 2xx, otherwise the failed check's JSON.
async fn probe(req: reqwest::RequestBuilder) -> Result<reqwest::Response, Value> {
    match req.send().await {
        Ok(resp) if resp.status().is_success() => Ok(resp),
        Ok(resp) => Err(json!({ "ok": false, "status": resp.status().as_u16() })),
        Err(err) => Err(json!({ "ok": false, "error": err.to_string() })),
    }
}

async fn probe_grpc_health(endpoint: &str) -> Result<i32, String> {
    let mut ep = Endpoint::from_shared(endpoint.to_owned())
        .map_err(|e| format!("invalid gRPC endpoint: {e}"))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(3));
    if endpoint.starts_with("https://") {
        ep = ep
            .tls_config(ClientTlsConfig::new().with_webpki_roots())
            .map_err(|e| format!("configure gRPC TLS: {e}"))?;
    }
    let channel = ep.connect().await.map_err(|e| e.to_string())?;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .map_err(|s| format!("gRPC health check failed ({}): {}", s.code(), s.message()))?;
    Ok(resp.into_inner().status)
}

fn grpc_serving_status_name(status: i32) -> &'static str {
    match GrpcServingStatus::try_from(status) {
        Ok(GrpcServingStatus::Serving) => "SERVING",
        Ok(GrpcServingStatus::NotServing) => "NOT_SERVING",
        Ok(GrpcServingStatus::ServiceUnknown) => "SERVICE_UNKNOWN",
        _ => "UNKNOWN",
    }
}

struct EndpointDoctorReport {
    endpoint: String,
    healthy: bool,
    checks: serde_json::Map<String, Value>,
}

impl EndpointDoctorReport {
    fn to_json(&self) -> Value {
        json!({
            "endpoint": self.endpoint,
            "healthy": self.healthy,
            "checks": self.checks,
        })
    }
}

async fn probe_single_endpoint(
    http: &reqwest::Client,
    raw_endpoint: &str,
    token: Option<&str>,
) -> EndpointDoctorReport {
    let is_ws = is_ws_scheme(raw_endpoint);
    let base = normalize_http_base(raw_endpoint);

    let (healthz, readyz, info) = tokio::join!(
        probe(http.get(format!("{base}/healthz"))),
        probe(http.get(format!("{base}/readyz"))),
        probe(http.get(format!("{base}/api/info"))),
    );

    // For http(s):// endpoints that fail HTTP /healthz (e.g., gRPC-only listener
    // on default 127.0.0.1:50052), fall back to gRPC health checking.
    if !is_ws
        && healthz.is_err()
        && let Ok(status) = probe_grpc_health(&base).await
    {
        let serving = status == GrpcServingStatus::Serving as i32;
        let mut checks = serde_json::Map::new();
        checks.insert(
            "grpc_health".into(),
            json!({ "ok": serving, "status": grpc_serving_status_name(status) }),
        );
        return EndpointDoctorReport {
            endpoint: base,
            healthy: serving,
            checks,
        };
    }

    let mut checks = serde_json::Map::new();
    let mut healthy = true;
    for (name, result) in [("healthz", healthz), ("readyz", readyz)] {
        let check = match result {
            Ok(resp) => json!({ "ok": true, "status": resp.status().as_u16() }),
            Err(failed) => {
                healthy = false;
                failed
            }
        };
        checks.insert(name.into(), check);
    }

    // A missing /api/info only skips the auth hint; an invalid JSON 200 body fails the check.
    let mut auth_enabled = None;
    let info_check = match info {
        Ok(resp) => {
            let status = resp.status().as_u16();
            match resp.json::<Value>().await {
                Ok(data) => {
                    auth_enabled = data.get("auth_enabled").and_then(Value::as_bool);
                    json!({ "ok": true, "status": status, "data": data })
                }
                Err(err) => {
                    healthy = false;
                    json!({ "ok": false, "status": status, "error": format!("invalid JSON response: {err}") })
                }
            }
        }
        Err(failed) => failed,
    };
    checks.insert("info".into(), info_check);

    if token.is_some() || auth_enabled == Some(false) {
        let mut req = http.get(format!("{base}/api/overview"));
        if let Some(tok) = token {
            req = req.bearer_auth(tok);
        }
        let check = match probe(req).await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                match resp.json::<Value>().await {
                    Ok(body) => {
                        let workers = body.get("workers").cloned().unwrap_or(Value::Null);
                        json!({ "ok": true, "status": status, "workers": workers })
                    }
                    Err(err) => {
                        healthy = false;
                        json!({ "ok": false, "status": status, "error": format!("invalid JSON response: {err}") })
                    }
                }
            }
            Err(failed) => {
                healthy = false;
                failed
            }
        };
        checks.insert("overview".into(), check);
    }

    EndpointDoctorReport {
        endpoint: base,
        healthy,
        checks,
    }
}

async fn evaluate_doctor(
    endpoint: &str,
    token: Option<&str>,
) -> Result<(bool, Value, Vec<EndpointDoctorReport>)> {
    let raw_endpoints: Vec<&str> = endpoint
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if raw_endpoints.is_empty() {
        bail!("endpoint must not be empty");
    }
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build HTTP probe client")?;

    let mut reports = Vec::with_capacity(raw_endpoints.len());
    for raw in raw_endpoints {
        reports.push(probe_single_endpoint(&http, raw, token).await);
    }
    let healthy = reports.iter().all(|r| r.healthy);
    let json_report = if reports.len() == 1 {
        reports[0].to_json()
    } else {
        json!({
            "healthy": healthy,
            "endpoints": reports.iter().map(EndpointDoctorReport::to_json).collect::<Vec<_>>(),
        })
    };
    Ok((healthy, json_report, reports))
}

pub(crate) async fn run_doctor_command(endpoint: &str, args: DoctorArgs) -> Result<()> {
    let token = arg_or_env(args.token, TOKEN_ENV, process_env);
    let (healthy, json_report, reports) = evaluate_doctor(endpoint, token.as_deref()).await?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&json_report)?);
    } else {
        for report in &reports {
            println!("Endpoint: {}", report.endpoint);
            for (name, val) in &report.checks {
                let ok = val.get("ok").and_then(Value::as_bool).unwrap_or(false);
                let tag = if ok { "ok" } else { "FAIL" };
                println!("[{tag}] {name}: {val}");
            }
        }
    }
    if !healthy {
        bail!("doctor health checks failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Server, Worker};
    use anyserve_client::execution::Attempt;
    use anyserve_core::auth::AuthPolicy;
    use anyserve_core::runtime::api::RuntimeConfig;
    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::Redirect;
    use axum::routing::get;
    use tokio_stream::wrappers::TcpListenerStream;

    #[test]
    fn token_mint_and_inspect_round_trip_and_validation() {
        let key = "test-signing-key-0123456789abcdef";
        let (minted, claims, _) = mint_token(TokenMintArgs {
            namespace: " team-z ".into(),
            role: TokenRoleArg::Client,
            subject: " ci-bot ".into(),
            ttl_secs: Some(600),
            forced_pool: Some(" gpu-t4 ".into()),
            jti: Some(" jti-fixed ".into()),
            signing_key: Some(key.into()),
            json: false,
        })
        .unwrap();
        assert_eq!(claims.ns, "team-z");
        assert_eq!(claims.sub, "ci-bot");
        assert_eq!(claims.jti, "jti-fixed");
        assert_eq!(claims.fp.as_deref(), Some("gpu-t4"));

        let (inspected, err) = inspect_token(TokenInspectArgs {
            token: minted.clone(),
            signing_key: Some(key.into()),
        })
        .unwrap();
        assert!(err.is_none());
        assert_eq!(inspected["signature_valid"], true);
        assert_eq!(inspected["expired"], false);

        // Zero TTL and blank subject are rejected.
        let err = mint_token(TokenMintArgs {
            namespace: "default".into(),
            role: TokenRoleArg::Client,
            subject: "ci-bot".into(),
            ttl_secs: Some(0),
            forced_pool: None,
            jti: None,
            signing_key: Some(key.into()),
            json: false,
        })
        .unwrap_err();
        assert!(
            err.to_string().contains("--ttl-secs must be positive"),
            "{err}"
        );

        let err = mint_token(TokenMintArgs {
            namespace: "default".into(),
            role: TokenRoleArg::Client,
            subject: "   ".into(),
            ttl_secs: None,
            forced_pool: None,
            jti: None,
            signing_key: Some(key.into()),
            json: false,
        })
        .unwrap_err();
        assert!(err.to_string().contains("--subject"), "{err}");

        // Wrong signing key fails verification.
        let err = run_token_command(TokenCommands::Inspect(TokenInspectArgs {
            token: minted,
            signing_key: Some("wrong-key".into()),
        }))
        .unwrap_err();
        assert!(err.to_string().contains("verification failed"), "{err}");

        // Expired token reports expired=true and fails when verified with signing key.
        let expired_tok = sign_token(
            key.as_bytes(),
            &SignedTokenClaims {
                jti: "jti-exp".into(),
                ns: "team-z".into(),
                role: "worker".into(),
                sub: "w1".into(),
                iat: 1_000,
                exp: Some(2_000),
                fp: None,
            },
        )
        .unwrap();
        let (unverified, unverified_err) = inspect_token(TokenInspectArgs {
            token: expired_tok.clone(),
            signing_key: None,
        })
        .unwrap();
        assert_eq!(unverified["expired"], true);
        assert!(unverified_err.is_none());

        let (verified, verified_err) = inspect_token(TokenInspectArgs {
            token: expired_tok.clone(),
            signing_key: Some(key.into()),
        })
        .unwrap();
        assert_eq!(verified["signature_valid"], true);
        assert_eq!(verified["expired"], true);
        assert_eq!(verified_err, Some("token has expired"));

        let cli_err = run_token_command(TokenCommands::Inspect(TokenInspectArgs {
            token: expired_tok,
            signing_key: Some(key.into()),
        }))
        .unwrap_err();
        assert!(cli_err.to_string().contains("expired"), "{cli_err}");
    }

    #[test]
    fn invoke_input_parses_inline_json_and_at_file_and_normalizes_http_base() {
        assert_eq!(
            normalize_http_base("wss://a.example.com/"),
            "https://a.example.com"
        );
        assert_eq!(
            normalize_http_base("WSS://A.Example.Com/"),
            "https://A.Example.Com"
        );
        assert_eq!(
            normalize_http_base("WS://localhost:3001"),
            "http://localhost:3001"
        );

        assert_eq!(read_invoke_input(Some("hello"), false).unwrap(), b"hello");
        assert!(read_invoke_input(Some("{bad json"), true).is_err());

        let path = crate::test_support::temporary_path("invoke-input.json");
        std::fs::write(&path, br#"{"x":1}"#).unwrap();
        let at = format!("@{}", path.display());
        assert_eq!(read_invoke_input(Some(&at), true).unwrap(), br#"{"x":1}"#);
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn invoke_handles_streaming_progress_unary_fallback_metadata_failure_and_zero_timeout() {
        // --timeout-secs 0 is rejected before connecting.
        let mut sink = Vec::new();
        let err = run_invoke_to_writer(
            "http://127.0.0.1:1",
            InvokeArgs {
                interface: "demo.v1".into(),
                input: Some("hi".into()),
                json: false,
                token: None,
                namespace: None,
                stream: false,
                timeout_secs: Some(0),
            },
            &mut sink,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("--timeout-secs must be positive"),
            "{err}"
        );

        let server = Server::start(RuntimeConfig::default(), AuthPolicy::default()).await;
        let mut worker = Worker::connect(&server, "w-inv", vec!["demo.inv.v1".into()], 2).await;

        // 1. Unary invoke against a streaming worker captures non-progress frames and sets content_type metadata.
        let ep = server.endpoint.clone();
        let invoke_task = tokio::spawn(async move {
            let mut out = Vec::new();
            run_invoke_to_writer(
                &ep,
                InvokeArgs {
                    interface: "demo.inv.v1".into(),
                    input: Some(r#"{"k":1}"#.into()),
                    json: true,
                    token: None,
                    namespace: None,
                    stream: false,
                    timeout_secs: Some(5),
                },
                &mut out,
            )
            .await
            .unwrap();
            out
        });
        let assign = worker.assignment().await;
        let mut attempt = Attempt::open(
            &mut worker.session.data,
            &assign,
            &worker.limits,
            worker.heartbeat_timeout,
        )
        .await
        .unwrap();
        assert_eq!(
            attempt.inputs[0]
                .metadata
                .get("content_type")
                .map(String::as_str),
            Some("application/json")
        );
        assert!(!attempt.inputs[0].metadata.contains_key("content-type"));
        attempt
            .frame(b"progress-ignore".to_vec().into(), HashMap::new(), true)
            .await
            .unwrap();
        attempt
            .frame(b"chunk-1-".to_vec().into(), HashMap::new(), false)
            .await
            .unwrap();
        attempt
            .frame(b"chunk-2".to_vec().into(), HashMap::new(), false)
            .await
            .unwrap();
        attempt
            .finish(&[], v2::finish::Outcome::Succeeded(v2::Succeeded {}))
            .await
            .unwrap();
        let unary_out = invoke_task.await.unwrap();
        assert_eq!(unary_out, b"chunk-1-chunk-2");

        // 2. --stream ignores progress=true frames and does not double-print Finished.outputs.
        worker.credit(2).await;
        let ep = server.endpoint.clone();
        let stream_task = tokio::spawn(async move {
            let mut out = Vec::new();
            run_invoke_to_writer(
                &ep,
                InvokeArgs {
                    interface: "demo.inv.v1".into(),
                    input: Some("go".into()),
                    json: false,
                    token: None,
                    namespace: None,
                    stream: true,
                    timeout_secs: Some(5),
                },
                &mut out,
            )
            .await
            .unwrap();
            out
        });
        let assign = worker.assignment().await;
        let mut attempt = Attempt::open(
            &mut worker.session.data,
            &assign,
            &worker.limits,
            worker.heartbeat_timeout,
        )
        .await
        .unwrap();
        attempt
            .frame(b"50%".to_vec().into(), HashMap::new(), true)
            .await
            .unwrap();
        attempt
            .frame(b"streamed".to_vec().into(), HashMap::new(), false)
            .await
            .unwrap();
        attempt
            .finish(
                &[v2::Object {
                    body: Some(v2::object::Body::Inline(b"streamed".to_vec().into())),
                    metadata: HashMap::new(),
                }],
                v2::finish::Outcome::Succeeded(v2::Succeeded {}),
            )
            .await
            .unwrap();
        assert_eq!(stream_task.await.unwrap(), b"streamed");

        // 3. Failed job returns error cleanly.
        worker.credit(2).await;
        let ep = server.endpoint.clone();
        let fail_task = tokio::spawn(async move {
            let mut out = Vec::new();
            run_invoke_to_writer(
                &ep,
                InvokeArgs {
                    interface: "demo.inv.v1".into(),
                    input: Some("bad".into()),
                    json: false,
                    token: None,
                    namespace: None,
                    stream: true,
                    timeout_secs: Some(5),
                },
                &mut out,
            )
            .await
        });
        let assign = worker.assignment().await;
        let mut attempt = Attempt::open(
            &mut worker.session.data,
            &assign,
            &worker.limits,
            worker.heartbeat_timeout,
        )
        .await
        .unwrap();
        attempt
            .finish(
                &[],
                v2::finish::Outcome::Failed(v2::ErrorInfo {
                    code: "boom".into(),
                    message: "worker exploded".into(),
                    retryable: false,
                }),
            )
            .await
            .unwrap();
        let err = fail_task.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("boom: worker exploded"), "{err}");

        server.stop().await;
    }

    async fn spawn_http(router: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn doctor_checks_healthy_readyz_redirect_invalid_json_grpc_fallback_and_multi_endpoint() {
        // 1. Healthy console HTTP endpoint.
        let (ok_ep, h_ok) = spawn_http(
            Router::new()
                .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
                .route("/readyz", get(|| async { StatusCode::NO_CONTENT }))
                .route(
                    "/api/info",
                    get(|| async { axum::Json(json!({ "auth_enabled": false })) }),
                )
                .route(
                    "/api/overview",
                    get(|| async { axum::Json(json!({ "workers": { "healthy": 2 } })) }),
                ),
        )
        .await;
        let (healthy, report, _) = evaluate_doctor(&ok_ep, None).await.unwrap();
        assert!(healthy, "{report}");
        assert_eq!(report["endpoint"], ok_ep);
        assert_eq!(report["checks"]["overview"]["workers"]["healthy"], 2);

        // 2. 503 /readyz fails doctor.
        let (unready_ep, h_unready) = spawn_http(
            Router::new()
                .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
                .route("/readyz", get(|| async { StatusCode::SERVICE_UNAVAILABLE }))
                .route(
                    "/api/info",
                    get(|| async { axum::Json(json!({ "auth_enabled": true })) }),
                ),
        )
        .await;
        let (healthy, report, _) = evaluate_doctor(&unready_ep, None).await.unwrap();
        assert!(!healthy, "{report}");
        assert_eq!(report["checks"]["readyz"]["status"], 503);

        // 3. 3xx redirect (e.g. SSO login redirect) must NOT be treated as healthy.
        let (redir_ep, h_redir) = spawn_http(
            Router::new()
                .route("/healthz", get(|| async { Redirect::temporary("/login") }))
                .route("/readyz", get(|| async { Redirect::temporary("/login") }))
                .route("/login", get(|| async { StatusCode::OK })),
        )
        .await;
        let (healthy, report, _) = evaluate_doctor(&redir_ep, None).await.unwrap();
        assert!(!healthy, "{report}");
        assert_eq!(report["checks"]["healthz"]["status"], 307);

        // 4. Invalid JSON on /api/overview fails doctor.
        let (bad_json_ep, h_bad_json) = spawn_http(
            Router::new()
                .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
                .route("/readyz", get(|| async { StatusCode::NO_CONTENT }))
                .route(
                    "/api/info",
                    get(|| async { axum::Json(json!({ "auth_enabled": false })) }),
                )
                .route("/api/overview", get(|| async { "<html>login</html>" })),
        )
        .await;
        let (healthy, report, _) = evaluate_doctor(&bad_json_ep, None).await.unwrap();
        assert!(!healthy, "{report}");
        assert_eq!(report["checks"]["overview"]["ok"], false);
        assert!(
            report["checks"]["overview"]["error"]
                .as_str()
                .unwrap()
                .contains("invalid JSON")
        );

        // 5. gRPC-only server passes via gRPC health fallback.
        let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc_addr = grpc_listener.local_addr().unwrap();
        let (mut reporter, health_service) = tonic_health::server::health_reporter();
        reporter
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;
        let h_grpc = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(health_service)
                .serve_with_incoming(TcpListenerStream::new(grpc_listener))
                .await
                .unwrap();
        });
        let grpc_ep = format!("http://{grpc_addr}");
        let (healthy, report, _) = evaluate_doctor(&grpc_ep, None).await.unwrap();
        assert!(healthy, "{report}");
        assert_eq!(report["checks"]["grpc_health"]["ok"], true);
        assert_eq!(report["checks"]["grpc_health"]["status"], "SERVING");

        // 6. Comma-separated endpoints return { healthy, endpoints: [...] }.
        let multi = format!(" {ok_ep} , {grpc_ep} ");
        let (healthy, report, _) = evaluate_doctor(&multi, None).await.unwrap();
        assert!(healthy, "{report}");
        let eps = report["endpoints"].as_array().unwrap();
        assert_eq!(eps.len(), 2);
        assert_eq!(eps[0]["healthy"], true);
        assert_eq!(eps[1]["healthy"], true);

        let mixed = format!("{ok_ep},{unready_ep}");
        let (healthy, report, _) = evaluate_doctor(&mixed, None).await.unwrap();
        assert!(!healthy, "{report}");
        assert_eq!(report["endpoints"][0]["healthy"], true);
        assert_eq!(report["endpoints"][1]["healthy"], false);

        for h in [h_ok, h_unready, h_redir, h_bad_json, h_grpc] {
            h.abort();
        }
    }
}
