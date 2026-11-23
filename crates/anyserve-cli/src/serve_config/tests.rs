use std::fs;
use std::time::Duration;

use super::{ResolvedServeConfig, RuntimeMode, ServeOverrides};

fn config(raw: &str, mut overrides: ServeOverrides) -> anyhow::Result<ResolvedServeConfig> {
    let path = crate::test_support::temporary_path("config").with_extension("toml");
    fs::write(&path, raw)?;
    overrides.config = Some(path.clone());
    let result = ResolvedServeConfig::load(overrides);
    fs::remove_file(path)?;
    result
}

#[test]
fn defaults_enable_a_local_full_sync_journal() {
    for resolved in [
        ResolvedServeConfig::load(ServeOverrides::default()).unwrap(),
        config("", ServeOverrides::default()).unwrap(),
        config(
            "[server]\n[server.tls]\n[runtime]\n[journal]\n[auth]",
            ServeOverrides::default(),
        )
        .unwrap(),
    ] {
        assert_eq!(resolved.grpc.host, "127.0.0.1");
        assert_eq!(resolved.grpc.port, 50_052);
        assert!(resolved.grpc.enabled);
        assert!(!resolved.grpc.tls_enabled);
        let journal = resolved.runtime.journal.unwrap();
        assert_eq!(
            journal.path,
            std::path::Path::new(".anyserve/journal.sqlite")
        );
        assert!(journal.full_sync);
        assert!(resolved.console.is_none());
        assert!(!resolved.auth.is_enabled());
    }
    let resolved = config("[console]\n[auth]", ServeOverrides::default()).unwrap();
    let console = resolved.console.unwrap();
    assert_eq!(console.listen, "127.0.0.1:3001");
    assert!(console.allowed_origins.is_empty());
}

#[test]
fn cli_overrides_tls_fields_without_clearing_other_file_settings() {
    let resolved = config(
        "[server]\ngrpc_host='0.0.0.0'\n[server.tls]\nenabled=true\ncert_file='file.crt'\nkey_file='file.key'",
        ServeOverrides {
            grpc_host: Some("127.0.0.2".into()),
            grpc_tls_enabled: Some(false),
            grpc_cert_file: Some("cli.crt".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(resolved.grpc.host, "127.0.0.2");
    assert_eq!(resolved.grpc.port, 50_052);
    assert!(!resolved.grpc.tls_enabled);
    assert_eq!(resolved.grpc.cert_file.as_deref(), Some("cli.crt"));
    assert_eq!(resolved.grpc.key_file.as_deref(), Some("file.key"));
}

#[test]
fn cli_can_select_ephemeral_mode_or_override_journal_path() {
    let resolved = config(
        "[journal]\npath='file.sqlite'",
        ServeOverrides {
            ephemeral: true,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(resolved.runtime.journal.is_none());
    let resolved = config(
        "[journal]\nenabled=false",
        ServeOverrides {
            journal_path: Some("override.sqlite".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        resolved.runtime.journal.unwrap().path,
        std::path::Path::new("override.sqlite")
    );
}

#[test]
fn file_configures_runtime_and_tls_and_cli_overrides_listener() {
    let resolved = config(
        r#"
[server]
grpc_host = "0.0.0.0"
grpc_port = 50062
[server.tls]
enabled = true
cert_file = "/tmp/cert.pem"
key_file = "/tmp/key.pem"
[runtime]
heartbeat_timeout_ms = 45000
max_attempts = 5
max_pending_bytes = 1048576
[runtime.interfaces."image.v1"]
max_queue = 3
queue_timeout_ms = 250
[journal]
path = "recovery.sqlite"
queue_capacity = 32
max_batch = 8
"#,
        ServeOverrides {
            grpc_port: Some(50072),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(resolved.grpc.port, 50072);
    assert_eq!(resolved.grpc.host, "0.0.0.0");
    assert!(resolved.grpc.tls_enabled);
    assert_eq!(resolved.grpc.cert_file.as_deref(), Some("/tmp/cert.pem"));
    assert_eq!(resolved.runtime.heartbeat_timeout, Duration::from_secs(45));
    assert_eq!(resolved.runtime.default_max_attempts, 5);
    assert_eq!(resolved.runtime.interfaces["image.v1"].max_queue, Some(3));
    assert_eq!(
        resolved.runtime.interfaces["image.v1"].queue_timeout,
        Some(Duration::from_millis(250))
    );
    assert_eq!(resolved.runtime.journal.unwrap().max_batch, 8);
}

#[test]
fn invalid_journal_limits_are_rejected() {
    for raw in [
        "[journal]\nqueue_capacity=0",
        "[journal]\nmax_batch=0",
        "[journal]\npath=''",
    ] {
        assert!(
            config(raw, ServeOverrides::default())
                .unwrap_err()
                .to_string()
                .contains("must be non-empty/positive")
        );
    }
}

#[test]
fn unknown_keys_and_sections_are_rejected() {
    for raw in [
        "[server]\nheartbeat_ttl_secs=30",
        "[server.storage]\nbackend='sqlite'",
        "[gateway]\nenabled=true",
        "[cluster]\npeer_token='cluster-secret'",
        "[journal]\ninternal_grpc_addr='http://127.0.0.1:50052'",
    ] {
        let message = format!("{:#}", config(raw, ServeOverrides::default()).unwrap_err());
        assert!(message.contains("unknown field"), "{message}");
    }
}

#[test]
fn all_repository_example_and_benchmark_toml_files_parse() -> anyhow::Result<()> {
    use anyhow::Context;
    use std::path::{Path, PathBuf};

    fn visit(directory: &Path, files: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // Benchmark output preserves configurations from older releases.
            if name.starts_with('.')
                || matches!(name.as_ref(), "target" | "node_modules" | "results")
            {
                continue;
            }
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                visit(&path, files)?;
            } else if path
                .extension()
                .is_some_and(|extension| extension == "toml")
            {
                files.push(path);
            }
        }
        Ok(())
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    visit(&root.join("examples"), &mut files)?;
    visit(&root.join("bench"), &mut files)?;
    files.sort();
    let mut servers = 0;
    for path in files {
        let raw = fs::read_to_string(&path)?;
        toml::from_str::<toml::Value>(&raw).with_context(|| format!("parse {}", path.display()))?;
        if matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some("Cargo.toml" | "pyproject.toml")
        ) {
            continue;
        }
        super::file::AnyserveConfigFile::load(&path)
            .with_context(|| format!("server schema {}", path.display()))?;
        servers += 1;
    }
    assert!(servers > 0);
    Ok(())
}

#[test]
fn cloudflare_profile_parses_grpc_disabled_public_endpoint_limits_and_mode() {
    let resolved = config(
        r#"
[server]
grpc_enabled = false

[console]
enabled = true
listen = "0.0.0.0:3001"
public_endpoint = "wss://anyserve.example.com"

[journal]
enabled = true
backend = "cloudflare"
endpoint = "http://journal.internal"
queue_capacity = 1024
max_batch = 64
retention_secs = 604800

[runtime]
max_inline_object_bytes = 8388608
max_inline_total_bytes = 25165824
"#,
        ServeOverrides::default(),
    )
    .unwrap();

    assert!(!resolved.grpc.enabled);
    assert_eq!(resolved.runtime_mode(), RuntimeMode::MemoryPlusCloudflare);
    assert_eq!(
        RuntimeMode::MemoryPlusCloudflare.code(),
        "memory_plus_cloudflare"
    );
    assert_eq!(
        RuntimeMode::MemoryPlusCloudflare.label(),
        "Memory + Cloudflare journal"
    );

    let console = resolved.console.unwrap();
    assert_eq!(console.listen, "0.0.0.0:3001");
    assert_eq!(
        console.public_endpoint.as_deref(),
        Some("wss://anyserve.example.com")
    );

    assert_eq!(resolved.runtime.limits.max_inline_object_bytes, 8_388_608);
    assert_eq!(resolved.runtime.limits.max_inline_total_bytes, 25_165_824);

    let journal = resolved.runtime.journal.unwrap();
    let cf = journal.cloudflare.unwrap();
    assert_eq!(cf.endpoint, "http://journal.internal");
    assert_eq!(journal.queue_capacity, 1024);
    assert_eq!(journal.max_batch, 64);
}

#[test]
fn resolved_serve_config_debug_redacts_all_auth_tokens() {
    let resolved = config(
        r#"
[auth]
client_tokens = ["raw-client-secret-abc"]
worker_tokens = ["raw-worker-secret-def"]

[[auth.scoped_tokens]]
token = "raw-scoped-secret-ghi"
role = "worker"
namespace = "tenant-z"
subject = "edge-node-1"
forced_pool = "gpu-pool"
"#,
        ServeOverrides::default(),
    )
    .unwrap();

    let dbg = format!("{resolved:?}");
    assert!(!dbg.contains("raw-client-secret-abc"));
    assert!(!dbg.contains("raw-worker-secret-def"));
    assert!(!dbg.contains("raw-scoped-secret-ghi"));
    assert!(dbg.contains("[redacted]"));
    assert!(dbg.contains("tenant-z"));
    assert!(dbg.contains("edge-node-1"));
    assert!(dbg.contains("gpu-pool"));
}

#[test]
fn runtime_interface_keys_are_bounded_to_64_non_whitespace_bytes() {
    let ok_name = format!("domain.action.{}.v1", "a".repeat(47));
    assert_eq!(ok_name.len(), 64);
    let resolved = config(
        &format!("[runtime.interfaces.\"{ok_name}\"]\nmax_queue = 5\n"),
        ServeOverrides::default(),
    )
    .unwrap();
    assert_eq!(resolved.runtime.interfaces[&ok_name].max_queue, Some(5));

    let too_long = format!("domain.action.{}.v1", "a".repeat(48));
    assert_eq!(too_long.len(), 65);
    for bad in [
        format!("[runtime.interfaces.\"{too_long}\"]\nmax_queue = 5\n"),
        "[runtime.interfaces.\"bad space\"]\nmax_queue = 5\n".to_string(),
        "[runtime.interfaces.\"\"]\nmax_queue = 5\n".to_string(),
    ] {
        let err = config(&bad, ServeOverrides::default()).unwrap_err();
        assert!(err.to_string().contains("1..=64 bytes"), "{err:#}");
    }
}

#[test]
fn cli_console_listen_enables_console_or_overrides_config_file() {
    let without_file = ResolvedServeConfig::load(ServeOverrides {
        console_listen: Some("0.0.0.0:4010".into()),
        ..Default::default()
    })
    .unwrap();
    let console = without_file.console.unwrap();
    assert_eq!(console.listen, "0.0.0.0:4010");
    assert!(console.allowed_origins.is_empty());

    let over_disabled = config(
        "[console]\nenabled = false\nlisten = '127.0.0.1:3001'\n",
        ServeOverrides {
            console_listen: Some("127.0.0.1:3999".into()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(over_disabled.console.unwrap().listen, "127.0.0.1:3999");

    let over_enabled = config(
        "[console]\nlisten = '127.0.0.1:3001'\nallowed_origins = ['http://localhost:5173']\n",
        ServeOverrides {
            console_listen: Some("127.0.0.1:3888".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let overridden = over_enabled.console.unwrap();
    assert_eq!(overridden.listen, "127.0.0.1:3888");
    assert_eq!(overridden.allowed_origins, vec!["http://localhost:5173"]);
}
