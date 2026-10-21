use std::fs;
use std::time::Duration;

use super::{ResolvedServeConfig, ServeOverrides};

fn config(raw: &str, mut overrides: ServeOverrides) -> anyhow::Result<ResolvedServeConfig> {
    let path = std::env::temp_dir().join(format!(
        "anyserve-config-{}-{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::write(&path, raw)?;
    overrides.config = Some(path.clone());
    let result = ResolvedServeConfig::load(overrides);
    fs::remove_file(path)?;
    result
}

#[test]
fn defaults_enable_a_local_full_sync_journal() {
    let resolved = ResolvedServeConfig::load(ServeOverrides::default()).unwrap();
    assert_eq!(resolved.grpc.host, "127.0.0.1");
    assert_eq!(resolved.grpc.port, 50_052);
    let journal = resolved.runtime.journal.unwrap();
    assert_eq!(
        journal.path,
        std::path::Path::new(".anyserve/journal.sqlite")
    );
    assert!(journal.full_sync);
    assert!(resolved.console.is_none());
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
fn obsolete_storage_configuration_requires_explicit_migration() {
    for raw in [
        "[server.storage]\nbackend='postgres'",
        "[server.frames]\nbackend='redis'",
    ] {
        let error = config(raw, ServeOverrides::default()).unwrap_err();
        assert!(error.to_string().contains("replaced by [journal]"));
    }
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
fn legacy_server_keys_point_to_the_existing_migration_section() {
    for raw in [
        "[server]\nheartbeat_ttl_secs=30",
        "[server]\ndefault_lease_ttl_secs=30",
        "[server.storage]\nbackend='sqlite'",
    ] {
        let message = format!("{:#}", config(raw, ServeOverrides::default()).unwrap_err());
        assert!(
            message.contains("docs/src/deployment.md#migration-from-04"),
            "{message}"
        );
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
fn removed_execution_gateway_sections_are_rejected() {
    for raw in ["[openai]\nenabled=true", "[invoke]\nenabled=true"] {
        let error = format!("{:#}", config(raw, ServeOverrides::default()).unwrap_err());
        assert!(error.contains("unknown field"), "{error}");
    }
}
