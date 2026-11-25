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

fn config_with_env(
    raw: &str,
    mut overrides: ServeOverrides,
    env: impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<ResolvedServeConfig> {
    let path = crate::test_support::temporary_path("config-env").with_extension("toml");
    fs::write(&path, raw)?;
    overrides.config = Some(path.clone());
    let result = ResolvedServeConfig::load_with_env(overrides, env);
    fs::remove_file(path)?;
    result
}

fn load_no_env(overrides: ServeOverrides) -> ResolvedServeConfig {
    ResolvedServeConfig::load_with_env(overrides, |_| None).unwrap()
}

fn assert_auto_token_shape(token: &str, expected_prefix: &str) {
    let hex = token
        .strip_prefix(expected_prefix)
        .unwrap_or_else(|| panic!("token {token:?} missing prefix {expected_prefix:?}"));
    assert_eq!(hex.len(), 32);
    assert!(hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')));
}

#[test]
fn cli_tokens_and_env_fallbacks_configure_auth_without_config_file() {
    use anyserve_core::auth::Role;

    let from_cli = load_no_env(ServeOverrides {
        client_tokens: vec!["cli-c1".into(), " cli-c2 , cli-c3 , ".into()],
        worker_tokens: vec!["cli-w1,cli-w2".into(), "  ".into()],
        ..Default::default()
    });
    assert!(from_cli.auth.is_enabled());
    assert!(from_cli.auto_tokens.is_none());
    for (tok, role) in [
        ("cli-c1", Role::Client),
        ("cli-c2", Role::Client),
        ("cli-c3", Role::Client),
        ("cli-w1", Role::Worker),
        ("cli-w2", Role::Worker),
    ] {
        assert_eq!(from_cli.auth.role_for_token(tok), Some(role));
    }

    let from_env =
        ResolvedServeConfig::load_with_env(ServeOverrides::default(), |name| match name {
            "ANYSERVE_CLIENT_TOKENS" => Some("env-c1, env-c2".into()),
            "ANYSERVE_WORKER_TOKENS" => Some("env-w1".into()),
            _ => None,
        })
        .unwrap();
    assert!(from_env.auth.is_enabled());
    assert_eq!(from_env.auth.role_for_token("env-c1"), Some(Role::Client));
    assert_eq!(from_env.auth.role_for_token("env-c2"), Some(Role::Client));
    assert_eq!(from_env.auth.role_for_token("env-w1"), Some(Role::Worker));
}

#[test]
fn cli_public_endpoint_overrides_env_and_toml_and_works_without_console() {
    let env_fn =
        |name: &str| (name == "ANYSERVE_PUBLIC_ENDPOINT").then(|| "wss://env.example.com".into());

    let overridden = config_with_env(
        "[console]\nlisten = '127.0.0.1:3001'\npublic_endpoint = 'wss://toml.example.com'\n",
        ServeOverrides {
            public_endpoint: Some("  https://cli.example.com  ".into()),
            ..Default::default()
        },
        env_fn,
    )
    .unwrap();
    assert_eq!(
        overridden
            .console
            .as_ref()
            .unwrap()
            .public_endpoint
            .as_deref(),
        Some("https://cli.example.com")
    );
    assert_eq!(
        overridden.public_endpoint.as_deref(),
        Some("https://cli.example.com")
    );

    // Blank CLI override falls back to env, then TOML.
    let fallback_env = config_with_env(
        "[console]\npublic_endpoint = 'wss://toml.example.com'\n",
        ServeOverrides {
            public_endpoint: Some("   ".into()),
            ..Default::default()
        },
        env_fn,
    )
    .unwrap();
    assert_eq!(
        fallback_env.public_endpoint.as_deref(),
        Some("wss://env.example.com")
    );
    assert_eq!(
        fallback_env
            .console
            .as_ref()
            .unwrap()
            .public_endpoint
            .as_deref(),
        Some("wss://env.example.com")
    );

    // Works even when console is disabled (without enabling console).
    let grpc_only = load_no_env(ServeOverrides {
        public_endpoint: Some("https://grpc.example.com".into()),
        ..Default::default()
    });
    assert!(grpc_only.console.is_none());
    assert_eq!(
        grpc_only.public_endpoint.as_deref(),
        Some("https://grpc.example.com")
    );
}

#[test]
fn no_grpc_flag_disables_grpc_overriding_default_and_toml() {
    let default_disabled = load_no_env(ServeOverrides {
        no_grpc: true,
        console_listen: Some("127.0.0.1:3001".into()),
        ..Default::default()
    });
    assert!(!default_disabled.grpc.enabled);
    assert!(default_disabled.console.is_some());

    let toml_overridden = config_with_env(
        "[server]\ngrpc_enabled = true\n[console]\nlisten = '127.0.0.1:3001'\n",
        ServeOverrides {
            no_grpc: true,
            ..Default::default()
        },
        |_| None,
    )
    .unwrap();
    assert!(!toml_overridden.grpc.enabled);
}

#[test]
fn auto_token_generates_persists_with_0600_reloads_and_redacts_debug() {
    use anyserve_core::auth::Role;

    let dir = crate::test_support::temporary_path("auto-token-dir");
    let journal = dir.join("journal.sqlite");
    let tokens_file = dir.join("tokens.env");
    let auto_opts = || ServeOverrides {
        journal_path: Some(journal.clone()),
        auto_token: true,
        ..Default::default()
    };

    let first = load_no_env(auto_opts());
    let auto1 = first.auto_tokens.as_ref().expect("auto_tokens populated");
    assert!(!auto1.reused_existing);
    assert_eq!(auto1.persisted_path.as_deref(), Some(tokens_file.as_path()));
    assert_auto_token_shape(&auto1.client_token, "as_c_");
    assert_auto_token_shape(&auto1.worker_token, "as_w_");
    assert_ne!(auto1.client_token, auto1.worker_token);
    assert_eq!(
        first.auth.role_for_token(&auto1.client_token),
        Some(Role::Client)
    );
    assert_eq!(
        first.auth.role_for_token(&auto1.worker_token),
        Some(Role::Worker)
    );

    let dbg = format!("{first:?}");
    assert!(!dbg.contains(&auto1.client_token));
    assert!(!dbg.contains(&auto1.worker_token));
    assert!(dbg.contains("[redacted]"));

    assert_eq!(
        fs::read_to_string(&tokens_file).unwrap(),
        format!(
            "ANYSERVE_CLIENT_TOKENS={}\nANYSERVE_WORKER_TOKENS={}\n",
            auto1.client_token, auto1.worker_token
        )
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&tokens_file).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    // Restart with same journal reloads without rotating.
    let second = load_no_env(auto_opts());
    let auto2 = second.auto_tokens.as_ref().expect("auto_tokens reloaded");
    assert!(auto2.reused_existing);
    assert_eq!(auto2.client_token, auto1.client_token);
    assert_eq!(auto2.worker_token, auto1.worker_token);
    assert_eq!(
        second.auth.role_for_token(&auto1.client_token),
        Some(Role::Client)
    );

    // Corrupt/empty tokens.env regenerates a fresh valid pair.
    fs::write(&tokens_file, "ANYSERVE_CLIENT_TOKENS=\n").unwrap();
    let recovered = load_no_env(auto_opts());
    let auto3 = recovered
        .auto_tokens
        .as_ref()
        .expect("auto_tokens regenerated");
    assert!(!auto3.reused_existing);
    assert_auto_token_shape(&auto3.client_token, "as_c_");
    assert_auto_token_shape(&auto3.worker_token, "as_w_");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn auto_token_ephemeral_stays_in_memory_and_noop_when_tokens_exist() {
    use anyserve_core::auth::Role;

    let ephemeral = load_no_env(ServeOverrides {
        ephemeral: true,
        auto_token: true,
        ..Default::default()
    });
    let auto = ephemeral
        .auto_tokens
        .as_ref()
        .expect("ephemeral auto_tokens");
    assert!(auto.persisted_path.is_none());
    assert!(!auto.reused_existing);
    assert_auto_token_shape(&auto.client_token, "as_c_");
    assert_auto_token_shape(&auto.worker_token, "as_w_");
    assert_eq!(
        ephemeral.auth.role_for_token(&auto.client_token),
        Some(Role::Client)
    );

    // No-op when user provided tokens via CLI, env, or TOML scoped, or when both listeners are off.
    let dir = crate::test_support::temporary_path("auto-token-noop");
    let journal = dir.join("journal.sqlite");
    let tokens_file = dir.join("tokens.env");

    let cli_case = load_no_env(ServeOverrides {
        journal_path: Some(journal.clone()),
        client_tokens: vec!["user-client".into()],
        auto_token: true,
        ..Default::default()
    });
    assert!(cli_case.auto_tokens.is_none());
    assert_eq!(
        cli_case.auth.role_for_token("user-client"),
        Some(Role::Client)
    );

    let env_case = ResolvedServeConfig::load_with_env(
        ServeOverrides {
            journal_path: Some(journal.clone()),
            auto_token: true,
            ..Default::default()
        },
        |name| (name == "ANYSERVE_WORKER_TOKENS").then(|| "env-worker".into()),
    )
    .unwrap();
    assert!(env_case.auto_tokens.is_none());
    assert_eq!(
        env_case.auth.role_for_token("env-worker"),
        Some(Role::Worker)
    );

    let scoped_case = config_with_env(
        "[[auth.scoped_tokens]]\ntoken = 'scoped-tok'\nrole = 'worker'\nnamespace = 'team-a'\n",
        ServeOverrides {
            journal_path: Some(journal.clone()),
            auto_token: true,
            ..Default::default()
        },
        |_| None,
    )
    .unwrap();
    assert!(scoped_case.auto_tokens.is_none());
    assert_eq!(
        scoped_case.auth.role_for_token("scoped-tok"),
        Some(Role::Worker)
    );

    let invalid_listeners = load_no_env(ServeOverrides {
        journal_path: Some(journal),
        no_grpc: true,
        auto_token: true,
        ..Default::default()
    });
    assert!(invalid_listeners.auto_tokens.is_none());
    assert!(!tokens_file.exists());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn signing_keys_and_revocation_url_resolve_from_env_and_auto_token_stays_noop() {
    use anyserve_core::auth::{Role, SignedTokenClaims, sign_token};

    let key = "shared-signing-secret-0123456789";
    let dir = crate::test_support::temporary_path("signing-key-env");
    let resolved = ResolvedServeConfig::load_with_env(
        ServeOverrides {
            journal_path: Some(dir.join("journal.sqlite")),
            auto_token: true,
            ..Default::default()
        },
        |name| match name {
            "ANYSERVE_TOKEN_SIGNING_KEY" => Some(key.into()),
            "ANYSERVE_AUTH_REVOCATION_URL" => {
                Some("http://auth-github:3002/internal/revocations".into())
            }
            _ => None,
        },
    )
    .unwrap();

    assert!(resolved.auth.is_enabled());
    assert!(resolved.auto_tokens.is_none());
    let rev = resolved
        .auth_revocation
        .as_ref()
        .expect("revocation config");
    assert_eq!(rev.url, "http://auth-github:3002/internal/revocations");
    assert!(!format!("{resolved:?}").contains(key));

    let tok = sign_token(
        key.as_bytes(),
        &SignedTokenClaims {
            jti: "jti-cfg".into(),
            ns: "gh-7".into(),
            role: "client".into(),
            sub: "gh:dev".into(),
            iat: 1_700_000_000,
            exp: None,
            fp: None,
        },
    )
    .unwrap();
    let p = resolved.auth.principal_for_token(&tok).unwrap();
    assert_eq!(p.namespace, "gh-7");
    assert_eq!(p.role, Role::Client);
    assert!(!p.is_operator);
    assert!(!dir.exists());
}

#[test]
fn env_scoped_tokens_tenant_quotas_journal_max_bytes_and_memory_limit_resolve() {
    let raw = r#"
[runtime]
max_pending_bytes = 999999

[runtime.tenant_quotas]
max_sessions_per_namespace = 4
max_pending_jobs_per_namespace = 12
max_interfaces_per_namespace = 6

[journal]
max_bytes = 1048576
"#;
    let resolved = config_with_env(
        raw,
        ServeOverrides {
            memory_limit_mb: Some(100),
            journal_max_bytes: Some(2_097_152),
            ..Default::default()
        },
        |name| match name {
            "ANYSERVE_SCOPED_TOKENS" => Some(
                r#"[{"token":"sc-1","role":"client","namespace":"tenant-x","subject":"app-x","forcedPool":"gpu-a"}]"#.into(),
            ),
            _ => None,
        },
    )
    .unwrap();

    // Explicit max_pending_bytes is preserved; unspecified budgets are derived from 100 MiB.
    assert_eq!(resolved.runtime.max_pending_bytes, 999_999);
    assert_eq!(resolved.runtime.max_result_bytes, 25 * 1024 * 1024);
    assert_eq!(resolved.runtime.max_event_bytes, 10 * 1024 * 1024);
    assert_eq!(
        resolved.runtime.tenant_quotas.max_sessions_per_namespace,
        Some(4)
    );
    assert_eq!(
        resolved
            .runtime
            .tenant_quotas
            .max_pending_jobs_per_namespace,
        Some(12)
    );
    assert_eq!(
        resolved.runtime.tenant_quotas.max_interfaces_per_namespace,
        Some(6)
    );
    assert_eq!(
        resolved.runtime.journal.as_ref().unwrap().max_bytes,
        Some(2_097_152)
    );

    let p = resolved.auth.principal_for_token("sc-1").unwrap();
    assert_eq!(p.namespace, "tenant-x");
    assert_eq!(p.subject, "app-x");
    assert_eq!(p.forced_pool.as_deref(), Some("gpu-a"));

    // Zero overrides / env vars are rejected.
    let err = config_with_env(
        "",
        ServeOverrides {
            memory_limit_mb: Some(0),
            ..Default::default()
        },
        |_| None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("positive"), "{err}");

    let err = config_with_env("", ServeOverrides::default(), |name| {
        (name == "ANYSERVE_JOURNAL_MAX_BYTES").then(|| "0".into())
    })
    .unwrap_err();
    assert!(err.to_string().contains("positive"), "{err}");
}

#[test]
fn revocation_env_precedence_portal_url_validation_scoped_tokens_and_budget_clamping() {
    // 1. ANYSERVE_AUTH_REVOCATION_URL overrides TOML [auth].revocation_url.
    let rev_resolved = config_with_env(
        "[auth]\nrevocation_url = 'http://toml-auth:3002/internal/revocations'\n",
        ServeOverrides::default(),
        |name| match name {
            "ANYSERVE_TOKEN_SIGNING_KEY" => Some("k-0123456789abcdef".into()),
            "ANYSERVE_AUTH_REVOCATION_URL" => {
                Some("http://env-auth:3002/internal/revocations".into())
            }
            _ => None,
        },
    )
    .unwrap();
    assert_eq!(
        rev_resolved.auth_revocation.unwrap().url,
        "http://env-auth:3002/internal/revocations"
    );

    // 2. auth_portal_url rejects non-http(s) schemes (e.g. javascript:) from both TOML and env.
    for (toml_str, env_url) in [
        (
            "[console]\nauth_portal_url = 'javascript:alert(1)'\n",
            None::<&str>,
        ),
        ("[console]\n", Some("javascript:alert(1)")),
        ("[console]\n", Some("data:text/html,evil")),
    ] {
        let err = config_with_env(toml_str, ServeOverrides::default(), |name| {
            (name == "ANYSERVE_CONSOLE_AUTH_PORTAL_URL").then(|| env_url.map(str::to_owned))?
        })
        .unwrap_err();
        assert!(err.to_string().contains("http:// or https://"), "{err}");
    }

    // 3. ANYSERVE_SCOPED_TOKENS rejects unknown fields and trims forced_pool / subject.
    let err = config_with_env("", ServeOverrides::default(), |name| {
        (name == "ANYSERVE_SCOPED_TOKENS").then(|| {
            r#"[{"token":"sc-1","role":"client","namespace":"t1","typoField":123}]"#.into()
        })
    })
    .unwrap_err();
    assert!(format!("{err:#}").contains("unknown field"), "{err:#}");

    let trimmed = config_with_env("", ServeOverrides::default(), |name| {
        (name == "ANYSERVE_SCOPED_TOKENS").then(|| {
            r#"[{"token":" sc-trim ","role":"worker","namespace":"t1","subject":" sub-1 ","forced_pool":" pool-a "}]"#.into()
        })
    })
    .unwrap();
    let p = trimmed.auth.principal_for_token("sc-trim").unwrap();
    assert_eq!(p.subject, "sub-1");
    assert_eq!(p.forced_pool.as_deref(), Some("pool-a"));

    // 4. --memory-limit 0 reports the CLI flag name (--memory-limit).
    let err = config_with_env(
        "",
        ServeOverrides {
            memory_limit_mb: Some(0),
            ..Default::default()
        },
        |_| None,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("--memory-limit must be positive"),
        "{err}"
    );
    let err = config_with_env(
        "",
        ServeOverrides {
            journal_max_bytes: Some(0),
            ..Default::default()
        },
        |_| None,
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("--journal-max-bytes must be positive"),
        "{err}"
    );

    // 5. Small --memory-limit clamps unspecified inline and event_log budgets.
    let small_mem = config_with_env(
        "",
        ServeOverrides {
            memory_limit_mb: Some(16),
            ..Default::default()
        },
        |_| None,
    )
    .unwrap();
    assert_eq!(small_mem.runtime.max_pending_bytes, 4 * 1024 * 1024);
    assert_eq!(small_mem.runtime.max_result_bytes, 4 * 1024 * 1024);
    assert!(small_mem.runtime.limits.max_inline_total_bytes <= small_mem.runtime.max_pending_bytes);
    assert!(
        small_mem.runtime.limits.max_inline_object_bytes
            <= small_mem.runtime.limits.max_inline_total_bytes
    );
    assert!(small_mem.runtime.event_log_max_bytes as u64 <= small_mem.runtime.max_event_bytes);

    // 6. journal.max_bytes < 65536 is rejected from both TOML and CLI/env.
    let err = config_with_env(
        "[journal]\nmax_bytes = 4096\n",
        ServeOverrides::default(),
        |_| None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("65536"), "{err}");

    let err = config_with_env(
        "",
        ServeOverrides {
            journal_max_bytes: Some(1024),
            ..Default::default()
        },
        |_| None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("65536"), "{err}");
}
