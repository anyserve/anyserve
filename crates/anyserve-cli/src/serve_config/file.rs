use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use anyserve_core::auth::AuthPolicy;
use serde::Deserialize;

use super::{
    AuthRevocationConfig, DEFAULT_CONSOLE_LISTEN, DEFAULT_REVOCATION_POLL_MS,
    MIN_REVOCATION_POLL_MS, ServeConsoleConfig,
};
use anyserve_core::runtime::api::{InterfacePolicy, JournalConfig, RuntimeConfig};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AnyserveConfigFile {
    pub(super) server: Option<ServerConfigFile>,
    pub(super) runtime: Option<RuntimeConfigFile>,
    pub(super) journal: Option<JournalConfigFile>,
    pub(super) console: Option<ServeConsoleConfigFile>,
    pub(super) auth: Option<AuthConfigFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopedTokenFile {
    token: Option<String>,
    token_env: Option<String>,
    role: String,
    namespace: String,
    #[serde(default)]
    subject: String,
    forced_pool: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuthConfigFile {
    #[serde(default)]
    client_tokens: Vec<String>,
    /// Env var holding comma-separated client tokens.
    client_tokens_env: Option<String>,
    #[serde(default)]
    worker_tokens: Vec<String>,
    /// Env var holding comma-separated worker tokens.
    worker_tokens_env: Option<String>,
    #[serde(default)]
    scoped_tokens: Vec<ScopedTokenFile>,
    /// Env var holding comma-separated HMAC signing keys for stateless plugin tokens.
    signing_keys_env: Option<String>,
    /// Optional internal revocation snapshot endpoint exposed by an auth plugin.
    revocation_url: Option<String>,
    revocation_poll_interval_ms: Option<u64>,
}

pub(super) const DEFAULT_CLIENT_TOKENS_ENV: &str = "ANYSERVE_CLIENT_TOKENS";
pub(super) const DEFAULT_WORKER_TOKENS_ENV: &str = "ANYSERVE_WORKER_TOKENS";
pub(super) const DEFAULT_SCOPED_TOKENS_ENV: &str = "ANYSERVE_SCOPED_TOKENS";
pub(super) const DEFAULT_SIGNING_KEYS_ENV: &str = anyserve_core::auth::SIGNING_KEYS_ENV;
pub(super) const DEFAULT_REVOCATION_URL_ENV: &str = "ANYSERVE_AUTH_REVOCATION_URL";
pub(super) const PUBLIC_ENDPOINT_ENV: &str = "ANYSERVE_PUBLIC_ENDPOINT";
pub(super) const CONSOLE_AUTH_PORTAL_URL_ENV: &str = "ANYSERVE_CONSOLE_AUTH_PORTAL_URL";
pub(super) const MEMORY_LIMIT_MB_ENV: &str = "ANYSERVE_MEMORY_LIMIT_MB";
pub(super) const JOURNAL_MAX_BYTES_ENV: &str = "ANYSERVE_JOURNAL_MAX_BYTES";
pub(super) const MIN_JOURNAL_MAX_BYTES: u64 = 65_536;

pub(super) fn validate_journal_max_bytes(bytes: u64) -> Result<()> {
    anyhow::ensure!(
        bytes >= MIN_JOURNAL_MAX_BYTES,
        "journal.max_bytes must be at least {MIN_JOURNAL_MAX_BYTES} bytes (64 KiB)"
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvScopedToken {
    token: String,
    role: String,
    namespace: String,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default, alias = "forcedPool")]
    forced_pool: Option<String>,
}

pub(super) fn split_tokens(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_scoped_token_entry(
    token: String,
    role_raw: &str,
    namespace_raw: &str,
    subject: Option<String>,
    forced_pool: Option<String>,
) -> Result<anyserve_core::auth::ScopedToken> {
    let role = match role_raw.trim().to_ascii_lowercase().as_str() {
        "client" => anyserve_core::auth::Role::Client,
        "worker" => anyserve_core::auth::Role::Worker,
        other => anyhow::bail!("unknown scoped_token role '{other}' (expected client|worker)"),
    };
    let namespace = namespace_raw.trim();
    anyserve_core::auth::validate_namespace(namespace)
        .map_err(|s| anyhow::anyhow!("invalid scoped_token namespace: {}", s.message()))?;
    let token = token.trim();
    anyhow::ensure!(!token.is_empty(), "scoped_token token must not be empty");
    Ok(anyserve_core::auth::ScopedToken {
        token: token.to_owned(),
        role,
        namespace: namespace.to_string(),
        subject: nonempty_trimmed(subject).unwrap_or_else(|| format!("{}-token", role_raw.trim())),
        forced_pool: nonempty_trimmed(forced_pool),
    })
}

impl AuthConfigFile {
    pub(super) fn resolve_with_env(
        self,
        cli_client_tokens: &[String],
        cli_worker_tokens: &[String],
        mut env: impl FnMut(&str) -> Option<String>,
    ) -> Result<(AuthPolicy, Option<AuthRevocationConfig>)> {
        let mut collect = |mut tokens: Vec<String>,
                           explicit: Option<&str>,
                           default_env: &str,
                           cli: &[String]|
         -> Result<Vec<String>> {
            let env_raw = match explicit {
                Some(name) => Some(
                    env(name).with_context(|| format!("read auth tokens from env var {name}"))?,
                ),
                None => env(default_env),
            };
            if let Some(raw) = env_raw {
                tokens.extend(split_tokens(&raw));
            }
            for raw in cli {
                tokens.extend(split_tokens(raw));
            }
            Ok(tokens)
        };

        let client_tokens = collect(
            self.client_tokens,
            self.client_tokens_env.as_deref(),
            DEFAULT_CLIENT_TOKENS_ENV,
            cli_client_tokens,
        )?;
        let worker_tokens = collect(
            self.worker_tokens,
            self.worker_tokens_env.as_deref(),
            DEFAULT_WORKER_TOKENS_ENV,
            cli_worker_tokens,
        )?;
        let signing_keys: Vec<Vec<u8>> = collect(
            Vec::new(),
            self.signing_keys_env.as_deref(),
            DEFAULT_SIGNING_KEYS_ENV,
            &[],
        )?
        .into_iter()
        .map(String::into_bytes)
        .collect();
        let revocation_url = nonempty_trimmed(env(DEFAULT_REVOCATION_URL_ENV))
            .or_else(|| nonempty_trimmed(self.revocation_url));
        if revocation_url.is_some() && signing_keys.is_empty() {
            tracing::warn!(
                "revocation_url is set without ANYSERVE_TOKEN_SIGNING_KEY; signed token revocation polling is inactive"
            );
        }
        let auth_revocation = revocation_url
            .zip(signing_keys.first())
            .map(|(url, primary_key)| AuthRevocationConfig {
                url,
                bearer_token: anyserve_core::auth::internal_revocation_bearer(primary_key),
                poll_interval: Duration::from_millis(
                    self.revocation_poll_interval_ms
                        .unwrap_or(DEFAULT_REVOCATION_POLL_MS)
                        .max(MIN_REVOCATION_POLL_MS),
                ),
            });

        let mut scoped = Vec::with_capacity(self.scoped_tokens.len());
        for entry in self.scoped_tokens {
            let token = match (entry.token, entry.token_env) {
                (Some(t), _) => t,
                (None, Some(env_name)) => env(&env_name)
                    .with_context(|| format!("read scoped token from env var {env_name}"))?,
                (None, None) => {
                    anyhow::bail!("auth.scoped_tokens entry requires token or token_env")
                }
            };
            scoped.push(parse_scoped_token_entry(
                token,
                &entry.role,
                &entry.namespace,
                Some(entry.subject),
                entry.forced_pool,
            )?);
        }
        if let Some(raw_scoped) = nonempty_trimmed(env(DEFAULT_SCOPED_TOKENS_ENV)) {
            let entries: Vec<EnvScopedToken> = serde_json::from_str(&raw_scoped)
                .with_context(|| format!("parse {DEFAULT_SCOPED_TOKENS_ENV} JSON array"))?;
            for entry in entries {
                scoped.push(parse_scoped_token_entry(
                    entry.token,
                    &entry.role,
                    &entry.namespace,
                    entry.subject,
                    entry.forced_pool,
                )?);
            }
        }
        Ok((
            AuthPolicy::new(client_tokens, worker_tokens)
                .with_scoped_tokens(scoped)
                .with_signing_keys(signing_keys),
            auth_revocation,
        ))
    }
}

impl AnyserveConfigFile {
    pub(super) fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read config file {}", path.display()))?;
        toml::from_str(&raw).with_context(|| format!("parse config file {}", path.display()))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServerConfigFile {
    pub(super) grpc_enabled: Option<bool>,
    pub(super) grpc_host: Option<String>,
    pub(super) grpc_port: Option<u16>,
    pub(super) tls: Option<TlsConfigFile>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServeConsoleConfigFile {
    enabled: Option<bool>,
    listen: Option<String>,
    #[serde(default)]
    allowed_origins: Vec<String>,
    public_endpoint: Option<String>,
    auth_portal_url: Option<String>,
}

pub(crate) fn nonempty_trimmed(value: Option<String>) -> Option<String> {
    value.and_then(|raw| {
        let trimmed = raw.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

impl ServeConsoleConfigFile {
    /// The console section implied by `--console-listen` when the TOML has none.
    pub(super) fn with_listen(listen: String) -> Self {
        Self {
            listen: Some(listen),
            ..Self::default()
        }
    }

    pub(super) fn resolve_with_env(
        self,
        mut env: impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<ServeConsoleConfig>> {
        if !self.enabled.unwrap_or(true) {
            return Ok(None);
        }
        // A non-empty env var overrides the TOML value (like the journal URL), so
        // one baked-in image can advertise the endpoint of each deployment.
        let public_endpoint = nonempty_trimmed(env(PUBLIC_ENDPOINT_ENV))
            .or_else(|| nonempty_trimmed(self.public_endpoint));
        let auth_portal_url = nonempty_trimmed(env(CONSOLE_AUTH_PORTAL_URL_ENV))
            .or_else(|| nonempty_trimmed(self.auth_portal_url));
        if let Some(ref url) = auth_portal_url {
            let lower = url.to_ascii_lowercase();
            anyhow::ensure!(
                lower.starts_with("http://") || lower.starts_with("https://"),
                "console auth_portal_url must use http:// or https:// scheme (got '{url}')"
            );
        }

        Ok(Some(ServeConsoleConfig {
            listen: self
                .listen
                .unwrap_or_else(|| DEFAULT_CONSOLE_LISTEN.to_string()),
            allowed_origins: self.allowed_origins,
            public_endpoint,
            auth_portal_url,
        }))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TlsConfigFile {
    pub(super) enabled: Option<bool>,
    pub(super) cert_file: Option<String>,
    pub(super) key_file: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct JournalConfigFile {
    enabled: Option<bool>,
    backend: Option<JournalBackend>,
    endpoint: Option<String>,
    url_env: Option<String>,
    token_env: Option<String>,
    schema: Option<String>,
    lease_duration_ms: Option<u64>,
    poll_interval_ms: Option<u64>,
    max_connections: Option<u32>,
    r2_spill_threshold_bytes: Option<usize>,
    path: Option<PathBuf>,
    full_sync: Option<bool>,
    queue_capacity: Option<usize>,
    max_batch: Option<usize>,
    retention_secs: Option<u64>,
    max_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum JournalBackend {
    Sqlite,
    Postgres,
    Cloudflare,
}

#[cfg(test)]
mod postgres_tests {
    use super::*;

    #[test]
    fn postgres_config_uses_environment_and_redacts_the_url() {
        let file: JournalConfigFile =
            toml::from_str("backend='postgres'\nschema='cluster_a'\nlease_duration_ms=9000")
                .unwrap();
        let config = file
            .resolve_with_env(None, |name| {
                assert_eq!(name, "ANYSERVE_DATABASE_URL");
                Some("postgresql://user:secret@localhost/anyserve".into())
            })
            .unwrap()
            .unwrap();
        assert_eq!(config.postgres.as_ref().unwrap().schema, "cluster_a");
        assert_eq!(
            config.postgres.as_ref().unwrap().lease_duration,
            Duration::from_secs(9)
        );
        assert!(!format!("{config:?}").contains("secret"));
    }

    #[test]
    fn postgres_needs_an_explicit_url_but_overrides_do_not() {
        let file: JournalConfigFile = toml::from_str("backend='postgres'").unwrap();
        assert!(
            file.clone()
                .resolve_with_env(None, |_| None)
                .unwrap_err()
                .to_string()
                .contains("ANYSERVE_DATABASE_URL")
        );
        assert!(
            file.resolve_with_env(Some("local.sqlite".into()), |_| panic!(
                "must not read PG env"
            ))
            .unwrap()
            .unwrap()
            .postgres
            .is_none()
        );
        let file: JournalConfigFile = toml::from_str("backend='postgres'\nenabled=false").unwrap();
        assert!(
            file.resolve_with_env(None, |_| panic!("disabled"))
                .unwrap()
                .is_none()
        );
        assert!(toml::from_str::<JournalConfigFile>("backend='unknown'").is_err());
    }

    #[test]
    fn cloudflare_journal_resolves_endpoint_token_and_rejects_sqlite_flags() {
        let file: JournalConfigFile = toml::from_str(
            "backend='cloudflare'\nendpoint='http://custom.internal'\nlease_duration_ms=9000\npoll_interval_ms=350\nr2_spill_threshold_bytes=2048",
        )
        .unwrap();
        let cfg = file
            .clone()
            .resolve_with_env(None, |_| None)
            .unwrap()
            .unwrap();
        let cf = cfg.cloudflare.as_ref().unwrap();
        assert_eq!(cf.endpoint, "http://custom.internal");
        assert_eq!(cf.auth_token, None);
        assert_eq!(cf.lease_duration, Duration::from_secs(9));
        assert_eq!(cf.poll_interval, Duration::from_millis(350));
        assert_eq!(cf.r2_spill_threshold_bytes, 2048);

        // Env var ANYSERVE_CLOUDFLARE_JOURNAL_URL overrides TOML endpoint and
        // ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN provides optional auth_token.
        let cfg = file
            .resolve_with_env(None, |name| match name {
                "ANYSERVE_CLOUDFLARE_JOURNAL_URL" => Some("https://cf-override.test".into()),
                "ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN" => Some("secret-token".into()),
                _ => None,
            })
            .unwrap()
            .unwrap();
        let cf = cfg.cloudflare.as_ref().unwrap();
        assert_eq!(cf.endpoint, "https://cf-override.test");
        assert_eq!(cf.auth_token.as_deref(), Some("secret-token"));
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[redacted]"));

        // Explicit token_env fails clearly when missing/empty; defaults endpoint to http://journal.internal.
        let file: JournalConfigFile =
            toml::from_str("backend='cloudflare'\ntoken_env='CF_SECRET'").unwrap();
        assert!(
            file.clone()
                .resolve_with_env(None, |_| None)
                .unwrap_err()
                .to_string()
                .contains("CF_SECRET")
        );
        let cfg = file
            .resolve_with_env(None, |name| {
                (name == "CF_SECRET").then(|| "tok-123".to_string())
            })
            .unwrap()
            .unwrap();
        let cf = cfg.cloudflare.as_ref().unwrap();
        assert_eq!(cf.endpoint, "http://journal.internal");
        assert_eq!(cf.auth_token.as_deref(), Some("tok-123"));

        // Reject path or full_sync=false on cloudflare.
        for bad in [
            "backend='cloudflare'\npath='a.sqlite'",
            "backend='cloudflare'\nfull_sync=false",
            "backend='cloudflare'\ntoken_env=''",
        ] {
            let file: JournalConfigFile = toml::from_str(bad).unwrap();
            assert!(file.resolve_with_env(None, |_| None).is_err(), "{bad}");
        }
    }

    #[test]
    fn console_public_endpoint_env_overrides_toml_and_falls_back_when_unset_or_empty() {
        let empty: ServeConsoleConfigFile = toml::from_str("public_endpoint='   '").unwrap();
        let resolved = empty
            .resolve_with_env(|name| {
                (name == "ANYSERVE_PUBLIC_ENDPOINT").then(|| "  wss://edge.example.com  ".into())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            resolved.public_endpoint.as_deref(),
            Some("wss://edge.example.com")
        );
        assert!(resolved.auth_portal_url.is_none());

        let with_portal = ServeConsoleConfigFile::default()
            .resolve_with_env(|name| {
                (name == "ANYSERVE_CONSOLE_AUTH_PORTAL_URL")
                    .then(|| " https://relay.example.com/tokens ".into())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            with_portal.auth_portal_url.as_deref(),
            Some("https://relay.example.com/tokens")
        );

        let explicit = || -> ServeConsoleConfigFile {
            toml::from_str("public_endpoint='wss://toml.example.com'").unwrap()
        };
        let resolved = explicit()
            .resolve_with_env(|name| {
                (name == "ANYSERVE_PUBLIC_ENDPOINT").then(|| " wss://env.example.com ".into())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            resolved.public_endpoint.as_deref(),
            Some("wss://env.example.com")
        );

        for env_value in [None, Some("   ".to_string())] {
            let resolved = explicit()
                .resolve_with_env(|_| env_value.clone())
                .unwrap()
                .unwrap();
            assert_eq!(
                resolved.public_endpoint.as_deref(),
                Some("wss://toml.example.com")
            );
        }
    }

    #[test]
    fn auth_env_fallbacks_explicit_overrides_and_cli_merging() {
        use anyserve_core::auth::Role;

        let empty = AuthConfigFile::default();
        let (disabled, rev) = empty.resolve_with_env(&[], &[], |_| None).unwrap();
        assert!(!disabled.is_enabled());
        assert!(rev.is_none());

        // Automatic fallback to ANYSERVE_CLIENT_TOKENS / ANYSERVE_WORKER_TOKENS when unset in TOML.
        let (env_only, rev) = AuthConfigFile::default()
            .resolve_with_env(&[], &[], |name| match name {
                "ANYSERVE_CLIENT_TOKENS" => Some(" c1 , c2 ,, ".into()),
                "ANYSERVE_WORKER_TOKENS" => Some("w1, w2".into()),
                _ => None,
            })
            .unwrap();
        assert!(rev.is_none());
        assert!(env_only.is_enabled());
        assert_eq!(env_only.role_for_token("c1"), Some(Role::Client));
        assert_eq!(env_only.role_for_token("c2"), Some(Role::Client));
        assert_eq!(env_only.role_for_token("w1"), Some(Role::Worker));
        assert_eq!(env_only.role_for_token("w2"), Some(Role::Worker));
        assert_eq!(env_only.role_for_token("other"), None);

        // Explicit TOML client_tokens_env / worker_tokens_env overrides the default env var name.
        let explicit_toml: AuthConfigFile =
            toml::from_str("client_tokens_env='CUSTOM_C'\nworker_tokens_env='CUSTOM_W'").unwrap();
        let err = explicit_toml
            .resolve_with_env(&[], &[], |name| match name {
                "ANYSERVE_CLIENT_TOKENS" => Some("ignored-default".into()),
                "ANYSERVE_WORKER_TOKENS" => Some("ignored-default-w".into()),
                _ => None,
            })
            .unwrap_err();
        assert!(err.to_string().contains("CUSTOM_C"), "{err:#}");

        let explicit_toml: AuthConfigFile = toml::from_str(
            "client_tokens=['toml-c']\nclient_tokens_env='CUSTOM_C'\nworker_tokens_env='CUSTOM_W'\nrevocation_poll_interval_ms=50",
        )
        .unwrap();
        let (merged, rev) = explicit_toml
            .resolve_with_env(
                &["cli-c1, cli-c2".into()],
                &["cli-w1".into(), " , cli-w2 ".into()],
                |name| match name {
                    "CUSTOM_C" => Some("env-c".into()),
                    "CUSTOM_W" => Some("env-w".into()),
                    "ANYSERVE_TOKEN_SIGNING_KEY" => Some("sign-key-1,sign-key-old".into()),
                    "ANYSERVE_AUTH_REVOCATION_URL" => {
                        Some("http://127.0.0.1:3002/internal/revocations".into())
                    }
                    "ANYSERVE_CLIENT_TOKENS" => Some("must-not-load-c".into()),
                    "ANYSERVE_WORKER_TOKENS" => Some("must-not-load-w".into()),
                    _ => None,
                },
            )
            .unwrap();
        for tok in ["toml-c", "env-c", "cli-c1", "cli-c2"] {
            assert_eq!(merged.role_for_token(tok), Some(Role::Client), "{tok}");
        }
        for tok in ["env-w", "cli-w1", "cli-w2"] {
            assert_eq!(merged.role_for_token(tok), Some(Role::Worker), "{tok}");
        }
        assert_eq!(merged.role_for_token("must-not-load-c"), None);
        assert_eq!(merged.role_for_token("must-not-load-w"), None);
        let rev = rev.expect("revocation config");
        assert_eq!(rev.url, "http://127.0.0.1:3002/internal/revocations");
        assert_eq!(rev.poll_interval, Duration::from_millis(250));
        assert!(!format!("{rev:?}").contains(&rev.bearer_token));
        assert!(!format!("{rev:?}").contains("sign-key-1"));
        assert!(format!("{rev:?}").contains("[redacted]"));
    }
}

impl JournalConfigFile {
    pub(super) fn resolve_with_env(
        self,
        path_override: Option<PathBuf>,
        mut env: impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<JournalConfig>> {
        if !self.enabled.unwrap_or(true) && path_override.is_none() {
            return Ok(None);
        }
        let postgres = path_override.is_none() && self.backend == Some(JournalBackend::Postgres);
        let cloudflare =
            path_override.is_none() && self.backend == Some(JournalBackend::Cloudflare);
        let mut config = if postgres {
            if self.path.is_some() || self.full_sync == Some(false) {
                anyhow::bail!("journal.path and full_sync=false cannot be used with PostgreSQL");
            }
            let name = self.url_env.as_deref().unwrap_or("ANYSERVE_DATABASE_URL");
            let url = env(name)
                .filter(|url| !url.trim().is_empty())
                .with_context(|| format!("PostgreSQL requires connection URL in env var {name}"))?;
            let mut config = JournalConfig::postgres(url);
            let pg = config.postgres.as_mut().expect("PostgreSQL config");
            if let Some(schema) = self.schema {
                pg.schema = schema;
            }
            if let Some(value) = self.lease_duration_ms {
                pg.lease_duration = Duration::from_millis(value);
            }
            if let Some(value) = self.poll_interval_ms {
                pg.poll_interval = Duration::from_millis(value);
            }
            if let Some(value) = self.max_connections {
                pg.max_connections = value;
            }
            config
        } else if cloudflare {
            if self.path.is_some() || self.full_sync == Some(false) {
                anyhow::bail!("journal.path and full_sync=false cannot be used with Cloudflare");
            }
            let url_env = self
                .url_env
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or("ANYSERVE_CLOUDFLARE_JOURNAL_URL");
            let endpoint = nonempty_trimmed(env(url_env))
                .or_else(|| nonempty_trimmed(self.endpoint))
                .unwrap_or_else(|| "http://journal.internal".to_string());
            let auth_token = match self.token_env {
                Some(token_env) => {
                    let name = token_env.trim();
                    if name.is_empty() {
                        anyhow::bail!("journal.token_env must not be empty");
                    }
                    let token = nonempty_trimmed(env(name)).with_context(|| {
                        format!("Cloudflare journal token_env var {name} is missing or empty")
                    })?;
                    Some(token)
                }
                None => nonempty_trimmed(env("ANYSERVE_CLOUDFLARE_JOURNAL_TOKEN")),
            };
            let mut config = JournalConfig::cloudflare(endpoint);
            let cf = config.cloudflare.as_mut().expect("Cloudflare config");
            cf.auth_token = auth_token;
            if let Some(value) = self.lease_duration_ms {
                cf.lease_duration = Duration::from_millis(value);
            }
            if let Some(value) = self.poll_interval_ms {
                cf.poll_interval = Duration::from_millis(value);
            }
            if let Some(value) = self.r2_spill_threshold_bytes {
                cf.r2_spill_threshold_bytes = value;
            }
            config
        } else {
            JournalConfig::new(
                path_override
                    .or(self.path)
                    .unwrap_or_else(|| PathBuf::from(".anyserve/journal.sqlite")),
            )
        };
        if let Some(value) = self.full_sync {
            config.full_sync = value;
        }
        if let Some(value) = self.queue_capacity {
            config.queue_capacity = value;
        }
        if let Some(value) = self.max_batch {
            config.max_batch = value;
        }
        if let Some(value) = self.retention_secs {
            config.retention = Duration::from_secs(value);
        }
        if let Some(value) = self.max_bytes {
            config.max_bytes = Some(value);
        }
        if (!postgres && !cloudflare && config.path.as_os_str().is_empty())
            || config.queue_capacity == 0
            || config.max_batch == 0
            || config.max_bytes == Some(0)
        {
            anyhow::bail!(
                "journal path, queue_capacity, max_batch and max_bytes must be non-empty/positive"
            );
        }
        if let Some(bytes) = config.max_bytes {
            validate_journal_max_bytes(bytes)?;
        }
        Ok(Some(config))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TenantQuotasFile {
    max_sessions_per_namespace: Option<usize>,
    max_pending_jobs_per_namespace: Option<usize>,
    max_interfaces_per_namespace: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeConfigFile {
    heartbeat_interval_ms: Option<u64>,
    heartbeat_timeout_ms: Option<u64>,
    reconnect_grace_ms: Option<u64>,
    max_attempts: Option<u32>,
    max_queue: Option<usize>,
    default_max_in_flight_per_key: Option<usize>,
    max_pending_bytes: Option<u64>,
    max_pending_jobs: Option<usize>,
    max_event_bytes: Option<u64>,
    max_result_bytes: Option<u64>,
    max_inline_object_bytes: Option<u64>,
    max_inline_total_bytes: Option<u64>,
    max_sessions: Option<usize>,
    consumer_stall_timeout_ms: Option<u64>,
    cancel_grace_ms: Option<u64>,
    terminal_retention_secs: Option<u64>,
    max_terminal_jobs: Option<usize>,
    event_log_max_events: Option<usize>,
    event_log_max_bytes: Option<usize>,
    tenant_quotas: Option<TenantQuotasFile>,
    #[serde(default)]
    interfaces: BTreeMap<String, InterfaceConfigFile>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolTierFile {
    pool: String,
    #[serde(default)]
    spillover_after_ms: u64,
    reconnect_grace_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct InterfaceConfigFile {
    max_queue: Option<usize>,
    queue_timeout_ms: Option<u64>,
    max_in_flight_per_key: Option<usize>,
    #[serde(default)]
    pool_tiers: Vec<PoolTierFile>,
}
impl RuntimeConfigFile {
    /// `memory_limit_mb` scales the byte budgets; explicit TOML budgets below still win.
    pub(super) fn resolve_with_memory_limit(
        self,
        memory_limit_mb: Option<u64>,
    ) -> Result<RuntimeConfig> {
        let mut config = RuntimeConfig::default();
        if let Some(mb) = memory_limit_mb {
            let total = mb.saturating_mul(1024 * 1024);
            config.max_pending_bytes = (total / 4).max(1024 * 1024);
            config.max_result_bytes = config.max_pending_bytes;
            config.max_event_bytes = (total / 10).max(1024 * 1024);
        }
        if let Some(value) = self.heartbeat_interval_ms {
            config.heartbeat_interval = Duration::from_millis(value);
        }
        if let Some(value) = self.heartbeat_timeout_ms {
            config.heartbeat_timeout = Duration::from_millis(value);
        }
        if let Some(value) = self.reconnect_grace_ms {
            config.reconnect_grace = Some(Duration::from_millis(value));
        }
        if let Some(value) = self.max_attempts {
            config.default_max_attempts = value;
        }
        if let Some(value) = self.max_queue {
            config.default_max_queue = value;
        }
        if let Some(value) = self.default_max_in_flight_per_key {
            config.default_max_in_flight_per_key = Some(value);
        }
        if let Some(value) = self.max_pending_bytes {
            config.max_pending_bytes = value;
        }
        if let Some(value) = self.max_pending_jobs {
            config.max_pending_jobs = value;
        }
        if let Some(value) = self.max_event_bytes {
            config.max_event_bytes = value;
        }
        if let Some(value) = self.max_result_bytes {
            config.max_result_bytes = value;
        }
        if let Some(value) = self.max_inline_object_bytes {
            config.limits.max_inline_object_bytes = value;
        }
        if let Some(value) = self.max_inline_total_bytes {
            config.limits.max_inline_total_bytes = value;
        }
        if let Some(value) = self.max_sessions {
            config.max_sessions = value;
        }
        if let Some(value) = self.consumer_stall_timeout_ms {
            config.consumer_stall_timeout = Duration::from_millis(value);
        }
        if let Some(value) = self.cancel_grace_ms {
            config.cancel_grace = Duration::from_millis(value);
        }
        if let Some(value) = self.terminal_retention_secs {
            config.terminal_retention = Duration::from_secs(value);
        }
        if let Some(value) = self.max_terminal_jobs {
            config.max_terminal_jobs = value;
        }
        if let Some(value) = self.event_log_max_events {
            config.event_log_max_events = value;
        }
        if let Some(value) = self.event_log_max_bytes {
            config.event_log_max_bytes = value;
        }
        if memory_limit_mb.is_some() {
            let inline_cap = config.max_pending_bytes.min(config.max_result_bytes);
            if self.max_inline_total_bytes.is_none() {
                config.limits.max_inline_total_bytes =
                    config.limits.max_inline_total_bytes.min(inline_cap);
            }
            if self.max_inline_object_bytes.is_none() {
                config.limits.max_inline_object_bytes = config
                    .limits
                    .max_inline_object_bytes
                    .min(config.limits.max_inline_total_bytes);
            }
            if self.event_log_max_bytes.is_none() {
                let event_cap = usize::try_from(config.max_event_bytes).unwrap_or(usize::MAX);
                config.event_log_max_bytes = config.event_log_max_bytes.min(event_cap);
            }
        }
        if let Some(quotas) = self.tenant_quotas {
            config.tenant_quotas = anyserve_core::runtime::api::TenantQuotas {
                max_sessions_per_namespace: quotas.max_sessions_per_namespace,
                max_pending_jobs_per_namespace: quotas.max_pending_jobs_per_namespace,
                max_interfaces_per_namespace: quotas.max_interfaces_per_namespace,
            };
        }
        for name in self.interfaces.keys() {
            if !anyserve_core::runtime::api::is_valid_interface_name(name) {
                anyhow::bail!(
                    "runtime.interfaces key '{name}' must be 1..={} bytes with no ASCII whitespace or control characters",
                    anyserve_core::runtime::api::MAX_INTERFACE_BYTES
                );
            }
        }
        config.interfaces = self
            .interfaces
            .into_iter()
            .map(|(name, policy)| {
                (
                    name,
                    InterfacePolicy {
                        max_queue: policy.max_queue,
                        queue_timeout: policy.queue_timeout_ms.map(Duration::from_millis),
                        max_in_flight_per_key: policy.max_in_flight_per_key,
                        pool_tiers: policy
                            .pool_tiers
                            .into_iter()
                            .map(|tier| anyserve_core::runtime::api::PoolTier {
                                pool: tier.pool,
                                spillover_after: Duration::from_millis(tier.spillover_after_ms),
                                reconnect_grace: tier.reconnect_grace_ms.map(Duration::from_millis),
                            })
                            .collect(),
                    },
                )
            })
            .collect();
        Ok(config)
    }
}
