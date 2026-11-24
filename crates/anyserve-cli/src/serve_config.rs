mod file;
#[cfg(test)]
mod tests;

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::{JournalConfig, RuntimeConfig};

use file::AnyserveConfigFile;

const DEFAULT_CONSOLE_LISTEN: &str = "127.0.0.1:3001";
const AUTO_TOKEN_FILE_NAME: &str = "tokens.env";
const CLIENT_TOKEN_PREFIX: &str = "as_c_";
const WORKER_TOKEN_PREFIX: &str = "as_w_";

#[derive(Clone, Debug)]
pub struct GrpcConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 50_052,
            tls_enabled: false,
            cert_file: None,
            key_file: None,
        }
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub struct ServeOverrides {
    pub config: Option<PathBuf>,
    pub grpc_host: Option<String>,
    pub grpc_port: Option<u16>,
    pub grpc_tls_enabled: Option<bool>,
    pub grpc_cert_file: Option<String>,
    pub grpc_key_file: Option<String>,
    pub no_grpc: bool,
    pub ephemeral: bool,
    pub journal_path: Option<PathBuf>,
    pub console_listen: Option<String>,
    pub public_endpoint: Option<String>,
    pub client_tokens: Vec<String>,
    pub worker_tokens: Vec<String>,
    pub auto_token: bool,
}

impl std::fmt::Debug for ServeOverrides {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServeOverrides")
            .field("config", &self.config)
            .field("grpc_host", &self.grpc_host)
            .field("grpc_port", &self.grpc_port)
            .field("grpc_tls_enabled", &self.grpc_tls_enabled)
            .field("grpc_cert_file", &self.grpc_cert_file)
            .field("grpc_key_file", &self.grpc_key_file)
            .field("no_grpc", &self.no_grpc)
            .field("ephemeral", &self.ephemeral)
            .field("journal_path", &self.journal_path)
            .field("console_listen", &self.console_listen)
            .field("public_endpoint", &self.public_endpoint)
            .field("client_token_args", &self.client_tokens.len())
            .field("worker_token_args", &self.worker_tokens.len())
            .field("auto_token", &self.auto_token)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AutoTokens {
    pub client_token: String,
    pub worker_token: String,
    pub persisted_path: Option<PathBuf>,
    pub reused_existing: bool,
}

impl std::fmt::Debug for AutoTokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoTokens")
            .field("client_token", &"[redacted]")
            .field("worker_token", &"[redacted]")
            .field("persisted_path", &self.persisted_path)
            .field("reused_existing", &self.reused_existing)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct ResolvedServeConfig {
    pub config_path: Option<PathBuf>,
    pub grpc: GrpcConfig,
    pub runtime: RuntimeConfig,
    pub console: Option<ServeConsoleConfig>,
    pub public_endpoint: Option<String>,
    pub auth: AuthPolicy,
    pub auto_tokens: Option<AutoTokens>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeMode {
    Memory,
    MemoryPlusSqlite,
    MemoryPlusPostgres,
    MemoryPlusCloudflare,
}
impl RuntimeMode {
    pub fn code(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::MemoryPlusSqlite => "memory_plus_sqlite",
            Self::MemoryPlusPostgres => "memory_plus_postgres",
            Self::MemoryPlusCloudflare => "memory_plus_cloudflare",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Memory => "Memory",
            Self::MemoryPlusSqlite => "Memory + SQLite journal",
            Self::MemoryPlusPostgres => "Memory + PostgreSQL journal",
            Self::MemoryPlusCloudflare => "Memory + Cloudflare journal",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeConsoleConfig {
    pub listen: String,
    pub allowed_origins: Vec<String>,
    pub public_endpoint: Option<String>,
}

impl ResolvedServeConfig {
    pub fn load(overrides: ServeOverrides) -> Result<Self> {
        Self::load_with_env(overrides, |name| std::env::var(name).ok())
    }

    pub(crate) fn load_with_env(
        overrides: ServeOverrides,
        mut env: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self> {
        let file_path = overrides.config;
        let file_config = if let Some(path) = file_path.as_ref() {
            AnyserveConfigFile::load(path)?
        } else {
            AnyserveConfigFile::default()
        };

        let server = file_config.server.unwrap_or_default();
        let tls = server.tls.unwrap_or_default();
        let defaults = GrpcConfig::default();
        let mut runtime = file_config.runtime.unwrap_or_default().resolve()?;
        runtime.journal = if overrides.ephemeral {
            None
        } else {
            file_config
                .journal
                .unwrap_or_default()
                .resolve_with_env(overrides.journal_path, &mut env)?
        };
        let mut auth = file_config.auth.unwrap_or_default().resolve_with_env(
            &overrides.client_tokens,
            &overrides.worker_tokens,
            &mut env,
        )?;

        let cli_public_endpoint = file::nonempty_trimmed(overrides.public_endpoint);
        let mut console = file_config
            .console
            .and_then(|config| config.resolve_with_env(&mut env));
        if let Some(listen) = file::nonempty_trimmed(overrides.console_listen) {
            match console.as_mut() {
                Some(cfg) => cfg.listen = listen,
                None => {
                    console = Some(ServeConsoleConfig {
                        listen,
                        allowed_origins: Vec::new(),
                        public_endpoint: file::nonempty_trimmed(env(file::PUBLIC_ENDPOINT_ENV)),
                    });
                }
            }
        }
        if let Some(ep) = cli_public_endpoint.as_ref()
            && let Some(cfg) = console.as_mut()
        {
            cfg.public_endpoint = Some(ep.clone());
        }
        let public_endpoint = cli_public_endpoint.or_else(|| match &console {
            Some(cfg) => cfg.public_endpoint.clone(),
            None => file::nonempty_trimmed(env(file::PUBLIC_ENDPOINT_ENV)),
        });

        let grpc = GrpcConfig {
            enabled: if overrides.no_grpc {
                false
            } else {
                server.grpc_enabled.unwrap_or(defaults.enabled)
            },
            host: overrides
                .grpc_host
                .or(server.grpc_host)
                .unwrap_or(defaults.host),
            port: overrides
                .grpc_port
                .or(server.grpc_port)
                .unwrap_or(defaults.port),
            tls_enabled: overrides
                .grpc_tls_enabled
                .or(tls.enabled)
                .unwrap_or(defaults.tls_enabled),
            cert_file: overrides.grpc_cert_file.or(tls.cert_file),
            key_file: overrides.grpc_key_file.or(tls.key_file),
        };

        let auto_tokens =
            if overrides.auto_token && !auth.is_enabled() && (grpc.enabled || console.is_some()) {
                let (resolved_auto, client_tokens, worker_tokens) =
                    resolve_or_generate_auto_tokens(runtime.journal.as_ref())?;
                auth = AuthPolicy::new(client_tokens, worker_tokens);
                Some(resolved_auto)
            } else {
                None
            };

        Ok(Self {
            config_path: file_path,
            grpc,
            runtime,
            console,
            public_endpoint,
            auth,
            auto_tokens,
        })
    }

    pub fn runtime_mode(&self) -> RuntimeMode {
        match self.runtime.journal.as_ref() {
            Some(j) if j.cloudflare.is_some() => RuntimeMode::MemoryPlusCloudflare,
            Some(j) if j.postgres.is_some() => RuntimeMode::MemoryPlusPostgres,
            Some(_) => RuntimeMode::MemoryPlusSqlite,
            None => RuntimeMode::Memory,
        }
    }
}

fn auto_token_file_path(journal: Option<&JournalConfig>) -> Option<PathBuf> {
    let journal = journal.filter(|j| j.postgres.is_none() && j.cloudflare.is_none())?;
    Some(
        match journal.path.parent().filter(|d| !d.as_os_str().is_empty()) {
            Some(dir) => dir.join(AUTO_TOKEN_FILE_NAME),
            None => PathBuf::from(AUTO_TOKEN_FILE_NAME),
        },
    )
}

fn resolve_or_generate_auto_tokens(
    journal: Option<&JournalConfig>,
) -> Result<(AutoTokens, Vec<String>, Vec<String>)> {
    let persisted_path = auto_token_file_path(journal);
    if let Some(path) = &persisted_path
        && let Some((clients, workers)) = read_auto_tokens_file(path)?
    {
        return Ok((
            AutoTokens {
                client_token: clients[0].clone(),
                worker_token: workers[0].clone(),
                persisted_path: Some(path.clone()),
                reused_existing: true,
            },
            clients,
            workers,
        ));
    }

    let client_token = anyserve_core::auth::generate_token(CLIENT_TOKEN_PREFIX);
    let worker_token = anyserve_core::auth::generate_token(WORKER_TOKEN_PREFIX);
    if let Some(path) = &persisted_path {
        write_auto_tokens_file(path, &client_token, &worker_token)?;
    }
    Ok((
        AutoTokens {
            client_token: client_token.clone(),
            worker_token: worker_token.clone(),
            persisted_path,
            reused_existing: false,
        },
        vec![client_token],
        vec![worker_token],
    ))
}

fn read_auto_tokens_file(path: &Path) -> Result<Option<(Vec<String>, Vec<String>)>> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(parse_tokens_env(&raw)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("read auto-token file {}", path.display()))
        }
    }
}

fn parse_tokens_env(raw: &str) -> Option<(Vec<String>, Vec<String>)> {
    let parse_val = |v: &str| -> Option<Vec<String>> {
        let v = v.trim();
        let unquoted = v
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .or_else(|| v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
            .unwrap_or(v);
        let tokens = file::split_tokens(unquoted);
        (!tokens.is_empty()
            && tokens
                .iter()
                .all(|t| !t.is_empty() && t.bytes().all(|b| b.is_ascii_graphic())))
        .then_some(tokens)
    };

    let (mut clients, mut workers) = (None, None);
    for line in raw
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let entry = line.strip_prefix("export ").map(str::trim).unwrap_or(line);
        let Some((key, value)) = entry.split_once('=') else {
            continue;
        };
        match key.trim() {
            file::DEFAULT_CLIENT_TOKENS_ENV => clients = parse_val(value),
            file::DEFAULT_WORKER_TOKENS_ENV => workers = parse_val(value),
            _ => {}
        }
    }
    clients.zip(workers)
}

fn write_auto_tokens_file(path: &Path, client_token: &str, worker_token: &str) -> Result<()> {
    if let Some(parent) = path.parent().filter(|dir| !dir.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("create directory for {}", path.display()))?;
    }
    let content = format!(
        "{}={client_token}\n{}={worker_token}\n",
        file::DEFAULT_CLIENT_TOKENS_ENV,
        file::DEFAULT_WORKER_TOKENS_ENV,
    );
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("open auto-token file {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .with_context(|| format!("set 0600 permissions on {}", path.display()))?;
    }
    file.write_all(content.as_bytes())
        .with_context(|| format!("write auto-token file {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync auto-token file {}", path.display()))?;
    Ok(())
}
