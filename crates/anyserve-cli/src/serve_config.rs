mod file;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use anyhow::Result;
use anyserve_core::auth::AuthPolicy;
use anyserve_core::runtime::api::RuntimeConfig;

use file::AnyserveConfigFile;

const DEFAULT_CONSOLE_LISTEN: &str = "127.0.0.1:3001";

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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServeOverrides {
    pub config: Option<PathBuf>,
    pub grpc_host: Option<String>,
    pub grpc_port: Option<u16>,
    pub grpc_tls_enabled: Option<bool>,
    pub grpc_cert_file: Option<String>,
    pub grpc_key_file: Option<String>,
    pub ephemeral: bool,
    pub journal_path: Option<PathBuf>,
    pub console_listen: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ResolvedServeConfig {
    pub config_path: Option<PathBuf>,
    pub grpc: GrpcConfig,
    pub runtime: RuntimeConfig,
    pub console: Option<ServeConsoleConfig>,
    pub auth: AuthPolicy,
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
                .resolve(overrides.journal_path)?
        };
        let auth = file_config.auth.unwrap_or_default().resolve()?;
        let mut console = file_config.console.and_then(|config| config.resolve());
        if let Some(listen) = overrides
            .console_listen
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            match console.as_mut() {
                Some(cfg) => cfg.listen = listen.to_owned(),
                None => {
                    let public_endpoint = std::env::var("ANYSERVE_PUBLIC_ENDPOINT")
                        .ok()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty());
                    console = Some(ServeConsoleConfig {
                        listen: listen.to_owned(),
                        allowed_origins: Vec::new(),
                        public_endpoint,
                    });
                }
            }
        }
        let grpc = GrpcConfig {
            enabled: server.grpc_enabled.unwrap_or(defaults.enabled),
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

        Ok(Self {
            config_path: file_path,
            grpc,
            runtime,
            console,
            auth,
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
