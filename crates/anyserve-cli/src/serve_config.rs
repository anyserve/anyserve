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
    pub host: String,
    pub port: u16,
    pub tls_enabled: bool,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
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
    PostgresCluster,
}
impl RuntimeMode {
    pub fn code(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::MemoryPlusSqlite => "memory_plus_sqlite",
            Self::PostgresCluster => "postgres_cluster",
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Self::Memory => "Memory",
            Self::MemoryPlusSqlite => "Memory + SQLite journal",
            Self::PostgresCluster => "PostgreSQL active-active",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServeConsoleConfig {
    pub listen: String,
    pub allowed_origins: Vec<String>,
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
        let mut runtime = file_config.runtime.unwrap_or_default().resolve();
        runtime.journal = if overrides.ephemeral {
            None
        } else {
            file_config
                .journal
                .unwrap_or_default()
                .resolve(overrides.journal_path)?
        };
        let console = file_config.console.and_then(|config| config.resolve());
        let grpc = GrpcConfig {
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
            auth: file_config.auth.unwrap_or_default().resolve()?,
        })
    }

    pub fn runtime_mode(&self) -> RuntimeMode {
        if self
            .runtime
            .journal
            .as_ref()
            .is_some_and(|j| j.postgres.is_some())
        {
            RuntimeMode::PostgresCluster
        } else if self.runtime.journal.is_some() {
            RuntimeMode::MemoryPlusSqlite
        } else {
            RuntimeMode::Memory
        }
    }
}
