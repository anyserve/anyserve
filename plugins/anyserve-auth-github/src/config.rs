use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser, Clone, Debug)]
#[command(
    name = "anyserve-auth-github",
    version,
    about = "Optional GitHub OAuth login and self-service token portal plugin for AnyServe."
)]
pub struct Args {
    /// Address for the plugin HTTP server (ANYSERVE_AUTH_LISTEN, default 127.0.0.1:3002).
    #[arg(long)]
    pub listen: Option<String>,
    /// Public base URL used for GitHub OAuth callback and secure cookies (ANYSERVE_AUTH_PUBLIC_URL).
    #[arg(long)]
    pub public_url: Option<String>,
    /// Optional AnyServe gRPC endpoint displayed on the portal card (ANYSERVE_AUTH_GRPC_ENDPOINT).
    #[arg(long)]
    pub grpc_endpoint: Option<String>,
    /// Optional AnyServe WebSocket endpoint displayed on the portal card (ANYSERVE_AUTH_WS_ENDPOINT).
    #[arg(long)]
    pub ws_endpoint: Option<String>,
    /// Optional Console URL for the "Open in Console" button (ANYSERVE_AUTH_CONSOLE_URL).
    #[arg(long)]
    pub console_url: Option<String>,
    /// Shared HMAC signing secret (ANYSERVE_TOKEN_SIGNING_KEY; comma-separated allowed).
    #[arg(long)]
    pub signing_key: Option<String>,
    /// GitHub OAuth App Client ID (GITHUB_CLIENT_ID).
    #[arg(long)]
    pub github_client_id: Option<String>,
    /// GitHub OAuth App Client Secret (GITHUB_CLIENT_SECRET).
    #[arg(long)]
    pub github_client_secret: Option<String>,
    /// SQLite database path (ANYSERVE_AUTH_DB, default .anyserve/auth-github.sqlite).
    #[arg(long = "db")]
    pub db_path: Option<PathBuf>,
    /// Token lifetime in days; 0 disables expiry (ANYSERVE_AUTH_TOKEN_TTL_DAYS, default 30).
    #[arg(long)]
    pub token_ttl_days: Option<u64>,
    /// Maximum active tokens per GitHub user (ANYSERVE_AUTH_MAX_TOKENS_PER_USER, default 10).
    #[arg(long)]
    pub max_tokens_per_user: Option<u64>,
    /// Minimum GitHub account age in days (ANYSERVE_AUTH_MIN_ACCOUNT_AGE_DAYS, default 0).
    #[arg(long)]
    pub min_account_age_days: Option<u64>,
    /// Optional comma-separated GitHub login allowlist (ANYSERVE_AUTH_ALLOWED_USERS).
    #[arg(long)]
    pub allowed_users: Option<String>,
    /// GitHub OAuth base URL (ANYSERVE_AUTH_GITHUB_OAUTH_BASE, default https://github.com).
    #[arg(long)]
    pub github_oauth_base: Option<String>,
    /// GitHub REST API base URL (ANYSERVE_AUTH_GITHUB_API_BASE, default https://api.github.com).
    #[arg(long)]
    pub github_api_base: Option<String>,
}

#[derive(Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub public_url: String,
    pub grpc_endpoint: Option<String>,
    pub ws_endpoint: Option<String>,
    pub console_url: Option<String>,
    pub signing_keys: Vec<Vec<u8>>,
    pub revocation_bearers: Vec<String>,
    pub github_client_id: String,
    pub github_client_secret: String,
    pub db_path: PathBuf,
    pub token_ttl_days: u64,
    pub max_tokens_per_user: u64,
    pub min_account_age_days: u64,
    pub allowed_users: BTreeSet<String>,
    pub github_oauth_base: String,
    pub github_api_base: String,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("listen", &self.listen)
            .field("public_url", &self.public_url)
            .field("grpc_endpoint", &self.grpc_endpoint)
            .field("ws_endpoint", &self.ws_endpoint)
            .field("console_url", &self.console_url)
            .field("signing_key_count", &self.signing_keys.len())
            .field("revocation_bearer_count", &self.revocation_bearers.len())
            .field("github_client_id", &self.github_client_id)
            .field("github_client_secret", &"[redacted]")
            .field("db_path", &self.db_path)
            .field("token_ttl_days", &self.token_ttl_days)
            .field("max_tokens_per_user", &self.max_tokens_per_user)
            .field("min_account_age_days", &self.min_account_age_days)
            .field("allowed_users_count", &self.allowed_users.len())
            .finish()
    }
}

impl Config {
    pub fn primary_signing_key(&self) -> &[u8] {
        self.signing_keys
            .first()
            .expect("Config always has at least one signing key")
    }

    pub fn from_args(args: Args) -> Result<Self> {
        let env_str = |key: &str| std::env::var(key).ok();
        let listen_raw = args
            .listen
            .or_else(|| env_str("ANYSERVE_AUTH_LISTEN"))
            .unwrap_or_else(|| "127.0.0.1:3002".to_string());
        let listen: SocketAddr = listen_raw
            .trim()
            .parse()
            .with_context(|| format!("invalid listen address {listen_raw:?}"))?;

        let public_url = clean_nonempty(
            args.public_url
                .or_else(|| env_str("ANYSERVE_AUTH_PUBLIC_URL")),
        )
        .unwrap_or_else(|| "http://127.0.0.1:3002".to_string());

        let signing_keys = anyserve_core::auth::parse_signing_keys(
            &args
                .signing_key
                .or_else(|| env_str(anyserve_core::auth::SIGNING_KEYS_ENV))
                .unwrap_or_default(),
        );
        anyhow::ensure!(
            !signing_keys.is_empty(),
            "ANYSERVE_TOKEN_SIGNING_KEY (--signing-key) must not be empty"
        );
        let revocation_bearers: Vec<String> = signing_keys
            .iter()
            .map(|k| anyserve_core::auth::internal_revocation_bearer(k))
            .collect();

        let github_client_id = clean_nonempty(
            args.github_client_id
                .or_else(|| env_str("GITHUB_CLIENT_ID")),
        )
        .context("GITHUB_CLIENT_ID (--github-client-id) must not be empty")?;
        let github_client_secret = clean_nonempty(
            args.github_client_secret
                .or_else(|| env_str("GITHUB_CLIENT_SECRET")),
        )
        .context("GITHUB_CLIENT_SECRET (--github-client-secret) must not be empty")?;

        let parse_u64_env = |key: &str, default: u64| -> Result<u64> {
            match env_str(key).filter(|s| !s.trim().is_empty()) {
                Some(raw) => raw
                    .trim()
                    .parse::<u64>()
                    .with_context(|| format!("parse {key}={raw:?} as unsigned integer")),
                None => Ok(default),
            }
        };

        let allowed_users = args
            .allowed_users
            .or_else(|| env_str("ANYSERVE_AUTH_ALLOWED_USERS"))
            .unwrap_or_default()
            .split(',')
            .map(|u| u.trim().to_ascii_lowercase())
            .filter(|u| !u.is_empty())
            .collect();

        Ok(Self {
            listen,
            public_url,
            grpc_endpoint: clean_nonempty(
                args.grpc_endpoint
                    .or_else(|| env_str("ANYSERVE_AUTH_GRPC_ENDPOINT")),
            ),
            ws_endpoint: clean_nonempty(
                args.ws_endpoint
                    .or_else(|| env_str("ANYSERVE_AUTH_WS_ENDPOINT")),
            ),
            console_url: clean_nonempty(
                args.console_url
                    .or_else(|| env_str("ANYSERVE_AUTH_CONSOLE_URL")),
            ),
            signing_keys,
            revocation_bearers,
            github_client_id,
            github_client_secret,
            db_path: args
                .db_path
                .or_else(|| {
                    env_str("ANYSERVE_AUTH_DB")
                        .filter(|s| !s.trim().is_empty())
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".anyserve/auth-github.sqlite")),
            token_ttl_days: match args.token_ttl_days {
                Some(v) => v,
                None => parse_u64_env("ANYSERVE_AUTH_TOKEN_TTL_DAYS", 30)?,
            },
            max_tokens_per_user: match args.max_tokens_per_user {
                Some(v) => v,
                None => parse_u64_env("ANYSERVE_AUTH_MAX_TOKENS_PER_USER", 10)?,
            }
            .max(1),
            min_account_age_days: match args.min_account_age_days {
                Some(v) => v,
                None => parse_u64_env("ANYSERVE_AUTH_MIN_ACCOUNT_AGE_DAYS", 0)?,
            },
            allowed_users,
            github_oauth_base: clean_nonempty(
                args.github_oauth_base
                    .or_else(|| env_str("ANYSERVE_AUTH_GITHUB_OAUTH_BASE")),
            )
            .unwrap_or_else(|| "https://github.com".to_string()),
            github_api_base: clean_nonempty(
                args.github_api_base
                    .or_else(|| env_str("ANYSERVE_AUTH_GITHUB_API_BASE")),
            )
            .unwrap_or_else(|| "https://api.github.com".to_string()),
        })
    }

    pub fn callback_url(&self) -> String {
        format!("{}/auth/github/callback", self.public_url)
    }

    pub fn cookie_secure(&self) -> bool {
        self.public_url
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
    }
}

fn clean_nonempty(raw: Option<String>) -> Option<String> {
    raw.and_then(|s| {
        let trimmed = s.trim();
        (!trimmed.is_empty()).then(|| trimmed.trim_end_matches('/').to_owned())
    })
}
