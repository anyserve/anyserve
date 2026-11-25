use anyserve_core::auth::{constant_time_eq, hmac_sha256};
use axum::http::HeaderMap;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};

pub const SESSION_COOKIE_BASE: &str = "anyserve_session";
pub const OAUTH_STATE_COOKIE_BASE: &str = "anyserve_oauth_state";
pub const HOST_SESSION_COOKIE_NAME: &str = "__Host-anyserve_session";
pub const HOST_OAUTH_STATE_COOKIE_NAME: &str = "__Host-anyserve_oauth_state";
pub const SESSION_TTL_SECS: u64 = 24 * 60 * 60;
pub const OAUTH_STATE_TTL_SECS: u64 = 10 * 60;

pub fn session_cookie_name(secure: bool) -> &'static str {
    if secure {
        HOST_SESSION_COOKIE_NAME
    } else {
        SESSION_COOKIE_BASE
    }
}

pub fn oauth_state_cookie_name(secure: bool) -> &'static str {
    if secure {
        HOST_OAUTH_STATE_COOKIE_NAME
    } else {
        OAUTH_STATE_COOKIE_BASE
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionUser {
    pub github_id: i64,
    pub login: String,
    pub avatar_url: String,
    pub namespace: String,
    pub exp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OAuthState {
    pub nonce: String,
    pub exp: u64,
}

pub fn derive_cookie_key(signing_key: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"anyserve-auth-github-cookie-v1:");
    hasher.update(signing_key);
    hasher.finalize().into()
}

fn cookie_signing_input(purpose: &str, payload_b64: &str) -> Vec<u8> {
    let mut input = Vec::with_capacity(purpose.len() + 1 + payload_b64.len());
    input.extend_from_slice(purpose.as_bytes());
    input.push(b':');
    input.extend_from_slice(payload_b64.as_bytes());
    input
}

pub fn seal_cookie<T: Serialize>(
    key: &[u8; 32],
    purpose: &str,
    value: &T,
) -> anyhow::Result<String> {
    let json = serde_json::to_vec(value)?;
    let payload = URL_SAFE_NO_PAD.encode(json);
    let input = cookie_signing_input(purpose, &payload);
    let sig = URL_SAFE_NO_PAD.encode(hmac_sha256(key, &input));
    Ok(format!("{payload}.{sig}"))
}

pub fn unseal_cookie<T: DeserializeOwned>(key: &[u8; 32], purpose: &str, raw: &str) -> Option<T> {
    let (payload_b64, sig_b64) = raw.trim().split_once('.')?;
    if sig_b64.contains('.') {
        return None;
    }
    let input = cookie_signing_input(purpose, payload_b64);
    let expected_sig = URL_SAFE_NO_PAD.encode(hmac_sha256(key, &input));
    if !constant_time_eq(&expected_sig, sig_b64) {
        return None;
    }
    let bytes = URL_SAFE_NO_PAD.decode(payload_b64).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn read_cookie<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    for header in headers.get_all(axum::http::header::COOKIE) {
        let Ok(raw) = header.to_str() else {
            continue;
        };
        for part in raw.split(';') {
            let trimmed = part.trim();
            if let Some((k, v)) = trimmed.split_once('=')
                && k.trim() == name
                && !v.trim().is_empty()
            {
                return Some(v.trim());
            }
        }
    }
    None
}

pub fn format_cookie(name: &str, value: &str, max_age_secs: i64, secure: bool) -> String {
    let secure_attr = if secure { "; Secure" } else { "" };
    format!("{name}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age_secs}{secure_attr}")
}
