use anyserve_core::auth::{constant_time_eq, hmac_sha256};
use axum::http::HeaderMap;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest as _, Sha256};

pub const SESSION_COOKIE_NAME: &str = "anyserve_auth_session";
pub const OAUTH_STATE_COOKIE_NAME: &str = "anyserve_oauth_state";
pub const SESSION_TTL_SECS: u64 = 24 * 60 * 60;
pub const OAUTH_STATE_TTL_SECS: u64 = 10 * 60;

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

pub fn seal_cookie<T: Serialize>(key: &[u8; 32], value: &T) -> anyhow::Result<String> {
    let json = serde_json::to_vec(value)?;
    let payload = URL_SAFE_NO_PAD.encode(json);
    let sig = URL_SAFE_NO_PAD.encode(hmac_sha256(key, payload.as_bytes()));
    Ok(format!("{payload}.{sig}"))
}

pub fn unseal_cookie<T: DeserializeOwned>(key: &[u8; 32], raw: &str) -> Option<T> {
    let (payload_b64, sig_b64) = raw.trim().split_once('.')?;
    if sig_b64.contains('.') {
        return None;
    }
    let expected_sig = URL_SAFE_NO_PAD.encode(hmac_sha256(key, payload_b64.as_bytes()));
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
