use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyserve_core::auth::{SignedTokenClaims, constant_time_eq, sign_token};
use axum::extract::{Path, Query, Request, State};
use axum::http::header::{
    CACHE_CONTROL, CONTENT_TYPE, HeaderName, SET_COOKIE, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::config::Config;
use crate::github::{GitHubClient, check_user_eligibility};
use crate::session::{
    OAUTH_STATE_TTL_SECS, OAuthState, SESSION_TTL_SECS, SessionUser, derive_cookie_key,
    format_cookie, oauth_state_cookie_name, read_cookie, seal_cookie, session_cookie_name,
    unseal_cookie,
};
use crate::store::{SqliteStore, TokenRecord, make_token_preview};

const PORTAL_HTML: &str = include_str!("portal.html");
const PORTAL_CSP: &str = "default-src 'self'; img-src 'self' https:; style-src 'unsafe-inline'; script-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'self'";

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: SqliteStore,
    pub github: GitHubClient,
    pub cookie_keys: Vec<[u8; 32]>,
}

impl AppState {
    pub fn new(config: Config, store: SqliteStore) -> anyhow::Result<Self> {
        let github = GitHubClient::new(
            config.github_oauth_base.clone(),
            config.github_api_base.clone(),
            config.github_client_id.clone(),
            config.github_client_secret.clone(),
            config.callback_url(),
        )?;
        let cookie_keys = config
            .signing_keys
            .iter()
            .map(|k| derive_cookie_key(k))
            .collect();
        Ok(Self {
            config: Arc::new(config),
            store,
            github,
            cookie_keys,
        })
    }

    fn primary_cookie_key(&self) -> &[u8; 32] {
        self.cookie_keys
            .first()
            .expect("AppState always has at least one cookie key")
    }

    fn unseal_any<T: serde::de::DeserializeOwned>(&self, purpose: &str, raw: &str) -> Option<T> {
        self.cookie_keys
            .iter()
            .find_map(|key| unseal_cookie(key, purpose, raw))
    }

    fn session_cookie_name(&self) -> &'static str {
        session_cookie_name(self.config.cookie_secure())
    }

    fn oauth_state_cookie_name(&self) -> &'static str {
        oauth_state_cookie_name(self.config.cookie_secure())
    }
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { StatusCode::NO_CONTENT }))
        .route("/", get(portal_page))
        .route("/tokens", get(portal_page))
        .route("/auth/login", get(portal_page))
        .route("/auth/github", get(oauth_start))
        .route("/auth/github/callback", get(oauth_callback))
        .route("/auth/logout", post(logout))
        .route("/auth/api/me", get(api_me))
        .route("/auth/api/tokens", post(api_mint_token))
        .route("/auth/api/tokens/:jti/revoke", post(api_revoke_token))
        .route("/auth/api/tokens/revoke-all", post(api_revoke_all_tokens))
        .route("/internal/revocations", get(internal_revocations))
        .layer(middleware::from_fn(security_headers))
        .with_state(state)
}

async fn security_headers(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    headers.insert(
        HeaderName::from_static("x-frame-options"),
        HeaderValue::from_static("DENY"),
    );
    headers.insert(
        HeaderName::from_static("referrer-policy"),
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(PORTAL_CSP),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    resp
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn portal_redirect_with_error(msg: &str) -> Response {
    let encoded: String = msg
        .bytes()
        .flat_map(|b| {
            if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
                vec![b as char]
            } else {
                format!("%{b:02X}").chars().collect()
            }
        })
        .collect();
    Redirect::temporary(&format!("/tokens?error={encoded}")).into_response()
}

fn json_error(status: StatusCode, msg: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": msg.into() }))).into_response()
}

fn internal_error(context: &'static str, err: impl std::fmt::Display) -> Response {
    tracing::warn!(context, error = %err, "internal auth plugin error");
    json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

fn require_csrf_header(headers: &HeaderMap) -> Result<(), &'static str> {
    let ok = headers
        .get("x-requested-with")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("fetch"));
    if ok {
        Ok(())
    } else {
        Err("missing X-Requested-With header")
    }
}

fn authenticated_user(state: &AppState, headers: &HeaderMap) -> Option<SessionUser> {
    let cookie_name = state.session_cookie_name();
    let raw = read_cookie(headers, cookie_name)?;
    let user: SessionUser = state.unseal_any(cookie_name, raw)?;
    (now_secs() < user.exp).then_some(user)
}

async fn portal_page() -> Response {
    (
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        )],
        Html(PORTAL_HTML),
    )
        .into_response()
}

async fn oauth_start(State(state): State<AppState>) -> Response {
    let nonce = Uuid::new_v4().simple().to_string();
    let state_cookie = state.oauth_state_cookie_name();
    let cookie_val = match seal_cookie(
        state.primary_cookie_key(),
        state_cookie,
        &OAuthState {
            nonce: nonce.clone(),
            exp: now_secs() + OAUTH_STATE_TTL_SECS,
        },
    ) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "seal OAuth state cookie");
            return portal_redirect_with_error("failed to start login");
        }
    };
    let set_cookie = format_cookie(
        state_cookie,
        &cookie_val,
        OAUTH_STATE_TTL_SECS as i64,
        state.config.cookie_secure(),
    );
    let url = state.github.authorize_url(&nonce);
    ([(SET_COOKIE, set_cookie)], Redirect::temporary(&url)).into_response()
}

#[derive(Deserialize)]
struct OAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

async fn oauth_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<OAuthCallbackQuery>,
) -> Response {
    let state_cookie = state.oauth_state_cookie_name();
    let clear_state = format_cookie(state_cookie, "", 0, state.config.cookie_secure());

    if let Some(err) = query.error {
        let msg = match err.as_str() {
            "access_denied" => "GitHub login was cancelled",
            "redirect_uri_mismatch" => "OAuth redirect URI mismatch",
            "incorrect_client_credentials" => "OAuth client configuration error",
            _ => "GitHub OAuth login failed; please try again",
        };
        return ([(SET_COOKIE, clear_state)], portal_redirect_with_error(msg)).into_response();
    }

    let Some(expected_cookie) = read_cookie(&headers, state_cookie)
        .and_then(|c| state.unseal_any::<OAuthState>(state_cookie, c))
        .filter(|s| now_secs() < s.exp)
    else {
        return (
            [(SET_COOKIE, clear_state)],
            portal_redirect_with_error("login state expired; please try again"),
        )
            .into_response();
    };

    let (Some(code), Some(got_state)) = (query.code, query.state) else {
        return (
            [(SET_COOKIE, clear_state)],
            portal_redirect_with_error("missing OAuth code or state"),
        )
            .into_response();
    };
    if !constant_time_eq(&expected_cookie.nonce, &got_state) {
        return (
            [(SET_COOKIE, clear_state)],
            portal_redirect_with_error("invalid OAuth state"),
        )
            .into_response();
    }

    let access_token = match state.github.exchange_code(&code).await {
        Ok(tok) => tok,
        Err(err) => {
            tracing::warn!(error = %err, "GitHub OAuth code exchange failed");
            return (
                [(SET_COOKIE, clear_state)],
                portal_redirect_with_error("GitHub OAuth login failed; please try again"),
            )
                .into_response();
        }
    };
    let gh_user = match state.github.fetch_user(&access_token).await {
        Ok(user) => user,
        Err(err) => {
            tracing::warn!(error = %err, "GitHub user profile fetch failed");
            return (
                [(SET_COOKIE, clear_state)],
                portal_redirect_with_error("failed to load GitHub profile; please try again"),
            )
                .into_response();
        }
    };

    let now = now_secs();
    if let Err(reason) = check_user_eligibility(
        &gh_user,
        &state.config.allowed_users,
        state.config.min_account_age_days,
        now,
    ) {
        return (
            [(SET_COOKIE, clear_state)],
            portal_redirect_with_error(&reason),
        )
            .into_response();
    }

    let namespace = format!("gh-{}", gh_user.id);
    if let Err(err) = state
        .store
        .upsert_user(
            gh_user.id,
            &gh_user.login,
            &gh_user.avatar_url,
            &namespace,
            now,
        )
        .await
    {
        tracing::warn!(error = %err, "save GitHub user record");
        return (
            [(SET_COOKIE, clear_state)],
            portal_redirect_with_error("failed to save user record; please try again"),
        )
            .into_response();
    }

    let session_name = state.session_cookie_name();
    let session_cookie = match seal_cookie(
        state.primary_cookie_key(),
        session_name,
        &SessionUser {
            github_id: gh_user.id,
            login: gh_user.login,
            avatar_url: gh_user.avatar_url,
            namespace,
            exp: now + SESSION_TTL_SECS,
        },
    ) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!(error = %err, "seal session cookie");
            return (
                [(SET_COOKIE, clear_state)],
                portal_redirect_with_error("failed to create session; please try again"),
            )
                .into_response();
        }
    };
    let set_session = format_cookie(
        session_name,
        &session_cookie,
        SESSION_TTL_SECS as i64,
        state.config.cookie_secure(),
    );

    let mut resp = Redirect::temporary("/tokens").into_response();
    if let Ok(val) = HeaderValue::from_str(&clear_state) {
        resp.headers_mut().append(SET_COOKIE, val);
    }
    if let Ok(val) = HeaderValue::from_str(&set_session) {
        resp.headers_mut().append(SET_COOKIE, val);
    }
    resp
}

async fn logout(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(msg) = require_csrf_header(&headers) {
        return json_error(StatusCode::FORBIDDEN, msg);
    }
    let clear = format_cookie(
        state.session_cookie_name(),
        "",
        0,
        state.config.cookie_secure(),
    );
    ([(SET_COOKIE, clear)], StatusCode::NO_CONTENT).into_response()
}

#[derive(Serialize)]
struct PortalUserDto {
    github_id: i64,
    login: String,
    avatar_url: String,
    namespace: String,
}

#[derive(Serialize)]
struct PortalConfigDto<'a> {
    grpc_endpoint: Option<&'a str>,
    ws_endpoint: Option<&'a str>,
    console_url: Option<&'a str>,
    token_ttl_days: u64,
    max_tokens_per_user: u64,
}

async fn api_me(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(user) = authenticated_user(&state, &headers) else {
        return Json(serde_json::json!({ "authenticated": false })).into_response();
    };
    let tokens = match state.store.list_user_tokens(user.github_id).await {
        Ok(list) => list,
        Err(err) => return internal_error("list user tokens", err),
    };
    Json(serde_json::json!({
        "authenticated": true,
        "user": PortalUserDto {
            github_id: user.github_id,
            login: user.login,
            avatar_url: user.avatar_url,
            namespace: user.namespace,
        },
        "config": PortalConfigDto {
            grpc_endpoint: state.config.grpc_endpoint.as_deref(),
            ws_endpoint: state.config.ws_endpoint.as_deref(),
            console_url: state.config.console_url.as_deref(),
            token_ttl_days: state.config.token_ttl_days,
            max_tokens_per_user: state.config.max_tokens_per_user,
        },
        "tokens": tokens,
    }))
    .into_response()
}

#[derive(Deserialize)]
struct MintTokenRequest {
    role: String,
    #[serde(default)]
    label: String,
}

async fn api_mint_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<MintTokenRequest>,
) -> Response {
    if let Err(msg) = require_csrf_header(&headers) {
        return json_error(StatusCode::FORBIDDEN, msg);
    }
    let Some(user) = authenticated_user(&state, &headers) else {
        return json_error(StatusCode::UNAUTHORIZED, "authentication required");
    };
    if !state.config.allowed_users.is_empty()
        && !state
            .config
            .allowed_users
            .contains(&user.login.to_ascii_lowercase())
    {
        return json_error(
            StatusCode::FORBIDDEN,
            "GitHub account is no longer allowlisted",
        );
    }
    let role = req.role.trim().to_ascii_lowercase();
    if role != "client" && role != "worker" {
        return json_error(StatusCode::BAD_REQUEST, "role must be 'client' or 'worker'");
    }
    let label: String = req
        .label
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .chars()
        .take(48)
        .collect();
    let label = if label.is_empty() {
        "default".to_string()
    } else {
        label
    };

    let now = now_secs();
    let jti = format!("jti_{}", Uuid::new_v4().simple());
    let exp = (state.config.token_ttl_days > 0)
        .then(|| now.saturating_add(state.config.token_ttl_days.saturating_mul(86_400)));
    let claims = SignedTokenClaims {
        jti: jti.clone(),
        ns: user.namespace.clone(),
        role: role.clone(),
        sub: format!("gh:{}", user.login),
        iat: now,
        exp,
        fp: None,
    };
    let token = match sign_token(state.config.primary_signing_key(), &claims) {
        Ok(t) => t,
        Err(err) => return internal_error("sign token", err),
    };
    let record = TokenRecord {
        jti,
        role,
        label,
        token_preview: make_token_preview(&token),
        created_at: now,
        expires_at: exp,
        revoked_at: None,
    };
    match state
        .store
        .mint_token_with_limit(
            user.github_id,
            &user.namespace,
            &record,
            now,
            state.config.max_tokens_per_user,
        )
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            return json_error(
                StatusCode::TOO_MANY_REQUESTS,
                format!(
                    "active token limit ({}) reached; revoke an unused token first",
                    state.config.max_tokens_per_user
                ),
            );
        }
        Err(err) => return internal_error("mint token", err),
    }

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "token": token,
            "record": record,
        })),
    )
        .into_response()
}

async fn api_revoke_token(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(jti): Path<String>,
) -> Response {
    if let Err(msg) = require_csrf_header(&headers) {
        return json_error(StatusCode::FORBIDDEN, msg);
    }
    let Some(user) = authenticated_user(&state, &headers) else {
        return json_error(StatusCode::UNAUTHORIZED, "authentication required");
    };
    match state
        .store
        .revoke_token(user.github_id, &jti, now_secs())
        .await
    {
        Ok(true) => Json(serde_json::json!({ "revoked": true, "jti": jti })).into_response(),
        Ok(false) => json_error(StatusCode::NOT_FOUND, "active token not found"),
        Err(err) => internal_error("revoke token", err),
    }
}

async fn api_revoke_all_tokens(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(msg) = require_csrf_header(&headers) {
        return json_error(StatusCode::FORBIDDEN, msg);
    }
    let Some(user) = authenticated_user(&state, &headers) else {
        return json_error(StatusCode::UNAUTHORIZED, "authentication required");
    };
    match state
        .store
        .revoke_all_tokens(user.github_id, now_secs())
        .await
    {
        Ok(count) => Json(serde_json::json!({ "revoked_count": count })).into_response(),
        Err(err) => internal_error("revoke all tokens", err),
    }
}

#[derive(Deserialize)]
struct RevocationsQuery {
    since_version: Option<u64>,
}

async fn internal_revocations(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<RevocationsQuery>,
) -> Response {
    let bearer = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim);
    let Some(candidate) = bearer else {
        return json_error(StatusCode::UNAUTHORIZED, "missing bearer token");
    };
    let valid_bearer = state
        .config
        .revocation_bearers
        .iter()
        .fold(false, |ok, expected| {
            ok | constant_time_eq(expected, candidate)
        });
    if !valid_bearer {
        return json_error(StatusCode::UNAUTHORIZED, "invalid internal bearer token");
    }

    match state
        .store
        .revocation_snapshot_if_changed(query.since_version, now_secs())
        .await
    {
        Ok(Some(snapshot)) => Json(snapshot).into_response(),
        Ok(None) => StatusCode::NOT_MODIFIED.into_response(),
        Err(err) => internal_error("read revocation snapshot", err),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use anyserve_core::auth::{AuthPolicy, RevocationSnapshot, Role};
    use axum::http::HeaderMap;
    use tokio::net::TcpListener;

    use super::*;

    async fn spawn_mock_github(
        user_id: i64,
        login: &'static str,
        created_at: &'static str,
    ) -> String {
        let router = Router::new()
            .route(
                "/login/oauth/access_token",
                post(|| async { Json(serde_json::json!({ "access_token": "gho_mock_123" })) }),
            )
            .route(
                "/user",
                get(move || async move {
                    Json(serde_json::json!({
                        "id": user_id,
                        "login": login,
                        "avatar_url": "https://avatars.example.com/u/1",
                        "created_at": created_at,
                    }))
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        format!("http://{addr}")
    }

    async fn spawn_plugin(
        mock_gh: &str,
        min_age_days: u64,
        allowed: Vec<String>,
    ) -> (String, Config) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let key = b"plugin-test-signing-key-32bytes!".to_vec();
        let old_key = b"plugin-old-signing-key-32bytes!!".to_vec();
        let cfg = Config {
            listen: addr,
            public_url: base_url.clone(),
            grpc_endpoint: Some("https://grpc.example.com".into()),
            ws_endpoint: Some("wss://ws.example.com".into()),
            console_url: Some("https://console.example.com".into()),
            revocation_bearers: vec![
                anyserve_core::auth::internal_revocation_bearer(&key),
                anyserve_core::auth::internal_revocation_bearer(&old_key),
            ],
            signing_keys: vec![key, old_key],
            github_client_id: "cid".into(),
            github_client_secret: "csecret".into(),
            db_path: PathBuf::from(":memory:"),
            token_ttl_days: 7,
            max_tokens_per_user: 2,
            min_account_age_days: min_age_days,
            allowed_users: allowed.into_iter().collect(),
            github_oauth_base: mock_gh.to_owned(),
            github_api_base: mock_gh.to_owned(),
        };
        let store = SqliteStore::open_in_memory().await.unwrap();
        let state = AppState::new(cfg.clone(), store).unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app(state)).await;
        });
        (base_url, cfg)
    }

    #[tokio::test]
    async fn full_github_login_mint_verify_and_revoke_lifecycle() {
        let mock_gh = spawn_mock_github(9001, "AliceDev", "2020-01-01T00:00:00Z").await;
        let (base, cfg) = spawn_plugin(&mock_gh, 30, Vec::new()).await;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        // 1. CSRF state mismatch is rejected.
        let bad_cb = client
            .get(format!("{base}/auth/github/callback?code=abc&state=bogus"))
            .send()
            .await
            .unwrap();
        assert_eq!(bad_cb.status(), StatusCode::TEMPORARY_REDIRECT);
        assert!(
            bad_cb.headers()["location"]
                .to_str()
                .unwrap()
                .contains("error=")
        );

        // 2. Real OAuth flow: /auth/github -> state cookie -> callback -> session cookie.
        let start = client
            .get(format!("{base}/auth/github"))
            .send()
            .await
            .unwrap();
        assert_eq!(start.status(), StatusCode::TEMPORARY_REDIRECT);
        let state_cookie_header = start.headers()[SET_COOKIE].to_str().unwrap().to_owned();
        let loc = start.headers()["location"].to_str().unwrap().to_owned();
        let state_param = loc
            .split("state=")
            .nth(1)
            .unwrap()
            .split('&')
            .next()
            .unwrap();
        let state_cookie_kv = state_cookie_header.split(';').next().unwrap().to_owned();

        let cb = client
            .get(format!(
                "{base}/auth/github/callback?code=ok&state={state_param}"
            ))
            .header("cookie", state_cookie_kv)
            .send()
            .await
            .unwrap();
        assert_eq!(cb.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(cb.headers()["location"], "/tokens");
        let session_cookie = cb
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .find(|s| s.starts_with(crate::session::SESSION_COOKIE_BASE))
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();

        // 3. Authenticated /auth/api/me returns gh-9001 namespace.
        let me: serde_json::Value = client
            .get(format!("{base}/auth/api/me"))
            .header("cookie", &session_cookie)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(me["authenticated"], true);
        assert_eq!(me["user"]["namespace"], "gh-9001");
        assert_eq!(me["user"]["login"], "AliceDev");

        // 4. POST without X-Requested-With is rejected; valid POST mints client & worker tokens up to limit 2.
        let missing_csrf = client
            .post(format!("{base}/auth/api/tokens"))
            .header("cookie", &session_cookie)
            .json(&serde_json::json!({ "role": "client", "label": "desk" }))
            .send()
            .await
            .unwrap();
        assert_eq!(missing_csrf.status(), StatusCode::FORBIDDEN);

        let mint_c: serde_json::Value = client
            .post(format!("{base}/auth/api/tokens"))
            .header("cookie", &session_cookie)
            .header("x-requested-with", "fetch")
            .json(&serde_json::json!({ "role": "client", "label": "desk" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let client_tok = mint_c["token"].as_str().unwrap().to_owned();
        let client_jti = mint_c["record"]["jti"].as_str().unwrap().to_owned();

        let mint_w: serde_json::Value = client
            .post(format!("{base}/auth/api/tokens"))
            .header("cookie", &session_cookie)
            .header("x-requested-with", "fetch")
            .json(&serde_json::json!({ "role": "worker", "label": "gpu-box" }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let worker_tok = mint_w["token"].as_str().unwrap().to_owned();

        // Limit = 2 -> 3rd active token fails with 429.
        let over = client
            .post(format!("{base}/auth/api/tokens"))
            .header("cookie", &session_cookie)
            .header("x-requested-with", "fetch")
            .json(&serde_json::json!({ "role": "client", "label": "extra" }))
            .send()
            .await
            .unwrap();
        assert_eq!(over.status(), StatusCode::TOO_MANY_REQUESTS);

        // 5. Verify minted tokens directly with anyserve_core::auth::AuthPolicy.
        let policy = AuthPolicy::default().with_signing_keys(cfg.signing_keys.clone());
        let mut ch = HeaderMap::new();
        ch.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {client_tok}").parse().unwrap(),
        );
        let cp = policy.authorize_console_headers(&ch).unwrap();
        assert_eq!(cp.namespace, "gh-9001");
        assert_eq!(cp.role, Role::Client);
        assert_eq!(cp.subject, "gh:AliceDev");
        assert!(!cp.is_operator);
        // Signed tenant tokens must never carry operator privilege (PermissionDenied = 7).
        assert_eq!(
            policy.authorize_operator_headers(&ch).unwrap_err().code() as i32,
            7
        );
        assert_eq!(policy.role_for_token(&worker_tok), Some(Role::Worker));

        // 6. /internal/revocations requires internal bearer (primary or rotated secondary); 200 vs 304 since_version.
        let unauth_rev = client
            .get(format!("{base}/internal/revocations"))
            .send()
            .await
            .unwrap();
        assert_eq!(unauth_rev.status(), StatusCode::UNAUTHORIZED);

        let rev0: RevocationSnapshot = client
            .get(format!("{base}/internal/revocations"))
            .bearer_auth(&cfg.revocation_bearers[0])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rev0.version, 0);
        assert!(rev0.revoked_jtis.is_empty());

        let not_mod = client
            .get(format!("{base}/internal/revocations?since_version=0"))
            .bearer_auth(&cfg.revocation_bearers[1])
            .send()
            .await
            .unwrap();
        assert_eq!(not_mod.status(), StatusCode::NOT_MODIFIED);

        // 7. Revoke client token -> snapshot v1 -> AuthPolicy rejects client_tok, keeps worker_tok.
        let rev_one = client
            .post(format!("{base}/auth/api/tokens/{client_jti}/revoke"))
            .header("cookie", &session_cookie)
            .header("x-requested-with", "fetch")
            .send()
            .await
            .unwrap();
        assert_eq!(rev_one.status(), StatusCode::OK);

        let rev1: RevocationSnapshot = client
            .get(format!("{base}/internal/revocations?since_version=0"))
            .bearer_auth(&cfg.revocation_bearers[0])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rev1.version, 1);
        assert_eq!(rev1.revoked_jtis, vec![client_jti.clone()]);
        policy.apply_revocations(rev1);
        assert!(policy.principal_for_token(&client_tok).is_none());
        assert!(policy.principal_for_token(&worker_tok).is_some());

        // 8. Revoke all remaining active tokens -> snapshot v2 -> worker_tok rejected too.
        let rev_all: serde_json::Value = client
            .post(format!("{base}/auth/api/tokens/revoke-all"))
            .header("cookie", &session_cookie)
            .header("x-requested-with", "fetch")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rev_all["revoked_count"], 1);

        let rev2: RevocationSnapshot = client
            .get(format!("{base}/internal/revocations?since_version=1"))
            .bearer_auth(&cfg.revocation_bearers[0])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(rev2.version, 2);
        policy.apply_revocations(rev2);
        assert!(policy.principal_for_token(&worker_tok).is_none());
    }

    #[tokio::test]
    async fn enforces_allowed_users_and_cross_user_isolation() {
        let mock_gh = spawn_mock_github(42, "BlockedUser", "2020-01-01T00:00:00Z").await;
        let (base, cfg) = spawn_plugin(&mock_gh, 0, vec!["onlyalice".into()]).await;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        let start = client
            .get(format!("{base}/auth/github"))
            .send()
            .await
            .unwrap();
        let state_cookie = start.headers()[SET_COOKIE]
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let loc = start.headers()["location"].to_str().unwrap().to_owned();
        let state_param = loc
            .split("state=")
            .nth(1)
            .unwrap()
            .split('&')
            .next()
            .unwrap();
        let cb = client
            .get(format!(
                "{base}/auth/github/callback?code=ok&state={state_param}"
            ))
            .header("cookie", state_cookie)
            .send()
            .await
            .unwrap();
        assert_eq!(cb.status(), StatusCode::TEMPORARY_REDIRECT);
        assert!(
            cb.headers()["location"]
                .to_str()
                .unwrap()
                .contains("error=")
        );
        // No session cookie was issued for disallowed user.
        let has_session = cb
            .headers()
            .get_all(SET_COOKIE)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .any(|v| v.starts_with(crate::session::SESSION_COOKIE_BASE));
        assert!(!has_session);
        assert!(!format!("{cfg:?}").contains("csecret"));

        // Verify an existing session cookie signed by the secondary key still authenticates,
        // while a non-allowlisted user holding a signed session cookie is blocked from minting.
        let secondary_cookie_key = derive_cookie_key(&cfg.signing_keys[1]);
        let cookie_name = session_cookie_name(cfg.cookie_secure());
        let forged_disallowed_cookie = seal_cookie(
            &secondary_cookie_key,
            cookie_name,
            &SessionUser {
                github_id: 42,
                login: "BlockedUser".into(),
                avatar_url: String::new(),
                namespace: "gh-42".into(),
                exp: now_secs() + 600,
            },
        )
        .unwrap();
        let mint_blocked = client
            .post(format!("{base}/auth/api/tokens"))
            .header(
                "cookie",
                format!("{cookie_name}={forged_disallowed_cookie}"),
            )
            .header("x-requested-with", "fetch")
            .json(&serde_json::json!({ "role": "client", "label": "late" }))
            .send()
            .await
            .unwrap();
        assert_eq!(mint_blocked.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn parallel_mint_token_with_limit_enforces_exact_cap() {
        let dir = std::env::temp_dir().join(format!("anyserve-auth-race-{}", Uuid::new_v4()));
        let db_path = dir.join("auth.sqlite");
        let store = SqliteStore::open(&db_path).await.unwrap();
        let now = now_secs();
        store
            .upsert_user(77, "racer", "", "gh-77", now)
            .await
            .unwrap();

        let mut tasks = Vec::new();
        for i in 0..20u64 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let record = TokenRecord {
                    jti: format!("jti-race-{i}"),
                    role: "client".into(),
                    label: format!("t-{i}"),
                    token_preview: "asv1.test...1234".into(),
                    created_at: now,
                    expires_at: Some(now + 3600),
                    revoked_at: None,
                };
                store
                    .mint_token_with_limit(77, "gh-77", &record, now, 5)
                    .await
                    .unwrap()
            }));
        }
        let mut minted = 0usize;
        for t in tasks {
            if t.await.unwrap() {
                minted += 1;
            }
        }
        assert_eq!(minted, 5);
        assert_eq!(store.list_user_tokens(77).await.unwrap().len(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn security_headers_host_cookies_key_rotation_and_error_sanitization() {
        let mock_gh = spawn_mock_github(501, "SecUser", "2020-01-01T00:00:00Z").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base = format!("http://{addr}");
        let key1 = b"sec-primary-key-32-bytes-long!!!".to_vec();
        let key2 = b"sec-rotated-key-32-bytes-long!!!".to_vec();
        let cfg = Config {
            listen: addr,
            // Mixed-case HTTPS must still trigger cookie_secure() and __Host- cookies.
            public_url: "HTTPS://auth.example.com".into(),
            grpc_endpoint: None,
            ws_endpoint: None,
            console_url: None,
            revocation_bearers: vec![anyserve_core::auth::internal_revocation_bearer(&key1)],
            signing_keys: vec![key1.clone(), key2.clone()],
            github_client_id: "cid".into(),
            github_client_secret: "csecret".into(),
            db_path: PathBuf::from(":memory:"),
            token_ttl_days: 7,
            max_tokens_per_user: 5,
            min_account_age_days: 0,
            allowed_users: Default::default(),
            github_oauth_base: mock_gh.clone(),
            github_api_base: mock_gh,
        };
        assert!(cfg.cookie_secure());
        let store = SqliteStore::open_in_memory().await.unwrap();
        let state = AppState::new(cfg.clone(), store).unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app(state)).await;
        });

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();

        // 1. Security headers present on portal, API, and internal revocation routes.
        let portal = client.get(format!("{base}/")).send().await.unwrap();
        assert_eq!(portal.headers()["x-frame-options"], "DENY");
        assert_eq!(portal.headers()["referrer-policy"], "no-referrer");
        assert_eq!(portal.headers()["x-content-type-options"], "nosniff");
        assert_eq!(portal.headers()["cache-control"], "no-store");
        let csp = portal.headers()["content-security-policy"]
            .to_str()
            .unwrap();
        assert!(csp.contains("frame-ancestors 'none'"));
        assert!(csp.contains("script-src 'unsafe-inline'"));

        // 2. __Host- prefix + Secure attribute on HTTPS public_url.
        let start = client
            .get(format!("{base}/auth/github"))
            .send()
            .await
            .unwrap();
        let set_cookie = start.headers()[SET_COOKIE].to_str().unwrap();
        assert!(set_cookie.starts_with("__Host-anyserve_oauth_state="));
        assert!(set_cookie.contains("; Secure"));

        // 3. Purpose-bound HMAC blocks cross-purpose replay and allows secondary rotated key.
        let k2 = derive_cookie_key(&key2);
        let user = SessionUser {
            github_id: 501,
            login: "SecUser".into(),
            avatar_url: String::new(),
            namespace: "gh-501".into(),
            exp: now_secs() + 600,
        };
        let wrong_purpose = seal_cookie(&k2, "__Host-anyserve_oauth_state", &user).unwrap();
        let unauth_me: serde_json::Value = client
            .get(format!("{base}/auth/api/me"))
            .header("cookie", format!("__Host-anyserve_session={wrong_purpose}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(unauth_me["authenticated"], false);

        let right_purpose = seal_cookie(&k2, "__Host-anyserve_session", &user).unwrap();
        let ok_me = client
            .get(format!("{base}/auth/api/me"))
            .header("cookie", format!("__Host-anyserve_session={right_purpose}"))
            .send()
            .await
            .unwrap();
        assert_eq!(ok_me.headers()["cache-control"], "no-store");
        let ok_json: serde_json::Value = ok_me.json().await.unwrap();
        assert_eq!(ok_json["authenticated"], true);
        assert_eq!(ok_json["user"]["namespace"], "gh-501");

        // 4. oauth_callback does NOT reflect attacker-controlled error_description.
        let cb = client
            .get(format!(
                "{base}/auth/github/callback?error=access_denied&error_description=phishing+evil.example"
            ))
            .send()
            .await
            .unwrap();
        let loc = cb.headers()["location"].to_str().unwrap();
        assert!(!loc.contains("phishing"));
        assert!(!loc.contains("evil.example"));
        assert!(loc.contains("error="));
    }
}
