use std::collections::BTreeSet;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct GitHubClient {
    http: reqwest::Client,
    oauth_base: String,
    api_base: String,
    client_id: String,
    client_secret: String,
    callback_url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GitHubUser {
    pub id: i64,
    pub login: String,
    #[serde(default)]
    pub avatar_url: String,
    #[serde(default)]
    pub created_at: String,
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

impl GitHubClient {
    pub fn new(
        oauth_base: String,
        api_base: String,
        client_id: String,
        client_secret: String,
        callback_url: String,
    ) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .context("build GitHub HTTP client")?;
        Ok(Self {
            http,
            oauth_base,
            api_base,
            client_id,
            client_secret,
            callback_url,
        })
    }

    pub fn authorize_url(&self, state: &str) -> String {
        format!(
            "{}/login/oauth/authorize?client_id={}&redirect_uri={}&state={}&scope=read%3Auser",
            self.oauth_base,
            encode_query_component(&self.client_id),
            encode_query_component(&self.callback_url),
            encode_query_component(state),
        )
    }

    pub async fn exchange_code(&self, code: &str) -> Result<String> {
        let resp = self
            .http
            .post(format!("{}/login/oauth/access_token", self.oauth_base))
            .header(reqwest::header::ACCEPT, "application/json")
            .header(reqwest::header::USER_AGENT, "anyserve-auth-github")
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("code", code),
                ("redirect_uri", self.callback_url.as_str()),
            ])
            .send()
            .await
            .context("exchange GitHub OAuth code")?;
        anyhow::ensure!(
            resp.status().is_success(),
            "GitHub OAuth token exchange returned HTTP {}",
            resp.status()
        );
        let payload: OAuthTokenResponse = resp
            .json()
            .await
            .context("parse GitHub OAuth token response")?;
        if let Some(tok) = payload.access_token.filter(|t| !t.trim().is_empty()) {
            return Ok(tok);
        }
        let err = payload.error.unwrap_or_else(|| "oauth_failed".into());
        let desc = payload.error_description.unwrap_or_default();
        anyhow::bail!("GitHub OAuth failed ({err}): {desc}");
    }

    pub async fn fetch_user(&self, access_token: &str) -> Result<GitHubUser> {
        let resp = self
            .http
            .get(format!("{}/user", self.api_base))
            .bearer_auth(access_token)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header(reqwest::header::USER_AGENT, "anyserve-auth-github")
            .send()
            .await
            .context("fetch GitHub user profile")?;
        anyhow::ensure!(
            resp.status().is_success(),
            "GitHub /user returned HTTP {}",
            resp.status()
        );
        resp.json::<GitHubUser>()
            .await
            .context("parse GitHub /user JSON")
    }
}

pub fn check_user_eligibility(
    user: &GitHubUser,
    allowed_users: &BTreeSet<String>,
    min_account_age_days: u64,
    now_secs: u64,
) -> std::result::Result<(), String> {
    if !allowed_users.is_empty() && !allowed_users.contains(&user.login.to_ascii_lowercase()) {
        return Err(format!(
            "GitHub user '{}' is not in ANYSERVE_AUTH_ALLOWED_USERS",
            user.login
        ));
    }
    if min_account_age_days > 0 {
        let created = parse_rfc3339_utc_secs(&user.created_at)
            .ok_or_else(|| "unable to verify GitHub account creation age".to_string())?;
        let required_age = min_account_age_days.saturating_mul(86_400);
        if now_secs.saturating_sub(created) < required_age {
            return Err(format!(
                "GitHub account must be at least {min_account_age_days} days old"
            ));
        }
    }
    Ok(())
}

pub fn parse_rfc3339_utc_secs(raw: &str) -> Option<u64> {
    let s = raw.trim().strip_suffix('Z')?;
    let (date_part, time_part) = s.split_once('T')?;
    let mut date_it = date_part.split('-');
    let year: i64 = date_it.next()?.parse().ok()?;
    let month: u32 = date_it.next()?.parse().ok()?;
    let day: u32 = date_it.next()?.parse().ok()?;
    if date_it.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let time_no_frac = time_part.split('.').next()?;
    let mut time_it = time_no_frac.split(':');
    let hour: u64 = time_it.next()?.parse().ok()?;
    let min: u64 = time_it.next()?.parse().ok()?;
    let sec: u64 = time_it.next()?.parse().ok()?;
    if time_it.next().is_some() || hour >= 24 || min >= 60 || sec >= 60 {
        return None;
    }

    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }
    Some((days as u64) * 86_400 + hour * 3_600 + min * 60 + sec)
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i64::from(month);
    let d = i64::from(day);
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn encode_query_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_github_rfc3339_timestamps() {
        assert_eq!(parse_rfc3339_utc_secs("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_rfc3339_utc_secs("2020-05-01T12:34:56Z"),
            Some(1_588_336_496)
        );
        assert_eq!(
            parse_rfc3339_utc_secs("2024-02-29T00:00:00.123Z"),
            Some(1_709_164_800)
        );
        assert_eq!(parse_rfc3339_utc_secs("not-a-date"), None);
    }

    #[test]
    fn enforces_allowlist_and_account_age() {
        let user = GitHubUser {
            id: 42,
            login: "OctoCat".into(),
            avatar_url: String::new(),
            created_at: "2024-01-01T00:00:00Z".into(),
        };
        let created = parse_rfc3339_utc_secs(&user.created_at).unwrap();
        let allow: BTreeSet<String> = ["octocat".into()].into_iter().collect();

        assert!(check_user_eligibility(&user, &allow, 30, created + 31 * 86_400).is_ok());
        assert!(check_user_eligibility(&user, &allow, 30, created + 10 * 86_400).is_err());

        let denied: BTreeSet<String> = ["someone-else".into()].into_iter().collect();
        assert!(check_user_eligibility(&user, &denied, 0, created).is_err());
    }
}
