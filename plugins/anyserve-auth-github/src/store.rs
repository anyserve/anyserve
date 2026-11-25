use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use anyserve_core::auth::RevocationSnapshot;
use serde::Serialize;
use sqlx::Row;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

#[derive(Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct TokenRecord {
    pub jti: String,
    pub role: String,
    pub label: String,
    pub token_preview: String,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub revoked_at: Option<u64>,
}

pub fn make_token_preview(token: &str) -> String {
    let chars: Vec<char> = token.chars().collect();
    if chars.len() <= 16 {
        return "[redacted]".to_string();
    }
    let prefix: String = chars[..12].iter().collect();
    let suffix: String = chars[chars.len() - 4..].iter().collect();
    format!("{prefix}...{suffix}")
}

impl SqliteStore {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)
                .with_context(|| format!("create directory {}", parent.display()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .with_context(|| format!("open sqlite store {}", path.display()))?;
        Self::init(pool).await
    }

    #[cfg(test)]
    pub async fn open_in_memory() -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(":memory:")
            .in_memory(true)
            .shared_cache(false);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .context("open in-memory sqlite store")?;
        Self::init(pool).await
    }

    async fn init(pool: SqlitePool) -> Result<Self> {
        let store = Self { pool };
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS users (
                github_id INTEGER PRIMARY KEY,
                login TEXT NOT NULL,
                avatar_url TEXT NOT NULL,
                namespace TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL,
                last_login_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&store.pool)
        .await
        .context("create users table")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tokens (
                jti TEXT PRIMARY KEY,
                github_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                role TEXT NOT NULL,
                label TEXT NOT NULL,
                token_preview TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER,
                revoked_at INTEGER
            )
            "#,
        )
        .execute(&store.pool)
        .await
        .context("create tokens table")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tokens_user_created ON tokens(github_id, created_at DESC)",
        )
        .execute(&store.pool)
        .await
        .context("create tokens user index")?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_tokens_revoked ON tokens(revoked_at) WHERE revoked_at IS NOT NULL",
        )
        .execute(&store.pool)
        .await
        .context("create tokens revoked index")?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            "#,
        )
        .execute(&store.pool)
        .await
        .context("create meta table")?;

        sqlx::query("INSERT OR IGNORE INTO meta(key, value) VALUES ('revocation_version', '0')")
            .execute(&store.pool)
            .await
            .context("seed revocation_version")?;

        Ok(store)
    }

    pub async fn upsert_user(
        &self,
        github_id: i64,
        login: &str,
        avatar_url: &str,
        namespace: &str,
        now_secs: u64,
    ) -> Result<()> {
        let now_i64 = i64::try_from(now_secs).context("timestamp out of range")?;
        sqlx::query(
            r#"
            INSERT INTO users(github_id, login, avatar_url, namespace, created_at, last_login_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?5)
            ON CONFLICT(github_id) DO UPDATE SET
                login = excluded.login,
                avatar_url = excluded.avatar_url,
                last_login_at = excluded.last_login_at
            "#,
        )
        .bind(github_id)
        .bind(login)
        .bind(avatar_url)
        .bind(namespace)
        .bind(now_i64)
        .execute(&self.pool)
        .await
        .context("upsert github user")?;
        Ok(())
    }

    pub async fn list_user_tokens(&self, github_id: i64) -> Result<Vec<TokenRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT jti, role, label, token_preview, created_at, expires_at, revoked_at
            FROM tokens
            WHERE github_id = ?1
            ORDER BY created_at DESC, jti ASC
            "#,
        )
        .bind(github_id)
        .fetch_all(&self.pool)
        .await
        .context("list user tokens")?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let created_at: i64 = row.try_get("created_at")?;
            let expires_at: Option<i64> = row.try_get("expires_at")?;
            let revoked_at: Option<i64> = row.try_get("revoked_at")?;
            out.push(TokenRecord {
                jti: row.try_get("jti")?,
                role: row.try_get("role")?,
                label: row.try_get("label")?,
                token_preview: row.try_get("token_preview")?,
                created_at: u64::try_from(created_at.max(0)).unwrap_or(0),
                expires_at: expires_at.map(|v| u64::try_from(v.max(0)).unwrap_or(0)),
                revoked_at: revoked_at.map(|v| u64::try_from(v.max(0)).unwrap_or(0)),
            });
        }
        Ok(out)
    }

    pub async fn revoke_token(&self, github_id: i64, jti: &str, now_secs: u64) -> Result<bool> {
        let now_i64 = i64::try_from(now_secs).context("timestamp out of range")?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin revoke transaction")?;
        let res = sqlx::query(
            r#"
            UPDATE tokens
            SET revoked_at = ?1
            WHERE jti = ?2 AND github_id = ?3 AND revoked_at IS NULL
            "#,
        )
        .bind(now_i64)
        .bind(jti)
        .bind(github_id)
        .execute(&mut *tx)
        .await
        .context("mark token revoked")?;
        let changed = res.rows_affected() > 0;
        if changed {
            sqlx::query(
                "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'revocation_version'",
            )
            .execute(&mut *tx)
            .await
            .context("increment revocation_version")?;
        }
        tx.commit().await.context("commit revoke transaction")?;
        Ok(changed)
    }

    pub async fn revoke_all_tokens(&self, github_id: i64, now_secs: u64) -> Result<u64> {
        let now_i64 = i64::try_from(now_secs).context("timestamp out of range")?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin revoke-all transaction")?;
        let res = sqlx::query(
            r#"
            UPDATE tokens
            SET revoked_at = ?1
            WHERE github_id = ?2 AND revoked_at IS NULL
            "#,
        )
        .bind(now_i64)
        .bind(github_id)
        .execute(&mut *tx)
        .await
        .context("revoke all user tokens")?;
        let count = res.rows_affected();
        if count > 0 {
            sqlx::query(
                "UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'revocation_version'",
            )
            .execute(&mut *tx)
            .await
            .context("increment revocation_version")?;
        }
        tx.commit().await.context("commit revoke-all transaction")?;
        Ok(count)
    }

    /// Records a minted token unless the user already holds `max_tokens` active ones;
    /// returns `false` at the limit. One statement keeps the count and the insert atomic.
    pub async fn mint_token_with_limit(
        &self,
        github_id: i64,
        namespace: &str,
        record: &TokenRecord,
        now_secs: u64,
        max_tokens: u64,
    ) -> Result<bool> {
        let now_i64 = i64::try_from(now_secs).context("timestamp out of range")?;
        let created = i64::try_from(record.created_at).context("created_at out of range")?;
        let expires = record
            .expires_at
            .map(i64::try_from)
            .transpose()
            .context("expires_at out of range")?;
        let inserted = sqlx::query(
            r#"
            INSERT INTO tokens(jti, github_id, namespace, role, label, token_preview, created_at, expires_at, revoked_at)
            SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL
            WHERE (
                SELECT COUNT(*)
                FROM tokens
                WHERE github_id = ?2
                  AND revoked_at IS NULL
                  AND (expires_at IS NULL OR expires_at > ?9)
            ) < ?10
            "#,
        )
        .bind(&record.jti)
        .bind(github_id)
        .bind(namespace)
        .bind(&record.role)
        .bind(&record.label)
        .bind(&record.token_preview)
        .bind(created)
        .bind(expires)
        .bind(now_i64)
        .bind(i64::try_from(max_tokens).unwrap_or(i64::MAX))
        .execute(&self.pool)
        .await
        .context("insert minted token metadata")?
        .rows_affected();
        Ok(inserted == 1)
    }

    pub async fn revocation_snapshot_if_changed(
        &self,
        since_version: Option<u64>,
        now_secs: u64,
    ) -> Result<Option<RevocationSnapshot>> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin revocation snapshot transaction")?;
        let ver_row = sqlx::query(
            "SELECT CAST(value AS INTEGER) AS v FROM meta WHERE key = 'revocation_version'",
        )
        .fetch_one(&mut *tx)
        .await
        .context("read revocation_version")?;
        let version_i64: i64 = ver_row.try_get("v")?;
        let version = u64::try_from(version_i64.max(0)).unwrap_or(0);
        if since_version == Some(version) {
            return Ok(None);
        }

        let cutoff = i64::try_from(now_secs.saturating_sub(60)).unwrap_or(0);
        let jti_rows = sqlx::query(
            r#"
            SELECT jti
            FROM tokens
            WHERE revoked_at IS NOT NULL
              AND (expires_at IS NULL OR expires_at >= ?1)
            "#,
        )
        .bind(cutoff)
        .fetch_all(&mut *tx)
        .await
        .context("read active revoked jtis")?;

        let mut revoked_jtis = Vec::with_capacity(jti_rows.len());
        for row in jti_rows {
            revoked_jtis.push(row.try_get("jti")?);
        }
        tx.commit().await.context("commit revocation snapshot tx")?;
        Ok(Some(RevocationSnapshot {
            version,
            revoked_jtis,
        }))
    }
}
