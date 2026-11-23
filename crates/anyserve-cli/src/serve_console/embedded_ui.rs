use axum::body::Body;
use axum::http::header::{
    ALLOW, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};

#[cfg(feature = "embed-console")]
use axum::http::Request;

pub(crate) struct EmbeddedAsset {
    pub(crate) path: &'static str,
    pub(crate) content_type: &'static str,
    pub(crate) immutable: bool,
    pub(crate) bytes: &'static [u8],
}

#[cfg(feature = "embed-console")]
include!(concat!(env!("OUT_DIR"), "/embedded_console_assets.rs"));

#[cfg(feature = "embed-console")]
pub(crate) async fn fallback_handler(request: Request<Body>) -> Response {
    serve_embedded_asset(
        EMBEDDED_CONSOLE_ASSETS,
        request.method(),
        request.uri().path(),
    )
}

pub(crate) fn is_control_plane_path(path: &str) -> bool {
    matches!(path, "/healthz" | "/readyz" | "/metrics")
        || ["/api", "/rpc", "/ws"]
            .into_iter()
            .any(|prefix| path == prefix || path.starts_with(&format!("{prefix}/")))
}

fn asset_response(asset: &EmbeddedAsset, head_only: bool) -> Response {
    let cache_control = if asset.immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let body = if head_only {
        Body::empty()
    } else {
        Body::from(asset.bytes)
    };
    (
        StatusCode::OK,
        [
            (CONTENT_TYPE, asset.content_type.to_owned()),
            (CACHE_CONTROL, cache_control.to_owned()),
            (X_CONTENT_TYPE_OPTIONS, "nosniff".to_owned()),
            (CONTENT_LENGTH, asset.bytes.len().to_string()),
        ],
        body,
    )
        .into_response()
}

pub(crate) fn serve_embedded_asset(
    assets: &[EmbeddedAsset],
    method: &Method,
    raw_path: &str,
) -> Response {
    let path = raw_path.split('?').next().unwrap_or(raw_path);
    if is_control_plane_path(path) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if !matches!(*method, Method::GET | Method::HEAD) {
        return (
            StatusCode::METHOD_NOT_ALLOWED,
            [(ALLOW, "GET, HEAD")],
            Body::empty(),
        )
            .into_response();
    }
    let lookup_path = if path == "/" { "/index.html" } else { path };
    if lookup_path.split('/').any(|segment| segment == "..") {
        return StatusCode::NOT_FOUND.into_response();
    }

    if let Some(asset) = assets.iter().find(|asset| asset.path == lookup_path) {
        return asset_response(asset, *method == Method::HEAD);
    }

    let last_segment = lookup_path.rsplit('/').next().unwrap_or("");
    if last_segment.contains('.') {
        return StatusCode::NOT_FOUND.into_response();
    }

    match assets.iter().find(|asset| asset.path == "/index.html") {
        Some(index) => asset_response(index, *method == Method::HEAD),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::CONTENT_ENCODING;

    const TEST_ASSETS: &[EmbeddedAsset] = &[
        EmbeddedAsset {
            path: "/index.html",
            content_type: "text/html; charset=utf-8",
            immutable: false,
            bytes: b"<html>ok</html>",
        },
        EmbeddedAsset {
            path: "/assets/app-123.js",
            content_type: "application/javascript; charset=utf-8",
            immutable: true,
            bytes: b"console.log(1);",
        },
        EmbeddedAsset {
            path: "/assets/duckdb-eh-123.wasm.bin",
            content_type: "application/octet-stream",
            immutable: true,
            bytes: b"\x1f\x8b\x08\x00test",
        },
    ];

    #[test]
    fn serves_index_assets_and_spa_fallbacks_with_expected_headers() {
        let root = serve_embedded_asset(TEST_ASSETS, &Method::GET, "/");
        assert_eq!(root.status(), StatusCode::OK);
        assert_eq!(root.headers()[CONTENT_TYPE], "text/html; charset=utf-8");
        assert_eq!(root.headers()[CACHE_CONTROL], "no-cache");
        assert_eq!(root.headers()[X_CONTENT_TYPE_OPTIONS], "nosniff");

        let spa = serve_embedded_asset(TEST_ASSETS, &Method::GET, "/explore");
        assert_eq!(spa.status(), StatusCode::OK);
        assert_eq!(spa.headers()[CONTENT_TYPE], "text/html; charset=utf-8");
        assert_eq!(spa.headers()[CACHE_CONTROL], "no-cache");

        let js = serve_embedded_asset(TEST_ASSETS, &Method::HEAD, "/assets/app-123.js");
        assert_eq!(js.status(), StatusCode::OK);
        assert_eq!(
            js.headers()[CACHE_CONTROL],
            "public, max-age=31536000, immutable"
        );
        assert_eq!(js.headers()[CONTENT_LENGTH], "15");

        // .wasm.bin must NOT carry Content-Encoding: gzip (Explore decompresses via DecompressionStream)
        let wasm =
            serve_embedded_asset(TEST_ASSETS, &Method::GET, "/assets/duckdb-eh-123.wasm.bin");
        assert_eq!(wasm.status(), StatusCode::OK);
        assert_eq!(wasm.headers()[CONTENT_TYPE], "application/octet-stream");
        assert!(wasm.headers().get(CONTENT_ENCODING).is_none());
    }

    #[test]
    fn rejects_control_plane_paths_missing_assets_traversal_and_non_get_methods() {
        for path in [
            "/api",
            "/api/unknown",
            "/rpc/v2/unknown",
            "/ws/v2/unknown",
            "/healthz",
        ] {
            assert_eq!(
                serve_embedded_asset(TEST_ASSETS, &Method::GET, path).status(),
                StatusCode::NOT_FOUND,
                "path {path}"
            );
        }
        assert_eq!(
            serve_embedded_asset(TEST_ASSETS, &Method::GET, "/assets/missing.js").status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            serve_embedded_asset(TEST_ASSETS, &Method::GET, "/assets/../index.html").status(),
            StatusCode::NOT_FOUND
        );
        let post = serve_embedded_asset(TEST_ASSETS, &Method::POST, "/");
        assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(post.headers()[ALLOW], "GET, HEAD");
    }
}
