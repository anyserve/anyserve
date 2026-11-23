use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn collect_assets(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    println!("cargo:rerun-if-changed={}", dir.display());
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("read console dist dir {}: {err}", dir.display()))
        .map(|entry| {
            entry.unwrap_or_else(|err| panic!("read dir entry in {}: {err}", dir.display()))
        })
        .collect();
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let file_type = entry
            .file_type()
            .unwrap_or_else(|err| panic!("stat {}: {err}", path.display()));
        if file_type.is_dir() {
            collect_assets(root, &path, out);
        } else if file_type.is_file() {
            println!("cargo:rerun-if-changed={}", path.display());
            let rel = path
                .strip_prefix(root)
                .expect("asset inside dist root")
                .to_string_lossy()
                .replace('\\', "/");
            out.push((format!("/{rel}"), path));
        }
    }
}

fn content_type_for(url_path: &str) -> &'static str {
    let ext = Path::new(url_path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    match ext {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "svg" => "image/svg+xml",
        "json" | "map" => "application/json; charset=utf-8",
        "wasm" => "application/wasm",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "bin" => "application/octet-stream",
        _ => "application/octet-stream",
    }
}

fn main() {
    if env::var_os("CARGO_FEATURE_EMBED_CONSOLE").is_none() {
        return;
    }

    println!("cargo:rerun-if-env-changed=ANYSERVE_CONSOLE_DIST_DIR");
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let dist_dir = env::var_os("ANYSERVE_CONSOLE_DIST_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("../../console/dist"));

    let index_html = dist_dir.join("index.html");
    if !index_html.is_file() {
        panic!(
            "embed-console feature requires built Console assets at {}; run `npm --prefix console ci && npm --prefix console run build`",
            dist_dir.display()
        );
    }

    let abs_dist = dist_dir
        .canonicalize()
        .unwrap_or_else(|err| panic!("canonicalize {}: {err}", dist_dir.display()));

    let mut assets = Vec::new();
    collect_assets(&abs_dist, &abs_dist, &mut assets);
    assets.sort_by(|a, b| a.0.cmp(&b.0));

    let mut generated =
        String::from("pub(crate) const EMBEDDED_CONSOLE_ASSETS: &[EmbeddedAsset] = &[\n");
    for (url_path, abs_path) in assets {
        let content_type = content_type_for(&url_path);
        let immutable = url_path.starts_with("/assets/");
        let path_str = abs_path
            .to_str()
            .unwrap_or_else(|| panic!("non-UTF8 asset path: {}", abs_path.display()));
        generated.push_str(&format!(
            "    EmbeddedAsset {{ path: {url_path:?}, content_type: {content_type:?}, immutable: {immutable}, bytes: include_bytes!({path_str:?}) }},\n"
        ));
    }
    generated.push_str("];\n");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    fs::write(out_dir.join("embedded_console_assets.rs"), generated)
        .expect("write embedded_console_assets.rs");
}
