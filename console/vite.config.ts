import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";
import { defineConfig, loadEnv, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

const DUCKDB_EH_WASM_PATTERN = /^duckdb-eh-.*\.wasm$/;
const TEXT_ASSET_PATTERN = /\.(?:m?js|css|json|html|map)$/;

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function collectFiles(dir: string): string[] {
  const results: string[] = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectFiles(fullPath));
    } else if (entry.isFile()) {
      results.push(fullPath);
    }
  }
  return results;
}

function gzipDuckdbWasmPlugin(): Plugin {
  const defaultOutDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "dist");
  return {
    name: "anyserve-gzip-duckdb-wasm",
    apply: "build",
    writeBundle(outputOptions) {
      if (process.env.ANYSERVE_BUNDLE_GZIP_WASM === "0") return;
      const outDir = outputOptions.dir ?? defaultOutDir;
      const assetsDir = path.join(outDir, "assets");
      if (!fs.existsSync(assetsDir)) return;

      const wasmFiles = fs.readdirSync(assetsDir).filter((name) => DUCKDB_EH_WASM_PATTERN.test(name));
      if (wasmFiles.length === 0) return;

      const rewrites = wasmFiles.map((filename) => {
        const wasmPath = path.join(assetsDir, filename);
        const compressed = zlib.gzipSync(fs.readFileSync(wasmPath), { level: 9 });
        fs.writeFileSync(`${wasmPath}.bin`, compressed);
        fs.unlinkSync(wasmPath);
        return {
          pattern: new RegExp(`${escapeRegExp(filename)}(?!\\.bin)`, "g"),
          replacement: `${filename}.bin`,
        };
      });

      const candidateFiles = [
        ...collectFiles(assetsDir).filter((filePath) => TEXT_ASSET_PATTERN.test(filePath)),
        ...fs
          .readdirSync(outDir, { withFileTypes: true })
          .filter((entry) => entry.isFile() && TEXT_ASSET_PATTERN.test(entry.name))
          .map((entry) => path.join(outDir, entry.name)),
      ];

      for (const filePath of candidateFiles) {
        const original = fs.readFileSync(filePath, "utf8");
        let updated = original;
        for (const { pattern, replacement } of rewrites) {
          updated = updated.replace(pattern, replacement);
        }
        if (updated !== original) {
          fs.writeFileSync(filePath, updated, "utf8");
        }
      }
    },
  };
}

const consoleDir = path.dirname(fileURLToPath(import.meta.url));
const protoCandidates = [
  path.resolve(consoleDir, "../deploy/cloudflare/src/native/proto.ts"),
  path.resolve(consoleDir, "../src/native/proto.ts"),
];
const anyserveProtoPath = protoCandidates.find((p) => fs.existsSync(p)) ?? protoCandidates[0]!;

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const target = env.ANYSERVE_CONSOLE_PROXY_TARGET || "http://127.0.0.1:3001";
  return {
    plugins: process.env.ANYSERVE_BUNDLE_GZIP_WASM === "0" ? [react()] : [react(), gzipDuckdbWasmPlugin()],
    resolve: {
      alias: {
        "@anyserve/proto": anyserveProtoPath,
      },
    },
    worker: {
      format: "es",
    },
    server: {
      proxy: {
        "/api": target,
        "/healthz": target,
        "/rpc": target,
        "/ws": { target, ws: true },
      },
    },
  };
});
