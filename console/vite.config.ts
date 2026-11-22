import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const target = env.ANYSERVE_CONSOLE_PROXY_TARGET || "http://127.0.0.1:3001";
  return {
    plugins: [react()],
    server: {
      proxy: {
        "/api": target,
        "/healthz": target,
      },
    },
  };
});
