// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SettingsPane } from "./SettingsPane";
import type { OverviewResponse, SettingsResponse } from "./types";

const BASE_SETTINGS: SettingsResponse = {
  editable: true,
  persisted: true,
  storage_path: "/data/runtime-settings.json",
  modified: false,
  limits: { max_attempts_limit: 20, priority_levels: 4 },
  server_toml: "[runtime]\ndefault_max_attempts = 3\n",
  settings: {
    default_max_attempts: 3,
    default_max_queue: 500,
    default_max_in_flight_per_key: null,
    max_pending_jobs: 2000,
    max_terminal_jobs: 1000,
    terminal_retention_secs: 86400,
    max_sessions: 4096,
    heartbeat_interval_ms: 5000,
    heartbeat_timeout_ms: 15000,
    reconnect_grace_ms: null,
    default_initial_backoff_ms: 200,
    default_max_backoff_ms: 10000,
    consumer_stall_timeout_ms: 30000,
    cancel_grace_ms: 15000,
    interfaces: {},
  },
  defaults: {
    default_max_attempts: 3,
    default_max_queue: 500,
    default_max_in_flight_per_key: null,
    max_pending_jobs: 2000,
    max_terminal_jobs: 1000,
    terminal_retention_secs: 86400,
    max_sessions: 4096,
    heartbeat_interval_ms: 5000,
    heartbeat_timeout_ms: 15000,
    reconnect_grace_ms: null,
    default_initial_backoff_ms: 200,
    default_max_backoff_ms: 10000,
    consumer_stall_timeout_ms: 30000,
    cancel_grace_ms: 15000,
    interfaces: {},
  },
};

const OPERATOR_OVERVIEW: OverviewResponse = {
  runtime: { code: "memory_plus_sqlite", label: "Memory + SQLite", journal: "sqlite" },
  queue: { queued: 0, running: 0, active: 0 },
  history: { succeeded: 0, failed: 0, cancelled: 0, total: 0 },
  workers: { total: 0, healthy: 0, unavailable: 0, active_leases: 0, total_capacity: {}, last_seen_at_ms: null },
  interfaces: [],
  refreshed_at_ms: Date.now(),
  control_plane: {
    grpc_enabled: true,
    grpc_listen: "0.0.0.0:50052",
    grpc_tls_enabled: false,
    console_listen: "0.0.0.0:3001",
    public_endpoint: "https://anyserve.arcosx.org",
    auth_portal_url: "https://anyserve.arcosx.org/tokens",
    auth_enabled: true,
    generation: 2,
    retained_jobs: 0,
    sessions: 0,
    max_sessions: 4096,
    priority_levels: 4,
    default_max_attempts: 3,
    heartbeat_interval_ms: 5000,
    heartbeat_timeout_ms: 15000,
    reconnect_grace_ms: null,
  },
  scope: { is_operator: true, namespace: "default", subject: "operator" },
};

describe("SettingsPane", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("loads operator runtime settings, edits and saves live changes, and shows project links", async () => {
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url === "/api/settings" && (!init?.method || init.method === "GET")) {
        return new Response(JSON.stringify(BASE_SETTINGS), { status: 200 });
      }
      if (url === "/api/settings" && init?.method === "PUT") {
        const sent = JSON.parse(String(init.body));
        expect(sent.default_max_attempts).toBe(5);
        expect(sent.interfaces["demo.fast.v1"]).toBeTruthy();
        return new Response(
          JSON.stringify({
            ...BASE_SETTINGS,
            modified: true,
            settings: sent,
            server_toml: "[runtime]\ndefault_max_attempts = 5\n",
          }),
          { status: 200 },
        );
      }
      throw new Error(`Unexpected fetch ${url}`);
    });

    const onApplied = vi.fn();
    render(
      <SettingsPane
        overview={OPERATOR_OVERVIEW}
        authPortalUrl="https://anyserve.arcosx.org/tokens"
        themeMode="system"
        onThemeModeChange={vi.fn()}
        pollPreset="live"
        onPollPresetChange={vi.fn()}
        authenticated
        onDisconnect={vi.fn()}
        onOpenConnect={vi.fn()}
        onSettingsApplied={onApplied}
        onAuthenticationError={vi.fn()}
      />,
    );

    const attemptsInput = await screen.findByLabelText("Default max attempts");
    expect((attemptsInput as HTMLInputElement).value).toBe("3");

    fireEvent.change(attemptsInput, { target: { value: "5" } });
    fireEvent.change(screen.getByLabelText("New interface name"), { target: { value: "demo.fast.v1" } });
    fireEvent.click(screen.getByRole("button", { name: /Add policy/i }));

    fireEvent.click(screen.getByRole("button", { name: /Save changes/i }));

    await waitFor(() => expect(onApplied).toHaveBeenCalledTimes(1));
    expect(screen.getByRole("status").textContent).toContain("Saved and persisted");
    expect(screen.getByRole("link", { name: /Docs/i }).getAttribute("href")).toBe(
      "https://anyserve.github.io/anyserve/",
    );
    expect(screen.getByRole("link", { name: /GitHub/i }).getAttribute("href")).toBe(
      "https://github.com/anyserve/anyserve",
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("hides operator server controls for scoped tenants while keeping appearance and project links", () => {
    const fetchMock = vi.spyOn(globalThis, "fetch");
    const onTheme = vi.fn();
    render(
      <SettingsPane
        overview={{
          ...OPERATOR_OVERVIEW,
          scope: { is_operator: false, namespace: "gh-99", subject: "bob" },
        }}
        themeMode="system"
        onThemeModeChange={onTheme}
        pollPreset="live"
        onPollPresetChange={vi.fn()}
        authenticated
        onDisconnect={vi.fn()}
        onOpenConnect={vi.fn()}
        onSettingsApplied={vi.fn()}
        onAuthenticationError={vi.fn()}
      />,
    );

    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByText(/restricted to operator tokens/i)).toBeTruthy();
    fireEvent.click(screen.getByRole("radio", { name: "Dark" }));
    expect(onTheme).toHaveBeenCalledWith("dark");
  });
});
