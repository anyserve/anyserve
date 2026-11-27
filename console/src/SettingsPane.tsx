import { useCallback, useEffect, useMemo, useState } from "react";

import {
  AuthenticationError,
  AuthorizationError,
  getSettings,
  resetSettings,
  safeHttpUrl,
  updateSettings,
} from "./api";
import {
  PROJECT_DOCS_URL,
  PROJECT_GITHUB_URL,
  PROJECT_INSTALL_PS1_URL,
  PROJECT_INSTALL_SH_URL,
  PROJECT_RELEASES_URL,
  type PollPreset,
  type ThemeMode,
} from "./app-constants";
import { deriveEndpoint } from "./ConnectWorkerModal";
import { asErrorMessage } from "./format";
import {
  IconBook,
  IconExternalLink,
  IconGitHub,
  IconKey,
  IconPlus,
  IconRotateCcw,
  IconTrash,
} from "./icons";
import { CopyButton, PanelHeader } from "./pane-tables";
import type {
  InterfacePolicySettings,
  OverviewResponse,
  PoolTierSettings,
  RuntimeSettings,
  SettingsResponse,
} from "./types";

interface SettingsPaneProps {
  overview: OverviewResponse | null;
  authPortalUrl?: string | null;
  themeMode: ThemeMode;
  onThemeModeChange: (mode: ThemeMode) => void;
  pollPreset: PollPreset;
  onPollPresetChange: (preset: PollPreset) => void;
  authenticated: boolean;
  onDisconnect: () => void;
  onOpenConnect: () => void;
  onSettingsApplied: () => void;
  onAuthenticationError: (err: unknown) => void;
}

const POLL_OPTIONS: Array<{ id: PollPreset; label: string }> = [
  { id: "live", label: "Live" },
  { id: "2s", label: "2s" },
  { id: "5s", label: "5s" },
  { id: "10s", label: "10s" },
  { id: "paused", label: "Paused" },
];

const THEME_OPTIONS: Array<{ id: ThemeMode; label: string }> = [
  { id: "system", label: "System" },
  { id: "light", label: "Light" },
  { id: "dark", label: "Dark" },
];

function parsePositiveInt(raw: string, fallback: number, allowZero = false): number {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  const rounded = Math.trunc(n);
  return allowZero ? Math.max(0, rounded) : Math.max(1, rounded);
}

function parseOptionalInt(raw: string, allowZero = false): number | null {
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const n = Number(trimmed);
  if (!Number.isFinite(n)) return null;
  const rounded = Math.trunc(n);
  return allowZero ? Math.max(0, rounded) : Math.max(1, rounded);
}

function cloneSettings(s: RuntimeSettings): RuntimeSettings {
  return JSON.parse(JSON.stringify(s)) as RuntimeSettings;
}

function settingsEqual(a: RuntimeSettings | null, b: RuntimeSettings | null): boolean {
  if (!a || !b) return false;
  return JSON.stringify(a) === JSON.stringify(b);
}

function SettingRow({
  id,
  label,
  hint,
  children,
}: {
  id?: string;
  label: string;
  hint: string;
  children: React.ReactNode;
}) {
  return (
    <div className="settings-row">
      <div className="settings-row__info">
        {id ? (
          <label className="settings-row__label" htmlFor={id}>
            {label}
          </label>
        ) : (
          <div className="settings-row__label">{label}</div>
        )}
        <div className="settings-row__hint">{hint}</div>
      </div>
      <div className="settings-row__control">{children}</div>
    </div>
  );
}

export function SettingsPane({
  overview,
  authPortalUrl,
  themeMode,
  onThemeModeChange,
  pollPreset,
  onPollPresetChange,
  authenticated,
  onDisconnect,
  onOpenConnect,
  onSettingsApplied,
  onAuthenticationError,
}: SettingsPaneProps) {
  const isOperator = !overview?.scope || overview.scope.is_operator;
  const endpoint = deriveEndpoint(overview?.control_plane);
  const safePortalUrl = safeHttpUrl(authPortalUrl);

  const [serverData, setServerData] = useState<SettingsResponse | null>(null);
  const [draft, setDraft] = useState<RuntimeSettings | null>(null);
  const [loading, setLoading] = useState(false);
  const [unsupported, setUnsupported] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [savedNotice, setSavedNotice] = useState<string | null>(null);
  const [newIfaceName, setNewIfaceName] = useState("");
  const [ifaceError, setIfaceError] = useState<string | null>(null);

  const loadSettings = useCallback(
    async (signal?: AbortSignal) => {
      if (!isOperator) {
        setServerData(null);
        setDraft(null);
        setUnsupported(false);
        return;
      }
      setLoading(true);
      setError(null);
      try {
        const res = await getSettings(signal);
        if (signal?.aborted) return;
        setServerData(res);
        setDraft(cloneSettings(res.settings));
        setUnsupported(false);
      } catch (err) {
        if (signal?.aborted) return;
        if (err instanceof AuthenticationError) {
          onAuthenticationError(err);
          return;
        }
        if (err instanceof AuthorizationError) {
          setServerData(null);
          setDraft(null);
          return;
        }
        const msg = asErrorMessage(err);
        if (/404|Not Found/i.test(msg)) {
          setUnsupported(true);
          setError(null);
        } else {
          setError(msg);
        }
      } finally {
        if (!signal?.aborted) setLoading(false);
      }
    },
    [isOperator, onAuthenticationError],
  );

  useEffect(() => {
    const ctrl = new AbortController();
    void loadSettings(ctrl.signal);
    return () => ctrl.abort();
  }, [loadSettings]);

  const isDirty = useMemo(
    () => Boolean(draft && serverData && !settingsEqual(draft, serverData.settings)),
    [draft, serverData],
  );

  const patchDraft = (updater: (cur: RuntimeSettings) => RuntimeSettings) => {
    setSavedNotice(null);
    setError(null);
    setDraft((cur) => (cur ? updater(cur) : cur));
  };

  const handleSave = async () => {
    if (!draft || saving) return;
    setSaving(true);
    setError(null);
    setSavedNotice(null);
    try {
      const res = await updateSettings(draft);
      setServerData(res);
      setDraft(cloneSettings(res.settings));
      setSavedNotice(res.persisted ? "Saved and persisted to disk." : "Applied live in memory.");
      onSettingsApplied();
    } catch (err) {
      if (err instanceof AuthenticationError) {
        onAuthenticationError(err);
        return;
      }
      setError(asErrorMessage(err));
    } finally {
      setSaving(false);
    }
  };

  const handleReset = async () => {
    if (saving) return;
    setSaving(true);
    setError(null);
    setSavedNotice(null);
    try {
      const res = await resetSettings();
      setServerData(res);
      setDraft(cloneSettings(res.settings));
      setSavedNotice("Reset to server.toml startup defaults.");
      onSettingsApplied();
    } catch (err) {
      if (err instanceof AuthenticationError) {
        onAuthenticationError(err);
        return;
      }
      setError(asErrorMessage(err));
    } finally {
      setSaving(false);
    }
  };

  const handleAddInterface = () => {
    const trimmed = newIfaceName.trim();
    if (!trimmed) {
      setIfaceError("Enter an interface name such as demo.echo.v1.");
      return;
    }
    if (/\s/.test(trimmed) || trimmed.length > 64) {
      setIfaceError("Interface name must be 1–64 bytes with no whitespace.");
      return;
    }
    if (draft?.interfaces[trimmed]) {
      setIfaceError(`Interface '${trimmed}' already has a policy override.`);
      return;
    }
    setIfaceError(null);
    setNewIfaceName("");
    patchDraft((cur) => ({
      ...cur,
      interfaces: {
        ...cur.interfaces,
        [trimmed]: {
          max_queue: null,
          queue_timeout_ms: null,
          max_in_flight_per_key: null,
          pool_tiers: [],
        },
      },
    }));
  };

  const updateInterfacePolicy = (iface: string, updater: (p: InterfacePolicySettings) => InterfacePolicySettings) => {
    patchDraft((cur) => {
      const existing = cur.interfaces[iface];
      if (!existing) return cur;
      return {
        ...cur,
        interfaces: {
          ...cur.interfaces,
          [iface]: updater(existing),
        },
      };
    });
  };

  const removeInterface = (iface: string) => {
    patchDraft((cur) => {
      const next = { ...cur.interfaces };
      delete next[iface];
      return { ...cur, interfaces: next };
    });
  };

  const ifaceNames = useMemo(() => Object.keys(draft?.interfaces ?? {}).sort(), [draft]);

  return (
    <div className="settings-page">
      <section className="panel">
        <PanelHeader
          title="Settings"
          meta={
            isOperator
              ? serverData?.persisted
                ? "Live · Persisted"
                : serverData
                  ? "Live · In-memory"
                  : unsupported
                    ? "Console preferences"
                    : "Operator"
              : "Tenant preferences"
          }
          actions={
            draft && serverData ? (
              <div className="settings-header-actions">
                {serverData.modified ? (
                  <button
                    type="button"
                    className="button button--sm"
                    disabled={saving}
                    onClick={() => void handleReset()}
                    title="Restore startup server.toml defaults"
                  >
                    <IconRotateCcw />
                    <span>Reset defaults</span>
                  </button>
                ) : null}
                {isDirty ? (
                  <button
                    type="button"
                    className="button button--sm"
                    disabled={saving}
                    onClick={() => {
                      setDraft(cloneSettings(serverData.settings));
                      setError(null);
                    }}
                  >
                    Discard
                  </button>
                ) : null}
                <button
                  type="button"
                  className="button button--primary button--sm"
                  disabled={!isDirty || saving}
                  onClick={() => void handleSave()}
                >
                  {saving ? "Saving…" : "Save changes"}
                </button>
              </div>
            ) : null
          }
        />

        <div className="settings-body">
          {error ? (
            <div className="settings-alert settings-alert--error" role="alert">
              {error}
            </div>
          ) : null}
          {savedNotice ? (
            <div className="settings-alert settings-alert--ok" role="status">
              {savedNotice}
            </div>
          ) : null}

          {/* 1. Appearance & Console Preferences */}
          <div className="settings-section">
            <div className="settings-section__head">
              <h2>Appearance &amp; Console</h2>
              <p>Saved locally in this browser. Applies immediately without reconnecting.</p>
            </div>
            <div className="settings-card">
              <SettingRow
                label="Theme"
                hint="Switch between system preference, light, or dark appearance."
              >
                <div className="seg-control" role="radiogroup" aria-label="Theme mode">
                  {THEME_OPTIONS.map((opt) => (
                    <button
                      key={opt.id}
                      type="button"
                      role="radio"
                      aria-checked={themeMode === opt.id}
                      className={`seg-control__btn${themeMode === opt.id ? " is-active" : ""}`}
                      onClick={() => onThemeModeChange(opt.id)}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </SettingRow>

              <SettingRow
                label="Auto-refresh cadence"
                hint="How often Overview, Jobs, and Workers poll the control plane."
              >
                <div className="seg-control" role="radiogroup" aria-label="Auto-refresh cadence">
                  {POLL_OPTIONS.map((opt) => (
                    <button
                      key={opt.id}
                      type="button"
                      role="radio"
                      aria-checked={pollPreset === opt.id}
                      className={`seg-control__btn${pollPreset === opt.id ? " is-active" : ""}`}
                      onClick={() => onPollPresetChange(opt.id)}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
              </SettingRow>
            </div>
          </div>

          {/* 2. Runtime & Scheduling (Operator live settings) */}
          <div className="settings-section">
            <div className="settings-section__head">
              <h2>Runtime &amp; Scheduling</h2>
              <p>
                {!isOperator
                  ? "Server-wide runtime configuration requires an operator token."
                  : unsupported
                    ? "This control-plane backend does not expose live runtime settings; configure via environment or wrangler.jsonc."
                    : serverData?.persisted && serverData.storage_path
                      ? `Changes apply immediately and persist to ${serverData.storage_path}.`
                      : "Changes apply immediately in memory until the server restarts."}
              </p>
            </div>

            {!isOperator ? (
              <div className="settings-card settings-card--note">
                Signed in as scoped tenant <code className="mono">{overview?.scope?.namespace}</code>. Global scheduling
                limits, timeouts, and interface policies are restricted to operator tokens.
              </div>
            ) : loading && !draft ? (
              <div className="settings-card settings-card--note">Loading runtime settings…</div>
            ) : draft ? (
              <>
                <div className="settings-card">
                  <SettingRow
                    id="set-max-attempts"
                    label="Default max attempts"
                    hint={`Attempts per job when not set by caller (1–${serverData?.limits.max_attempts_limit ?? 20}).`}
                  >
                    <input
                      id="set-max-attempts"
                      type="number"
                      min={1}
                      max={serverData?.limits.max_attempts_limit ?? 20}
                      className="settings-input"
                      value={draft.default_max_attempts}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          default_max_attempts: parsePositiveInt(e.target.value, c.default_max_attempts),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-max-queue"
                    label="Default max queue"
                    hint="Maximum queued jobs per interface unless overridden below."
                  >
                    <input
                      id="set-max-queue"
                      type="number"
                      min={1}
                      max={1_000_000}
                      className="settings-input"
                      value={draft.default_max_queue}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          default_max_queue: parsePositiveInt(e.target.value, c.default_max_queue),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-max-in-flight"
                    label="Default max in-flight / key"
                    hint="Concurrency cap per concurrency_key (leave blank for 1 when key is set)."
                  >
                    <input
                      id="set-max-in-flight"
                      type="number"
                      min={1}
                      max={1_000_000}
                      placeholder="Unset"
                      className="settings-input"
                      value={draft.default_max_in_flight_per_key ?? ""}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          default_max_in_flight_per_key: parseOptionalInt(e.target.value),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-max-pending"
                    label="Max pending jobs"
                    hint="Total unfinished jobs admitted across all interfaces."
                  >
                    <input
                      id="set-max-pending"
                      type="number"
                      min={1}
                      max={1_000_000}
                      className="settings-input"
                      value={draft.max_pending_jobs}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          max_pending_jobs: parsePositiveInt(e.target.value, c.max_pending_jobs),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-max-sessions"
                    label="Max worker sessions"
                    hint="Maximum concurrent connected or grace-period worker sessions."
                  >
                    <input
                      id="set-max-sessions"
                      type="number"
                      min={1}
                      max={100_000}
                      className="settings-input"
                      value={draft.max_sessions}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          max_sessions: parsePositiveInt(e.target.value, c.max_sessions),
                        }))
                      }
                    />
                  </SettingRow>
                </div>

                <div className="settings-card">
                  <SettingRow
                    id="set-max-terminal"
                    label="Max retained terminal jobs"
                    hint="Completed jobs kept in memory for Overview / History (durable jobs remain in journal)."
                  >
                    <input
                      id="set-max-terminal"
                      type="number"
                      min={0}
                      max={1_000_000}
                      className="settings-input"
                      value={draft.max_terminal_jobs}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          max_terminal_jobs: parsePositiveInt(e.target.value, c.max_terminal_jobs, true),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-terminal-retention"
                    label="Terminal retention (seconds)"
                    hint="How long completed jobs stay in memory and survive restarts in Overview / History."
                  >
                    <input
                      id="set-terminal-retention"
                      type="number"
                      min={0}
                      max={31_536_000}
                      className="settings-input"
                      value={draft.terminal_retention_secs}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          terminal_retention_secs: parsePositiveInt(
                            e.target.value,
                            c.terminal_retention_secs,
                            true,
                          ),
                        }))
                      }
                    />
                  </SettingRow>
                </div>

                <div className="settings-card">
                  <SettingRow
                    id="set-hb-interval"
                    label="Heartbeat interval (ms)"
                    hint="Interval advertised to newly connected workers (500–300000 ms)."
                  >
                    <input
                      id="set-hb-interval"
                      type="number"
                      min={500}
                      max={300_000}
                      step={500}
                      className="settings-input"
                      value={draft.heartbeat_interval_ms}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          heartbeat_interval_ms: parsePositiveInt(e.target.value, c.heartbeat_interval_ms),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-hb-timeout"
                    label="Heartbeat timeout (ms)"
                    hint="Silent worker disconnect timeout (must exceed heartbeat interval)."
                  >
                    <input
                      id="set-hb-timeout"
                      type="number"
                      min={1000}
                      max={900_000}
                      step={1000}
                      className="settings-input"
                      value={draft.heartbeat_timeout_ms}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          heartbeat_timeout_ms: parsePositiveInt(e.target.value, c.heartbeat_timeout_ms),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-reconnect-grace"
                    label="Reconnect grace (ms)"
                    hint="Wait before rescheduling leases after worker disconnect (blank = heartbeat timeout)."
                  >
                    <input
                      id="set-reconnect-grace"
                      type="number"
                      min={100}
                      max={900_000}
                      step={100}
                      placeholder="Inherit timeout"
                      className="settings-input"
                      value={draft.reconnect_grace_ms ?? ""}
                      onChange={(e) =>
                        patchDraft((c) => ({
                          ...c,
                          reconnect_grace_ms: parseOptionalInt(e.target.value),
                        }))
                      }
                    />
                  </SettingRow>

                  <SettingRow
                    id="set-backoff-init"
                    label="Initial / Max retry backoff (ms)"
                    hint="Exponential backoff bounds between retryable attempt failures."
                  >
                    <div className="settings-pair">
                      <input
                        id="set-backoff-init"
                        aria-label="Initial retry backoff ms"
                        type="number"
                        min={10}
                        max={60_000}
                        className="settings-input settings-input--sm"
                        value={draft.default_initial_backoff_ms}
                        onChange={(e) =>
                          patchDraft((c) => ({
                            ...c,
                            default_initial_backoff_ms: parsePositiveInt(
                              e.target.value,
                              c.default_initial_backoff_ms,
                            ),
                          }))
                        }
                      />
                      <span className="muted">/</span>
                      <input
                        aria-label="Max retry backoff ms"
                        type="number"
                        min={10}
                        max={600_000}
                        className="settings-input settings-input--sm"
                        value={draft.default_max_backoff_ms}
                        onChange={(e) =>
                          patchDraft((c) => ({
                            ...c,
                            default_max_backoff_ms: parsePositiveInt(e.target.value, c.default_max_backoff_ms),
                          }))
                        }
                      />
                    </div>
                  </SettingRow>

                  <SettingRow
                    id="set-stall-timeout"
                    label="Consumer stall / Cancel grace (ms)"
                    hint="Timeouts for stalled Invoke readers and worker cancel acknowledgements."
                  >
                    <div className="settings-pair">
                      <input
                        id="set-stall-timeout"
                        aria-label="Consumer stall timeout ms"
                        type="number"
                        min={1000}
                        max={600_000}
                        step={1000}
                        className="settings-input settings-input--sm"
                        value={draft.consumer_stall_timeout_ms}
                        onChange={(e) =>
                          patchDraft((c) => ({
                            ...c,
                            consumer_stall_timeout_ms: parsePositiveInt(
                              e.target.value,
                              c.consumer_stall_timeout_ms,
                            ),
                          }))
                        }
                      />
                      <span className="muted">/</span>
                      <input
                        aria-label="Cancel grace ms"
                        type="number"
                        min={1000}
                        max={300_000}
                        step={1000}
                        className="settings-input settings-input--sm"
                        value={draft.cancel_grace_ms}
                        onChange={(e) =>
                          patchDraft((c) => ({
                            ...c,
                            cancel_grace_ms: parsePositiveInt(e.target.value, c.cancel_grace_ms),
                          }))
                        }
                      />
                    </div>
                  </SettingRow>
                </div>

                {/* 3. Interface Policies */}
                <div className="settings-section">
                  <div className="settings-section__head">
                    <h2>Interface Routing Policies</h2>
                    <p>Per-interface queue caps, timeouts, and pool tier spillover or fast failover grace.</p>
                  </div>

                  <div className="settings-card">
                    <div className="settings-add-row">
                      <input
                        type="text"
                        className="settings-input settings-input--grow mono"
                        placeholder="Interface name (e.g. demo.local_model.v1)"
                        aria-label="New interface name"
                        value={newIfaceName}
                        onChange={(e) => {
                          setNewIfaceName(e.target.value);
                          setIfaceError(null);
                        }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") {
                            e.preventDefault();
                            handleAddInterface();
                          }
                        }}
                      />
                      <button type="button" className="button button--sm" onClick={handleAddInterface}>
                        <IconPlus />
                        <span>Add policy</span>
                      </button>
                    </div>
                    {ifaceError ? <div className="settings-inline-err">{ifaceError}</div> : null}

                    {ifaceNames.length === 0 ? (
                      <div className="settings-empty-inline">
                        No interface policy overrides configured. All interfaces use the global defaults above.
                      </div>
                    ) : (
                      ifaceNames.map((iface) => {
                        const policy = draft.interfaces[iface]!;
                        return (
                          <div key={iface} className="settings-iface">
                            <div className="settings-iface__head">
                              <span className="mono settings-iface__name">{iface}</span>
                              <button
                                type="button"
                                className="button button--sm button--danger"
                                aria-label={`Remove policy for ${iface}`}
                                onClick={() => removeInterface(iface)}
                              >
                                <IconTrash />
                                <span>Remove</span>
                              </button>
                            </div>
                            <div className="settings-iface__grid">
                              <label className="settings-mini-field">
                                <span>Max queue</span>
                                <input
                                  type="number"
                                  min={1}
                                  placeholder="Default"
                                  className="settings-input settings-input--sm"
                                  value={policy.max_queue ?? ""}
                                  onChange={(e) =>
                                    updateInterfacePolicy(iface, (p) => ({
                                      ...p,
                                      max_queue: parseOptionalInt(e.target.value),
                                    }))
                                  }
                                />
                              </label>
                              <label className="settings-mini-field">
                                <span>Queue timeout (ms)</span>
                                <input
                                  type="number"
                                  min={1}
                                  placeholder="None"
                                  className="settings-input settings-input--sm"
                                  value={policy.queue_timeout_ms ?? ""}
                                  onChange={(e) =>
                                    updateInterfacePolicy(iface, (p) => ({
                                      ...p,
                                      queue_timeout_ms: parseOptionalInt(e.target.value),
                                    }))
                                  }
                                />
                              </label>
                              <label className="settings-mini-field">
                                <span>Max in-flight / key</span>
                                <input
                                  type="number"
                                  min={1}
                                  placeholder="Default"
                                  className="settings-input settings-input--sm"
                                  value={policy.max_in_flight_per_key ?? ""}
                                  onChange={(e) =>
                                    updateInterfacePolicy(iface, (p) => ({
                                      ...p,
                                      max_in_flight_per_key: parseOptionalInt(e.target.value),
                                    }))
                                  }
                                />
                              </label>
                            </div>

                            <div className="settings-tiers">
                              <div className="settings-tiers__head">
                                <span>Pool tiers</span>
                                <button
                                  type="button"
                                  className="button button--sm"
                                  onClick={() =>
                                    updateInterfacePolicy(iface, (p) => ({
                                      ...p,
                                      pool_tiers: [
                                        ...p.pool_tiers,
                                        {
                                          pool: p.pool_tiers.length === 0 ? "default" : "",
                                          spillover_after_ms: 0,
                                          reconnect_grace_ms: null,
                                        },
                                      ],
                                    }))
                                  }
                                >
                                  <IconPlus />
                                  <span>Add tier</span>
                                </button>
                              </div>
                              {policy.pool_tiers.map((tier: PoolTierSettings, idx: number) => (
                                <div key={`${iface}-tier-${idx}`} className="settings-tier-row">
                                  <input
                                    type="text"
                                    placeholder='Pool ("" = default)'
                                    aria-label={`${iface} tier ${idx + 1} pool`}
                                    className="settings-input settings-input--sm mono"
                                    value={tier.pool}
                                    onChange={(e) =>
                                      updateInterfacePolicy(iface, (p) => ({
                                        ...p,
                                        pool_tiers: p.pool_tiers.map((t, i) =>
                                          i === idx ? { ...t, pool: e.target.value } : t,
                                        ),
                                      }))
                                    }
                                  />
                                  <input
                                    type="number"
                                    min={0}
                                    placeholder="Spillover ms"
                                    aria-label={`${iface} tier ${idx + 1} spillover ms`}
                                    className="settings-input settings-input--sm"
                                    value={tier.spillover_after_ms}
                                    onChange={(e) =>
                                      updateInterfacePolicy(iface, (p) => ({
                                        ...p,
                                        pool_tiers: p.pool_tiers.map((t, i) =>
                                          i === idx
                                            ? {
                                                ...t,
                                                spillover_after_ms: parsePositiveInt(
                                                  e.target.value,
                                                  t.spillover_after_ms,
                                                  true,
                                                ),
                                              }
                                            : t,
                                        ),
                                      }))
                                    }
                                  />
                                  <input
                                    type="number"
                                    min={100}
                                    placeholder="Grace ms"
                                    aria-label={`${iface} tier ${idx + 1} reconnect grace ms`}
                                    className="settings-input settings-input--sm"
                                    value={tier.reconnect_grace_ms ?? ""}
                                    onChange={(e) =>
                                      updateInterfacePolicy(iface, (p) => ({
                                        ...p,
                                        pool_tiers: p.pool_tiers.map((t, i) =>
                                          i === idx
                                            ? { ...t, reconnect_grace_ms: parseOptionalInt(e.target.value) }
                                            : t,
                                        ),
                                      }))
                                    }
                                  />
                                  <button
                                    type="button"
                                    className="icon-btn"
                                    aria-label={`Delete tier ${idx + 1}`}
                                    onClick={() =>
                                      updateInterfacePolicy(iface, (p) => ({
                                        ...p,
                                        pool_tiers: p.pool_tiers.filter((_, i) => i !== idx),
                                      }))
                                    }
                                  >
                                    <IconTrash />
                                  </button>
                                </div>
                              ))}
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </div>

                {/* 4. Export server.toml */}
                {serverData ? (
                  <div className="settings-section">
                    <div className="settings-section__head">
                      <h2>Effective server.toml</h2>
                      <p>
                        {isDirty
                          ? "Save changes above to refresh the exported TOML block."
                          : "Copy into server.toml to version-control these settings."}
                      </p>
                    </div>
                    <div className="settings-card settings-card--code">
                      <div className="snippet-card__head">
                        <span className="mono">server.toml</span>
                        <CopyButton text={serverData.server_toml} />
                      </div>
                      <pre className="code-block code-block--snippet">{serverData.server_toml}</pre>
                    </div>
                  </div>
                ) : null}
              </>
            ) : null}
          </div>

          {/* 5. Connection & Project */}
          <div className="settings-section">
            <div className="settings-section__head">
              <h2>Connection &amp; Project</h2>
              <p>Control plane endpoint, authentication scope, CLI install, and project links.</p>
            </div>
            <div className="settings-card">
              <SettingRow label="Endpoint" hint="Active gRPC / WebSocket endpoint for workers and clients.">
                <div className="settings-inline-value">
                  <code className="mono">{endpoint}</code>
                  <CopyButton text={endpoint} />
                </div>
              </SettingRow>

              <SettingRow label="Runtime & Scope" hint="Journal backend and active token permissions.">
                <div className="settings-inline-value">
                  <span className="tag">{overview?.runtime.label ?? "Connecting"}</span>
                  <span className="tag mono">
                    {overview?.scope
                      ? overview.scope.is_operator
                        ? "Operator"
                        : `Tenant · ${overview.scope.namespace}`
                      : overview?.control_plane?.auth_enabled
                        ? "Operator"
                        : "Local (auth off)"}
                  </span>
                </div>
              </SettingRow>

              <SettingRow label="Session & Workers" hint="Open worker onboarding snippets, manage tokens, or switch token.">
                <div className="settings-inline-actions">
                  <button type="button" className="button button--sm" onClick={onOpenConnect}>
                    Connect Worker
                  </button>
                  {safePortalUrl ? (
                    <a className="button button--sm" href={safePortalUrl} target="_blank" rel="noopener noreferrer">
                      <IconKey />
                      <span>Token Portal</span>
                    </a>
                  ) : null}
                  {authenticated ? (
                    <button type="button" className="button button--sm button--danger" onClick={onDisconnect}>
                      Disconnect
                    </button>
                  ) : null}
                </div>
              </SettingRow>

              <SettingRow label="Project & Documentation" hint="Official docs, source repository, and release binaries.">
                <div className="settings-inline-actions">
                  <a className="button button--sm" href={PROJECT_DOCS_URL} target="_blank" rel="noopener noreferrer">
                    <IconBook />
                    <span>Docs</span>
                    <IconExternalLink />
                  </a>
                  <a className="button button--sm" href={PROJECT_GITHUB_URL} target="_blank" rel="noopener noreferrer">
                    <IconGitHub />
                    <span>GitHub</span>
                    <IconExternalLink />
                  </a>
                  <a className="button button--sm" href={PROJECT_RELEASES_URL} target="_blank" rel="noopener noreferrer">
                    <span>Releases</span>
                    <IconExternalLink />
                  </a>
                </div>
              </SettingRow>

              <SettingRow label="Quick CLI & SDK Install" hint="Install the anyserve binary or Python runtime SDK.">
                <div className="settings-install-stack">
                  <div className="settings-install-cmd">
                    <code className="mono">curl -fsSL {PROJECT_INSTALL_SH_URL} | sh</code>
                    <CopyButton text={`curl -fsSL ${PROJECT_INSTALL_SH_URL} | sh`} />
                  </div>
                  <div className="settings-install-cmd">
                    <code className="mono">irm {PROJECT_INSTALL_PS1_URL} | iex</code>
                    <CopyButton text={`irm ${PROJECT_INSTALL_PS1_URL} | iex`} />
                  </div>
                  <div className="settings-install-cmd">
                    <code className="mono">pip install anyserve-runtime</code>
                    <CopyButton text="pip install anyserve-runtime" />
                  </div>
                </div>
              </SettingRow>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
