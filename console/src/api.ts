import type {
  AnalyticsResponse,
  AnalyticsSource,
  CancelJobResponse,
  InfoResponse,
  JobDetailResponse,
  JobsResponse,
  OverviewResponse,
  WorkersResponse,
} from "./types";

const apiBase = (import.meta.env.VITE_CONSOLE_API_BASE_URL ?? "").replace(/\/$/, "");
const tokenKey = "anyserve.console.token";

export class AuthenticationError extends Error {}
export class AuthorizationError extends Error {}

export function safeHttpUrl(raw?: string | null): string | null {
  const trimmed = raw?.trim();
  if (!trimmed) return null;
  try {
    const base = typeof window !== "undefined" ? window.location.origin : "http://localhost";
    const url = new URL(trimmed, base);
    return url.protocol === "https:" || url.protocol === "http:" ? url.href : null;
  } catch {
    return null;
  }
}

export function setConsoleToken(token: string): void {
  sessionStorage.setItem(tokenKey, token.trim());
}

export function getConsoleToken(): string | null {
  try {
    return sessionStorage.getItem(tokenKey);
  } catch {
    return null;
  }
}

export function clearConsoleToken(): void {
  try {
    sessionStorage.removeItem(tokenKey);
  } catch {
    // Ignore restricted storage environments.
  }
}

export function hasConsoleToken(): boolean {
  const token = getConsoleToken();
  return Boolean(token && token.trim().length > 0);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = getConsoleToken();
  const response = await fetch(apiBase ? `${apiBase}${path}` : path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const payload = (await response.json()) as { error?: string | { message?: string } };
      if (typeof payload.error === "string") message = payload.error;
      else if (payload.error?.message) message = payload.error.message;
    } catch {
      // Ignore non-JSON error bodies.
    }
    if (response.status === 401) throw new AuthenticationError(message);
    if (response.status === 403) throw new AuthorizationError(message);
    throw new Error(message);
  }

  return response.status === 204 ? (undefined as T) : ((await response.json()) as T);
}

function withNamespace(path: string, namespace?: string): string {
  const ns = namespace?.trim();
  return ns && ns !== "default" ? `${path}?namespace=${encodeURIComponent(ns)}` : path;
}

export function getOverview(signal?: AbortSignal): Promise<OverviewResponse> {
  return request("/api/overview", { signal });
}

export interface GuestStatusResponse {
  guest_enabled: boolean;
  guest_ttl_secs?: number;
}

export interface GuestSandboxResponse {
  namespace: string;
  client_token: string;
  worker_token: string;
  expires_at: number;
  ttl_secs: number;
}

function sameOriginAuthPath(authPortalUrl: string | null | undefined, path: string): string | null {
  const safe = safeHttpUrl(authPortalUrl);
  if (!safe || typeof window === "undefined") return null;
  try {
    const portal = new URL(safe);
    if (portal.origin !== window.location.origin) return null;
    return `${portal.origin}${path}`;
  } catch {
    return null;
  }
}

export async function fetchGuestStatus(
  authPortalUrl: string | null | undefined,
  signal?: AbortSignal,
): Promise<GuestStatusResponse> {
  const url = sameOriginAuthPath(authPortalUrl, "/auth/api/me");
  if (!url) return { guest_enabled: false };
  try {
    const resp = await fetch(url, {
      signal,
      headers: { Accept: "application/json" },
    });
    if (!resp.ok) return { guest_enabled: false };
    const body = (await resp.json()) as { guest_enabled?: boolean; guest_ttl_secs?: number };
    return {
      guest_enabled: Boolean(body.guest_enabled),
      guest_ttl_secs: typeof body.guest_ttl_secs === "number" ? body.guest_ttl_secs : undefined,
    };
  } catch {
    return { guest_enabled: false };
  }
}

export async function createGuestSandbox(authPortalUrl: string | null | undefined): Promise<GuestSandboxResponse> {
  const url = sameOriginAuthPath(authPortalUrl, "/auth/api/guest");
  if (!url) throw new Error("Guest sandbox is only available when the auth portal shares the Console origin.");
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "X-Requested-With": "fetch",
    },
  });
  const body = (await resp.json().catch(() => ({}))) as Partial<GuestSandboxResponse> & { error?: string };
  if (!resp.ok) {
    throw new Error(body.error || `${resp.status} ${resp.statusText}`);
  }
  if (!body.client_token || !body.worker_token || !body.namespace) {
    throw new Error("Invalid guest sandbox response from auth portal");
  }
  return body as GuestSandboxResponse;
}

export function fetchInfo(signal?: AbortSignal): Promise<InfoResponse> {
  return request("/api/info", { signal });
}

export interface JobsQueryFilters {
  interface?: string;
  namespace?: string;
  worker?: string;
  q?: string;
  windowMs?: number;
  fromMs?: number;
  toMs?: number;
  buckets?: number;
}

export function getJobs(
  states: string[],
  limit = 50,
  offset: number | string = 0,
  signal?: AbortSignal,
  filters?: JobsQueryFilters,
): Promise<JobsResponse> {
  const params = new URLSearchParams();
  if (states.length > 0) params.set("states", states.join(","));
  params.set("limit", String(limit));
  params.set(typeof offset === "string" ? "page_token" : "offset", String(offset));
  const iface = filters?.interface?.trim();
  if (iface && iface !== "all") params.set("interface", iface);
  const ns = filters?.namespace?.trim();
  if (ns && ns !== "all") params.set("namespace", ns);
  const worker = filters?.worker?.trim();
  if (worker && worker !== "all") params.set("worker", worker);
  const q = filters?.q?.trim();
  if (q) params.set("q", q);
  if (filters?.fromMs != null) params.set("from_ms", String(filters.fromMs));
  if (filters?.toMs != null) params.set("to_ms", String(filters.toMs));
  if (filters?.windowMs != null && filters.fromMs == null && filters.toMs == null) {
    params.set("window_ms", String(filters.windowMs));
  }
  if (filters?.buckets != null && filters.buckets > 0) params.set("buckets", String(filters.buckets));
  return request(`/api/jobs?${params}`, { signal });
}

export function getAnalyticsJobs(
  source: AnalyticsSource,
  fromMs: number,
  toMs: number,
  limit: number,
  cursor: string | null,
  signal: AbortSignal,
  namespace?: string,
): Promise<AnalyticsResponse> {
  const params = new URLSearchParams({ source, from_ms: String(fromMs), to_ms: String(toMs), limit: String(limit) });
  if (cursor) params.set("cursor", cursor);
  const ns = namespace?.trim();
  if (ns) params.set("namespace", ns);
  return request(`/api/analytics/jobs?${params}`, { signal });
}

export function getJob(jobId: string, signal?: AbortSignal, namespace?: string): Promise<JobDetailResponse> {
  return request(withNamespace(`/api/jobs/${encodeURIComponent(jobId)}`, namespace), { signal });
}

export function cancelJob(jobId: string, namespace?: string): Promise<CancelJobResponse> {
  return request(withNamespace(`/api/jobs/${encodeURIComponent(jobId)}/cancel`, namespace), { method: "POST" });
}

export function getWorkers(signal?: AbortSignal): Promise<WorkersResponse> {
  return request("/api/workers", { signal });
}

export function drainWorker(sessionId: string): Promise<{ session_id: string; draining: boolean }> {
  return request(`/api/workers/${encodeURIComponent(sessionId)}/drain`, { method: "POST" });
}
