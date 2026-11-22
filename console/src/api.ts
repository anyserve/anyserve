import type {
  AnalyticsResponse,
  AnalyticsSource,
  CancelJobResponse,
  JobDetailResponse,
  JobsResponse,
  OverviewResponse,
  WorkersResponse,
} from "./types";

const apiBase = (import.meta.env.VITE_CONSOLE_API_BASE_URL ?? "").replace(/\/$/, "");
const tokenKey = "anyserve.console.token";

export class AuthenticationError extends Error {}

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
    if (response.status === 401 || response.status === 403) throw new AuthenticationError(message);
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
