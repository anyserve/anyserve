import type {
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

interface ApiErrorPayload {
  error?: string | { message?: string };
}

function apiUrl(path: string): string {
  return apiBase ? `${apiBase}${path}` : path;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = sessionStorage.getItem(tokenKey);
  const response = await fetch(apiUrl(path), {
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
      const payload = (await response.json()) as ApiErrorPayload;
      if (typeof payload.error === "string") {
        message = payload.error;
      } else if (payload.error?.message) {
        message = payload.error.message;
      }
    } catch {
      // Ignore non-JSON error bodies.
    }
    if (response.status === 401 || response.status === 403) {
      throw new AuthenticationError(message);
    }
    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function getOverview(signal?: AbortSignal): Promise<OverviewResponse> {
  return request("/api/overview", { signal });
}

export function getJobs(states: string[], limit = 50, offset = 0, signal?: AbortSignal): Promise<JobsResponse> {
  const params = new URLSearchParams({
    states: states.join(","),
    limit: String(limit),
    offset: String(offset),
  });
  return request(`/api/jobs?${params.toString()}`, { signal });
}

export function getJob(jobId: string, signal?: AbortSignal): Promise<JobDetailResponse> {
  return request(`/api/jobs/${jobId}`, { signal });
}

export function cancelJob(jobId: string): Promise<CancelJobResponse> {
  return request(`/api/jobs/${jobId}/cancel`, {
    method: "POST",
  });
}

export function getWorkers(signal?: AbortSignal): Promise<WorkersResponse> {
  return request("/api/workers", { signal });
}
