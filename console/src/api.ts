import type {
  CancelJobResponse,
  JobDetailResponse,
  JobsResponse,
  OverviewResponse,
  WorkersResponse,
} from "./types";

const apiBase = (import.meta.env.VITE_CONSOLE_API_BASE_URL ?? "").replace(/\/$/, "");

interface ApiErrorPayload {
  error?: {
    message?: string;
  };
}

function apiUrl(path: string): string {
  return apiBase ? `${apiBase}${path}` : path;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(apiUrl(path), {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.headers ?? {}),
    },
  });

  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const payload = (await response.json()) as ApiErrorPayload;
      if (payload.error?.message) {
        message = payload.error.message;
      }
    } catch {
      // Ignore non-JSON error bodies.
    }
    throw new Error(message);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export function getOverview(): Promise<OverviewResponse> {
  return request("/api/overview");
}

export function getJobs(states: string[], limit = 50, offset = 0): Promise<JobsResponse> {
  const params = new URLSearchParams({
    states: states.join(","),
    limit: String(limit),
    offset: String(offset),
  });
  return request(`/api/jobs?${params.toString()}`);
}

export function getJob(jobId: string): Promise<JobDetailResponse> {
  return request(`/api/jobs/${jobId}`);
}

export function cancelJob(jobId: string): Promise<CancelJobResponse> {
  return request(`/api/jobs/${jobId}/cancel`, {
    method: "POST",
  });
}

export function getWorkers(): Promise<WorkersResponse> {
  return request("/api/workers");
}
