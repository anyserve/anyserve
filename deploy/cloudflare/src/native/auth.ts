/**
 * Bearer-token authentication, tenant namespace validation, constant-time token
 * comparison, and gRPC-over-HTTP error status responses with full parity against
 * `crates/anyserve-core/src/auth.rs` and `crates/anyserve-core/src/http_ws_service.rs`.
 *
 * Compatible with both Cloudflare `workerd` and plain Node 24 Vitest,
 * with zero host-specific runtime imports.
 */

import { DEFAULT_NAMESPACE } from "./identity";

export { DEFAULT_NAMESPACE };
export const NAMESPACE_HEADER = "x-anyserve-namespace";
export const GRPC_CODE_HEADER = "x-anyserve-grpc-code";

const utf8Encoder = new TextEncoder();
const NODE_INSPECT_CUSTOM = Symbol.for("nodejs.util.inspect.custom");

// ---------- gRPC Status Codes & HTTP Mapping ----------

export enum GrpcCode {
  Ok = 0,
  Cancelled = 1,
  Unknown = 2,
  InvalidArgument = 3,
  DeadlineExceeded = 4,
  NotFound = 5,
  AlreadyExists = 6,
  PermissionDenied = 7,
  ResourceExhausted = 8,
  FailedPrecondition = 9,
  Aborted = 10,
  OutOfRange = 11,
  Unimplemented = 12,
  Internal = 13,
  Unavailable = 14,
  DataLoss = 15,
  Unauthenticated = 16,
}

/**
 * Exact parity with `http_status_for_grpc_code` in
 * `crates/anyserve-core/src/http_ws_service.rs`.
 */
export function httpStatusForGrpcCode(code: GrpcCode | number): number {
  switch (code) {
    case GrpcCode.InvalidArgument:
    case GrpcCode.OutOfRange:
      return 400;
    case GrpcCode.Unauthenticated:
      return 401;
    case GrpcCode.PermissionDenied:
      return 403;
    case GrpcCode.NotFound:
      return 404;
    case GrpcCode.AlreadyExists:
    case GrpcCode.Aborted:
      return 409;
    case GrpcCode.FailedPrecondition:
      return 412;
    case GrpcCode.ResourceExhausted:
      return 429;
    case GrpcCode.Unimplemented:
      return 501;
    case GrpcCode.Unavailable:
      return 503;
    case GrpcCode.DeadlineExceeded:
      return 504;
    default:
      return 500;
  }
}

export class GrpcStatusError extends Error {
  readonly code: GrpcCode;

  constructor(code: GrpcCode, message: string) {
    super(message);
    this.name = "GrpcStatusError";
    this.code = code;
  }

  get httpStatus(): number {
    return httpStatusForGrpcCode(this.code);
  }

  toResponse(extraHeaders?: HeadersInit): Response {
    return grpcErrorResponse(this.code, this.message, extraHeaders);
  }

  static invalidArgument(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.InvalidArgument, message);
  }
  static deadlineExceeded(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.DeadlineExceeded, message);
  }
  static notFound(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.NotFound, message);
  }
  static alreadyExists(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.AlreadyExists, message);
  }
  static permissionDenied(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.PermissionDenied, message);
  }
  static resourceExhausted(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.ResourceExhausted, message);
  }
  static failedPrecondition(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.FailedPrecondition, message);
  }
  static aborted(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.Aborted, message);
  }
  static outOfRange(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.OutOfRange, message);
  }
  static unimplemented(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.Unimplemented, message);
  }
  static internal(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.Internal, message);
  }
  static unavailable(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.Unavailable, message);
  }
  static unauthenticated(message: string): GrpcStatusError {
    return new GrpcStatusError(GrpcCode.Unauthenticated, message);
  }
}

export function grpcErrorResponse(
  error: GrpcStatusError,
  extraHeaders?: HeadersInit,
): Response;
export function grpcErrorResponse(
  code: GrpcCode | number,
  message: string,
  extraHeaders?: HeadersInit,
): Response;
export function grpcErrorResponse(
  codeOrError: GrpcStatusError | GrpcCode | number,
  messageOrHeaders?: string | HeadersInit,
  maybeHeaders?: HeadersInit,
): Response {
  const code =
    codeOrError instanceof GrpcStatusError ? codeOrError.code : codeOrError;
  const message =
    codeOrError instanceof GrpcStatusError
      ? codeOrError.message
      : (messageOrHeaders as string);
  const extra =
    codeOrError instanceof GrpcStatusError
      ? (messageOrHeaders as HeadersInit | undefined)
      : maybeHeaders;

  const headers = new Headers(extra);
  headers.set("content-type", "text/plain; charset=utf-8");
  headers.set(GRPC_CODE_HEADER, String(code | 0));
  return new Response(message, {
    status: httpStatusForGrpcCode(code),
    headers,
  });
}

// ---------- Namespace Validation ----------

/**
 * Validates a tenant namespace identifier (`1..=64` ASCII `[a-zA-Z0-9._-]`).
 * Throws `GrpcStatusError` (`InvalidArgument` = 3, HTTP 400) on invalid input.
 */
export function validateNamespace(namespace: string): string {
  if (namespace.length === 0 || namespace.length > 64) {
    throw GrpcStatusError.invalidArgument(
      "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
    );
  }
  for (let i = 0; i < namespace.length; i++) {
    const c = namespace.charCodeAt(i);
    const ok =
      (c >= 0x30 && c <= 0x39) || // 0-9
      (c >= 0x41 && c <= 0x5a) || // A-Z
      (c >= 0x61 && c <= 0x7a) || // a-z
      c === 0x2e || // .
      c === 0x5f || // _
      c === 0x2d; // -
    if (!ok) {
      throw GrpcStatusError.invalidArgument(
        "namespace must be 1..=64 ASCII characters in [a-zA-Z0-9._-]",
      );
    }
  }
  return namespace;
}

// ---------- Constant-Time Comparison Without Early Exit ----------

/**
 * Compares two byte arrays in constant time relative to `max(a.length, b.length)`
 * with NO early exit on length or byte mismatch.
 */
export function constantTimeEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
  let diff = a.byteLength ^ b.byteLength;
  const maxLen = a.byteLength > b.byteLength ? a.byteLength : b.byteLength;
  for (let i = 0; i < maxLen; i++) {
    const byteA = i < a.byteLength ? a[i]! : 0;
    const byteB = i < b.byteLength ? b[i]! : 0;
    diff |= byteA ^ byteB;
  }
  return diff === 0;
}

export function constantTimeEqual(left: string, right: string): boolean {
  return constantTimeEqualBytes(
    utf8Encoder.encode(left),
    utf8Encoder.encode(right),
  );
}

// ---------- Roles, Principals & Redacted Tokens ----------

export const Role = {
  Client: "client",
  Worker: "worker",
} as const;

export type Role = (typeof Role)[keyof typeof Role];

export interface Principal {
  namespace: string;
  role: Role;
  subject: string;
  forcedPool: string | null;
  isOperator: boolean;
}

export function defaultPrincipal(): Principal {
  return {
    namespace: DEFAULT_NAMESPACE,
    role: Role.Client,
    subject: "anonymous",
    forcedPool: null,
    isOperator: true,
  };
}

export interface ScopedTokenInit {
  token: string;
  role: Role;
  namespace: string;
  subject?: string;
  forcedPool?: string | null;
  forced_pool?: string | null;
}

export interface ScopedTokenRedactedJSON {
  token: "[redacted]";
  role: Role;
  namespace: string;
  subject: string;
  forcedPool: string | null;
}

export class ScopedToken {
  readonly #tokenBytes: Uint8Array;
  readonly role: Role;
  readonly namespace: string;
  readonly subject: string;
  readonly forcedPool: string | null;

  constructor(init: ScopedTokenInit) {
    const trimmedToken = init.token.trim();
    this.#tokenBytes = utf8Encoder.encode(trimmedToken);
    this.role = init.role;
    this.namespace = validateNamespace(init.namespace.trim());
    const rawSubject = init.subject?.trim() ?? "";
    this.subject =
      rawSubject.length > 0 ? rawSubject : `${init.role}-token`;
    const rawPool = (init.forcedPool ?? init.forced_pool ?? "").trim();
    this.forcedPool = rawPool.length > 0 ? rawPool : null;
  }

  /** Internal check whether the trimmed token was non-empty. */
  get isNonEmpty(): boolean {
    return this.#tokenBytes.byteLength > 0;
  }

  /** Constant-time match against pre-encoded candidate token bytes. */
  matchesBytes(candidateBytes: Uint8Array): boolean {
    if (this.#tokenBytes.byteLength === 0) return false;
    return constantTimeEqualBytes(this.#tokenBytes, candidateBytes);
  }

  toJSON(): ScopedTokenRedactedJSON {
    return {
      token: "[redacted]",
      role: this.role,
      namespace: this.namespace,
      subject: this.subject,
      forcedPool: this.forcedPool,
    };
  }

  toString(): string {
    const poolStr =
      this.forcedPool === null ? "None" : `Some(${JSON.stringify(this.forcedPool)})`;
    return `ScopedToken { token: "[redacted]", role: ${this.role}, namespace: ${JSON.stringify(this.namespace)}, subject: ${JSON.stringify(this.subject)}, forced_pool: ${poolStr} }`;
  }

  [NODE_INSPECT_CUSTOM](): string {
    return this.toString();
  }
}

export interface AuthEnvLike {
  ANYSERVE_CLIENT_TOKENS?: string;
  ANYSERVE_WORKER_TOKENS?: string;
  ANYSERVE_SCOPED_TOKENS?: string;
}

export type HeaderBag =
  | Headers
  | Record<string, string | readonly string[] | undefined>
  | Iterable<readonly [string, string]>;

function getHeader(headers: HeaderBag, lowerName: string): string | undefined {
  if (typeof Headers !== "undefined" && headers instanceof Headers) {
    return headers.get(lowerName) ?? undefined;
  }
  if (Symbol.iterator in headers) {
    for (const [k, v] of headers as Iterable<readonly [string, string]>) {
      if (k.toLowerCase() === lowerName) {
        return v;
      }
    }
    return undefined;
  }
  const rec = headers as Record<string, string | readonly string[] | undefined>;
  const direct = rec[lowerName];
  if (typeof direct === "string") return direct;
  if (Array.isArray(direct)) return direct[0];
  const keys = Object.keys(rec);
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    if (k.toLowerCase() === lowerName) {
      const val = rec[k];
      if (typeof val === "string") return val;
      if (Array.isArray(val)) return val[0];
    }
  }
  return undefined;
}

function extractBearerToken(headers: HeaderBag): string | null {
  const raw = getHeader(headers, "authorization");
  if (raw === undefined) return null;
  return raw.startsWith("Bearer ") || raw.startsWith("bearer ") ? raw.slice(7).trim() : null;
}

function parseCommaTokens(raw: string | undefined): string[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((t) => t.trim())
    .filter((t) => t.length > 0);
}

function dedupeToEncodedTokens(tokens: Iterable<string>): Uint8Array[] {
  const unique = new Set<string>();
  for (const tok of tokens) {
    const trimmed = tok.trim();
    if (trimmed.length > 0) {
      unique.add(trimmed);
    }
  }
  const sorted = Array.from(unique).sort();
  return sorted.map((t) => utf8Encoder.encode(t));
}

function containsTokenConstantTime(
  stored: readonly Uint8Array[],
  candidateBytes: Uint8Array,
): boolean {
  let found = 0;
  for (let i = 0; i < stored.length; i++) {
    if (constantTimeEqualBytes(stored[i]!, candidateBytes)) {
      found |= 1;
    }
  }
  return found === 1;
}

export interface AuthPolicyRedactedJSON {
  clientTokenCount: number;
  workerTokenCount: number;
  scopedTokens: ScopedTokenRedactedJSON[];
}

export class AuthPolicy {
  #clientTokens: readonly Uint8Array[];
  #workerTokens: readonly Uint8Array[];
  #scopedTokens: readonly ScopedToken[];

  constructor(
    clientTokens: Iterable<string> = [],
    workerTokens: Iterable<string> = [],
    scopedTokens: Iterable<ScopedToken | ScopedTokenInit> = [],
  ) {
    this.#clientTokens = dedupeToEncodedTokens(clientTokens);
    this.#workerTokens = dedupeToEncodedTokens(workerTokens);
    const normalizedScoped: ScopedToken[] = [];
    for (const item of scopedTokens) {
      const st = item instanceof ScopedToken ? item : new ScopedToken(item);
      if (st.isNonEmpty) {
        normalizedScoped.push(st);
      }
    }
    this.#scopedTokens = normalizedScoped;
  }

  static empty(): AuthPolicy {
    return new AuthPolicy();
  }

  /**
   * Builds an `AuthPolicy` from Cloudflare Worker / environment bindings:
   * - `ANYSERVE_CLIENT_TOKENS`: comma-separated operator client tokens (trimmed)
   * - `ANYSERVE_WORKER_TOKENS`: comma-separated default-namespace worker tokens (trimmed)
   * - `ANYSERVE_SCOPED_TOKENS`: JSON array of `{ token, role, namespace, subject?, forced_pool? }`
   */
  static fromEnv(env: AuthEnvLike): AuthPolicy {
    const clientTokens = parseCommaTokens(env.ANYSERVE_CLIENT_TOKENS);
    const workerTokens = parseCommaTokens(env.ANYSERVE_WORKER_TOKENS);
    const scoped: ScopedToken[] = [];

    const rawScoped = env.ANYSERVE_SCOPED_TOKENS?.trim();
    if (rawScoped) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(rawScoped);
      } catch {
        throw new Error("ANYSERVE_SCOPED_TOKENS must be valid JSON");
      }
      if (!Array.isArray(parsed)) {
        throw new Error("ANYSERVE_SCOPED_TOKENS must be a JSON array");
      }
      for (let i = 0; i < parsed.length; i++) {
        const entry = parsed[i] as Record<string, unknown> | null;
        if (!entry || typeof entry !== "object") {
          throw new Error(
            `ANYSERVE_SCOPED_TOKENS[${i}] must be a JSON object`,
          );
        }
        if (typeof entry.token !== "string" || entry.token.trim().length === 0) {
          throw new Error(
            `ANYSERVE_SCOPED_TOKENS[${i}].token must be a non-empty string`,
          );
        }
        if (typeof entry.role !== "string") {
          throw new Error(
            `ANYSERVE_SCOPED_TOKENS[${i}].role must be 'client' or 'worker'`,
          );
        }
        const roleLower = entry.role.trim().toLowerCase();
        let role: Role;
        if (roleLower === "client") {
          role = Role.Client;
        } else if (roleLower === "worker") {
          role = Role.Worker;
        } else {
          throw new Error(
            `unknown scoped_token role '${entry.role}' (expected client|worker)`,
          );
        }
        if (typeof entry.namespace !== "string") {
          throw new Error(
            `ANYSERVE_SCOPED_TOKENS[${i}].namespace must be a string`,
          );
        }
        const subject =
          typeof entry.subject === "string" ? entry.subject : undefined;
        const rawPool = entry.forced_pool ?? entry.forcedPool;
        const forcedPool = typeof rawPool === "string" ? rawPool : null;
        scoped.push(
          new ScopedToken({
            token: entry.token,
            role,
            namespace: entry.namespace,
            subject,
            forcedPool,
          }),
        );
      }
    }

    return new AuthPolicy(clientTokens, workerTokens, scoped);
  }

  withScopedTokens(
    tokens: Iterable<ScopedToken | ScopedTokenInit>,
  ): AuthPolicy {
    const next = new AuthPolicy([], [], tokens);
    const out = new AuthPolicy();
    out.#clientTokens = this.#clientTokens;
    out.#workerTokens = this.#workerTokens;
    out.#scopedTokens = next.#scopedTokens;
    return out;
  }

  isEnabled(): boolean {
    return (
      this.#clientTokens.length > 0 ||
      this.#workerTokens.length > 0 ||
      this.#scopedTokens.length > 0
    );
  }

  roleForToken(candidate: string): Role | null {
    return this.principalForToken(candidate)?.role ?? null;
  }

  /**
   * Resolves a bearer token to its `Principal` without early exit across any
   * configured token list. Precedence matches Rust `AuthPolicy::principal_for_token`:
   * scoped_tokens -> worker_tokens -> client_tokens.
   */
  principalForToken(candidate: string): Principal | null {
    const candidateBytes = utf8Encoder.encode(candidate);
    let matched: Principal | null = null;

    for (let i = 0; i < this.#scopedTokens.length; i++) {
      const entry = this.#scopedTokens[i]!;
      const eq = entry.matchesBytes(candidateBytes);
      if (eq && matched === null) {
        matched = {
          namespace: entry.namespace,
          role: entry.role,
          subject: entry.subject,
          forcedPool: entry.forcedPool,
          isOperator: false,
        };
      }
    }

    const workerHit = containsTokenConstantTime(
      this.#workerTokens,
      candidateBytes,
    );
    if (workerHit && matched === null) {
      matched = {
        namespace: DEFAULT_NAMESPACE,
        role: Role.Worker,
        subject: "worker-token",
        forcedPool: null,
        isOperator: false,
      };
    }

    const clientHit = containsTokenConstantTime(
      this.#clientTokens,
      candidateBytes,
    );
    if (clientHit && matched === null) {
      matched = {
        namespace: DEFAULT_NAMESPACE,
        role: Role.Client,
        subject: "client-token",
        forcedPool: null,
        isOperator: true,
      };
    }

    return matched;
  }

  #unauthenticatedPrincipal(role: Role, headers: HeaderBag): Principal {
    const rawNs = getHeader(headers, NAMESPACE_HEADER)?.trim();
    const namespace =
      rawNs && rawNs.length > 0 ? validateNamespace(rawNs) : DEFAULT_NAMESPACE;
    return {
      namespace,
      role,
      subject: "anonymous",
      forcedPool: null,
      isOperator: role === Role.Client,
    };
  }

  #verifyRoleHeaders(headers: HeaderBag, expected: Role): Principal {
    if (!this.isEnabled()) {
      return this.#unauthenticatedPrincipal(expected, headers);
    }
    const token = extractBearerToken(headers);
    const principal = token !== null ? this.principalForToken(token) : null;
    if (principal === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    if (principal.role !== expected) {
      throw GrpcStatusError.permissionDenied(
        expected === Role.Client
          ? "worker token cannot be used for client requests"
          : "client token cannot be used for worker requests",
      );
    }
    return principal;
  }

  verifyClientHeaders(headers: HeaderBag): Principal {
    return this.#verifyRoleHeaders(headers, Role.Client);
  }

  verifyWorkerHeaders(headers: HeaderBag): Principal {
    return this.#verifyRoleHeaders(headers, Role.Worker);
  }

  /**
   * Verifies management/operator HTTP access (`/api/*`):
   * - In anonymous mode (no tokens configured), returns an operator `Principal`
   *   honouring `x-anyserve-namespace` (throwing 400/3 if malformed).
   * - When auth is enabled, requires an operator-level client token (`client_tokens`);
   *   missing/unknown tokens throw 401/16, while worker and tenant-scoped tokens
   *   (even in `"default"`) throw 403/7.
   */
  authorizeOperatorHeaders(headers: HeaderBag): Principal {
    if (!this.isEnabled()) {
      return this.#unauthenticatedPrincipal(Role.Client, headers);
    }
    const token = extractBearerToken(headers);
    if (token === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    const principal = this.principalForToken(token);
    if (principal === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    if (principal.role === Role.Client && principal.isOperator) {
      return principal;
    }
    throw GrpcStatusError.permissionDenied(
      "management API requires an operator client token",
    );
  }

  /**
   * Verifies Console `/api/*` access: allows both operator client tokens and
   * tenant-scoped client tokens, returning `Principal` so handlers enforce
   * namespace isolation.
   */
  authorizeConsoleHeaders(headers: HeaderBag): Principal {
    if (!this.isEnabled()) {
      return this.#unauthenticatedPrincipal(Role.Client, headers);
    }
    const token = extractBearerToken(headers);
    if (token === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    const principal = this.principalForToken(token);
    if (principal === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    if (principal.role === Role.Client) {
      return principal;
    }
    throw GrpcStatusError.permissionDenied(
      "worker token cannot access console API",
    );
  }

  toJSON(): AuthPolicyRedactedJSON {
    return {
      clientTokenCount: this.#clientTokens.length,
      workerTokenCount: this.#workerTokens.length,
      scopedTokens: this.#scopedTokens.map((t) => t.toJSON()),
    };
  }

  toString(): string {
    const scopedStr = this.#scopedTokens.map((t) => t.toString()).join(", ");
    return `AuthPolicy { client_token_count: ${this.#clientTokens.length}, worker_token_count: ${this.#workerTokens.length}, scoped_tokens: [${scopedStr}] }`;
  }

  [NODE_INSPECT_CUSTOM](): string {
    return this.toString();
  }
}
