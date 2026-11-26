/**
 * Bearer-token authentication, tenant namespace validation, constant-time token
 * comparison, and gRPC-over-HTTP error status responses with full parity against
 * `crates/anyserve-core/src/auth.rs` and `crates/anyserve-core/src/http_ws_service.rs`.
 *
 * Compatible with both Cloudflare `workerd` and plain Node 24 Vitest,
 * with zero host-specific runtime imports.
 */

import { DEFAULT_NAMESPACE, sha256Hex } from "./identity";

export { DEFAULT_NAMESPACE };
export const NAMESPACE_HEADER = "x-anyserve-namespace";
export const GRPC_CODE_HEADER = "x-anyserve-grpc-code";
export const SIGNED_TOKEN_PREFIX = "asv1.";
export const WS_SUBPROTOCOL_V2 = "anyserve.v2";
export const WS_BEARER_SUBPROTOCOL_PREFIX = "anyserve.bearer.";

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
const utf8FatalDecoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: false,
});
const NODE_INSPECT_CUSTOM = Symbol.for("nodejs.util.inspect.custom");

export interface SignedTokenClaims {
  jti: string;
  ns: string;
  role: string;
  sub: string;
  iat: number;
  exp?: number | null;
  fp?: string | null;
}

export interface RevocationSnapshot {
  version: number;
  revoked_jtis: string[];
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlDecode(str: string): Uint8Array | null {
  if (!str || /[^A-Za-z0-9\-_]/.test(str)) {
    return null;
  }
  const padded = str.replace(/-/g, "+").replace(/_/g, "/") + "===".slice((str.length + 3) % 4);
  try {
    const binary = atob(padded);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      out[i] = binary.charCodeAt(i);
    }
    return out;
  } catch {
    return null;
  }
}

// Imported once per key buffer: a policy's signing keys verify every signed request.
const hmacKeys = new WeakMap<Uint8Array, Promise<CryptoKey>>();

async function hmacSha256(keyBytes: Uint8Array, data: Uint8Array): Promise<Uint8Array> {
  let key = hmacKeys.get(keyBytes);
  if (key === undefined) {
    key = crypto.subtle.importKey(
      "raw",
      Uint8Array.from(keyBytes),
      { name: "HMAC", hash: "SHA-256" },
      false,
      ["sign"],
    );
    hmacKeys.set(keyBytes, key);
  }
  const sig = await crypto.subtle.sign("HMAC", await key, Uint8Array.from(data));
  return new Uint8Array(sig);
}

export function internalRevocationBearer(signingKeyBytes: Uint8Array): string {
  const prefix = utf8Encoder.encode("anyserve-internal-revocations:");
  const input = new Uint8Array(prefix.byteLength + signingKeyBytes.byteLength);
  input.set(prefix, 0);
  input.set(signingKeyBytes, prefix.byteLength);
  return sha256Hex(input);
}

export async function signToken(
  keyBytes: Uint8Array,
  claims: SignedTokenClaims,
): Promise<string> {
  if (keyBytes.byteLength === 0) {
    throw new Error("signing key must not be empty");
  }
  if (!claims.jti || claims.jti.trim().length === 0) {
    throw new Error("token jti must not be empty");
  }
  validateNamespace(claims.ns);
  if (claims.role !== Role.Client && claims.role !== Role.Worker) {
    throw new Error("token role must be 'client' or 'worker'");
  }
  const payload = base64UrlEncode(utf8Encoder.encode(JSON.stringify(claims)));
  const signingInput = `${SIGNED_TOKEN_PREFIX}${payload}`;
  const mac = await hmacSha256(keyBytes, utf8Encoder.encode(signingInput));
  return `${signingInput}.${base64UrlEncode(mac)}`;
}

export async function verifySignedToken(
  token: string,
  keys: readonly Uint8Array[],
  nowSecs = Math.floor(Date.now() / 1000),
): Promise<SignedTokenClaims | null> {
  if (keys.length === 0 || !token.startsWith(SIGNED_TOKEN_PREFIX)) {
    return null;
  }
  const body = token.slice(SIGNED_TOKEN_PREFIX.length);
  const dotIdx = body.indexOf(".");
  if (dotIdx <= 0) {
    return null;
  }
  const payloadB64 = body.slice(0, dotIdx);
  const sigB64 = body.slice(dotIdx + 1);
  if (sigB64.includes(".")) {
    return null;
  }
  const sig = base64UrlDecode(sigB64);
  if (sig === null || sig.byteLength !== 32) {
    return null;
  }
  const signingBytes = utf8Encoder.encode(`${SIGNED_TOKEN_PREFIX}${payloadB64}`);
  let verified = false;
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]!;
    if (key.byteLength === 0) continue;
    const expected = await hmacSha256(key, signingBytes);
    if (constantTimeEqualBytes(expected, sig)) {
      verified = true;
    }
  }
  if (!verified) {
    return null;
  }
  const rawClaims = base64UrlDecode(payloadB64);
  if (rawClaims === null) {
    return null;
  }
  let claims: SignedTokenClaims;
  try {
    claims = JSON.parse(utf8Decoder.decode(rawClaims)) as SignedTokenClaims;
  } catch {
    return null;
  }
  if (
    typeof claims?.jti !== "string" ||
    claims.jti.trim().length === 0 ||
    typeof claims.ns !== "string" ||
    (claims.role !== Role.Client && claims.role !== Role.Worker) ||
    typeof claims.sub !== "string"
  ) {
    return null;
  }
  try {
    validateNamespace(claims.ns);
  } catch {
    return null;
  }
  if (claims.fp != null && typeof claims.fp !== "string") {
    return null;
  }
  if (claims.exp != null) {
    if (
      typeof claims.exp !== "number" ||
      !Number.isFinite(claims.exp) ||
      nowSecs >= claims.exp
    ) {
      return null;
    }
  }
  return claims;
}

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
  jti?: string | null;
  exp?: number | null;
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
  ANYSERVE_TOKEN_SIGNING_KEY?: string;
  ANYSERVE_AUTH_REVOCATION_URL?: string;
  ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS?: string;
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

export interface WsAuthResult {
  principal: Principal;
  selectedProtocol: string | null;
}

function parseWsSubprotocols(headers: HeaderBag): {
  selectedProtocol: string | null;
  bearerToken: string | null;
} {
  let hasV2 = false;
  let bearerEncoded: string | null = null;
  const raw = getHeader(headers, "sec-websocket-protocol");
  if (raw !== undefined) {
    for (const part of raw.split(",")) {
      const proto = part.trim();
      if (!proto) continue;
      if (proto === WS_SUBPROTOCOL_V2) {
        hasV2 = true;
      } else if (proto.startsWith(WS_BEARER_SUBPROTOCOL_PREFIX)) {
        if (bearerEncoded !== null) {
          throw GrpcStatusError.invalidArgument(
            "duplicate WebSocket bearer subprotocol",
          );
        }
        bearerEncoded = proto.slice(WS_BEARER_SUBPROTOCOL_PREFIX.length);
      }
    }
  }

  let decodedToken: string | null = null;
  if (bearerEncoded !== null) {
    if (getHeader(headers, "authorization") !== undefined) {
      throw GrpcStatusError.invalidArgument(
        "cannot supply both Authorization header and WebSocket bearer subprotocol",
      );
    }
    if (!hasV2) {
      throw GrpcStatusError.invalidArgument(
        "WebSocket bearer subprotocol requires 'anyserve.v2' subprotocol",
      );
    }
    if (bearerEncoded.length === 0 || bearerEncoded.length % 4 === 1) {
      throw GrpcStatusError.invalidArgument(
        "invalid WebSocket bearer subprotocol",
      );
    }
    const bytes = base64UrlDecode(bearerEncoded);
    if (bytes === null || base64UrlEncode(bytes) !== bearerEncoded) {
      throw GrpcStatusError.invalidArgument(
        "invalid WebSocket bearer subprotocol",
      );
    }
    let decoded: string;
    try {
      decoded = utf8FatalDecoder.decode(bytes);
    } catch {
      throw GrpcStatusError.invalidArgument(
        "invalid WebSocket bearer subprotocol",
      );
    }
    const trimmed = decoded.trim();
    if (trimmed.length === 0) {
      throw GrpcStatusError.invalidArgument(
        "invalid WebSocket bearer subprotocol",
      );
    }
    decodedToken = trimmed;
  }

  return {
    selectedProtocol: hasV2 ? WS_SUBPROTOCOL_V2 : null,
    bearerToken: decodedToken,
  };
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

export interface AuthPolicyOptions {
  signingKeys?: Iterable<string | Uint8Array>;
  revocationUrl?: string | null;
  revocationPollIntervalMs?: number;
}

export class AuthPolicy {
  #clientTokens: readonly Uint8Array[];
  #workerTokens: readonly Uint8Array[];
  #scopedTokens: readonly ScopedToken[];
  #signingKeys: readonly Uint8Array[];
  #revocationUrl: string | null;
  #revocationPollIntervalMs: number;
  #revokedJtis = new Set<string>();
  #revocationVersion = 0;
  #lastRevocationSyncAtMs = Number.NEGATIVE_INFINITY;
  #revocationSyncInFlight: Promise<boolean> | null = null;
  #nowMs: () => number = () => Date.now();
  #onRevocationsUpdated: ((snapshot: RevocationSnapshot) => void) | null = null;

  constructor(
    clientTokens: Iterable<string> = [],
    workerTokens: Iterable<string> = [],
    scopedTokens: Iterable<ScopedToken | ScopedTokenInit> = [],
    options: AuthPolicyOptions = {},
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
    const keys: Uint8Array[] = [];
    for (const k of options.signingKeys ?? []) {
      const bytes = typeof k === "string" ? utf8Encoder.encode(k.trim()) : k;
      if (bytes.byteLength > 0) keys.push(bytes);
    }
    this.#signingKeys = keys;
    const rawRevUrl = options.revocationUrl?.trim() ?? "";
    this.#revocationUrl = rawRevUrl.length > 0 ? rawRevUrl : null;
    this.#revocationPollIntervalMs = Math.max(
      250,
      options.revocationPollIntervalMs ?? 2_000,
    );
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

    const signingKeys = parseCommaTokens(env.ANYSERVE_TOKEN_SIGNING_KEY);
    const revocationUrl = env.ANYSERVE_AUTH_REVOCATION_URL?.trim() || null;
    const pollRaw = Number(env.ANYSERVE_AUTH_REVOCATION_POLL_INTERVAL_MS);
    const revocationPollIntervalMs =
      Number.isFinite(pollRaw) && pollRaw > 0 ? pollRaw : 2_000;

    return new AuthPolicy(clientTokens, workerTokens, scoped, {
      signingKeys,
      revocationUrl,
      revocationPollIntervalMs,
    });
  }

  withScopedTokens(
    tokens: Iterable<ScopedToken | ScopedTokenInit>,
  ): AuthPolicy {
    const next = new AuthPolicy([], [], tokens);
    const out = new AuthPolicy([], [], [], {
      signingKeys: this.#signingKeys,
      revocationUrl: this.#revocationUrl,
      revocationPollIntervalMs: this.#revocationPollIntervalMs,
    });
    out.#clientTokens = this.#clientTokens;
    out.#workerTokens = this.#workerTokens;
    out.#scopedTokens = next.#scopedTokens;
    out.#revokedJtis = new Set(this.#revokedJtis);
    out.#revocationVersion = this.#revocationVersion;
    out.#lastRevocationSyncAtMs = this.#lastRevocationSyncAtMs;
    out.#nowMs = this.#nowMs;
    out.#onRevocationsUpdated = this.#onRevocationsUpdated;
    return out;
  }

  setClock(nowMs: () => number): void {
    this.#nowMs = nowMs;
  }

  setRevocationListener(
    listener: ((snapshot: RevocationSnapshot) => void) | null,
  ): void {
    this.#onRevocationsUpdated = listener;
  }

  hasRevocationPolling(): boolean {
    return this.#revocationUrl !== null && this.#signingKeys.length > 0;
  }

  revocationPollIntervalMs(): number {
    return this.#revocationPollIntervalMs;
  }

  isJtiRevoked(jti: string): boolean {
    return this.#revokedJtis.has(jti);
  }

  revocationSnapshot(): RevocationSnapshot {
    return {
      version: this.#revocationVersion,
      revoked_jtis: [...this.#revokedJtis],
    };
  }

  isEnabled(): boolean {
    return (
      this.#clientTokens.length > 0 ||
      this.#workerTokens.length > 0 ||
      this.#scopedTokens.length > 0 ||
      this.#signingKeys.length > 0
    );
  }

  applyRevocations(snapshot: RevocationSnapshot, notify = true): boolean {
    if (
      !snapshot ||
      typeof snapshot.version !== "number" ||
      !Number.isFinite(snapshot.version) ||
      snapshot.version < this.#revocationVersion
    ) {
      return false;
    }
    const nextJtis = new Set(
      Array.isArray(snapshot.revoked_jtis)
        ? snapshot.revoked_jtis.filter((j): j is string => typeof j === "string")
        : [],
    );
    const changed =
      snapshot.version !== this.#revocationVersion ||
      nextJtis.size !== this.#revokedJtis.size ||
      [...nextJtis].some((j) => !this.#revokedJtis.has(j));
    this.#revocationVersion = snapshot.version;
    this.#revokedJtis = nextJtis;
    if (changed && notify) {
      this.#onRevocationsUpdated?.(this.revocationSnapshot());
    }
    return changed;
  }

  async maybeSyncRevocations(nowMs = this.#nowMs()): Promise<boolean> {
    if (!this.#revocationUrl || this.#signingKeys.length === 0) {
      return false;
    }
    if (this.#revocationSyncInFlight !== null) {
      return await this.#revocationSyncInFlight;
    }
    if (nowMs - this.#lastRevocationSyncAtMs < this.#revocationPollIntervalMs) {
      return false;
    }
    const syncPromise = (async (): Promise<boolean> => {
      let updated = false;
      try {
        const sep = this.#revocationUrl!.includes("?") ? "&" : "?";
        const url = `${this.#revocationUrl}${sep}since_version=${this.#revocationVersion}`;
        const bearer = internalRevocationBearer(this.#signingKeys[0]!);
        const resp = await fetch(url, {
          headers: { Authorization: `Bearer ${bearer}` },
          signal: AbortSignal.timeout(5_000),
        });
        if (resp.status === 200) {
          const snap = (await resp.json()) as RevocationSnapshot;
          updated = this.applyRevocations(snap);
        }
      } catch {
        // Best-effort cached polling; retain previous snapshot on transient errors.
      } finally {
        this.#lastRevocationSyncAtMs = nowMs;
        this.#revocationSyncInFlight = null;
      }
      return updated;
    })();
    this.#revocationSyncInFlight = syncPromise;
    return await syncPromise;
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

  async principalForTokenAsync(
    candidate: string,
    nowSecs = Math.floor(this.#nowMs() / 1000),
  ): Promise<Principal | null> {
    const staticPrincipal = this.principalForToken(candidate);
    if (staticPrincipal !== null) {
      return staticPrincipal;
    }
    if (!candidate.startsWith(SIGNED_TOKEN_PREFIX) || this.#signingKeys.length === 0) {
      return null;
    }
    // Only a correctly signed token may trigger a revocation refresh.
    const claims = await verifySignedToken(candidate, this.#signingKeys, nowSecs);
    if (claims === null) {
      return null;
    }
    await this.maybeSyncRevocations(nowSecs * 1000);
    if (this.#revokedJtis.has(claims.jti)) {
      return null;
    }
    return {
      namespace: claims.ns,
      role: claims.role as Role,
      subject: claims.sub,
      forcedPool: claims.fp ?? null,
      isOperator: false,
      jti: claims.jti,
      exp: claims.exp ?? null,
    };
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

  /**
   * Resolves the bearer principal (static or `asv1.` signed token) and lets `deny`
   * reject it with a 403 reason. Anonymous mode returns the `anonymousRole`
   * principal honouring `x-anyserve-namespace` (400/3 if malformed); a missing or
   * unknown token is 401/16.
   */
  async #authorize(
    headers: HeaderBag,
    anonymousRole: Role,
    deny: (principal: Principal) => string | null,
  ): Promise<Principal> {
    if (!this.isEnabled()) {
      return this.#unauthenticatedPrincipal(anonymousRole, headers);
    }
    const token = extractBearerToken(headers);
    const principal = token === null ? null : await this.principalForTokenAsync(token);
    if (principal === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    const reason = deny(principal);
    if (reason !== null) {
      throw GrpcStatusError.permissionDenied(reason);
    }
    return principal;
  }

  verifyClientHeaders(headers: HeaderBag): Promise<Principal> {
    return this.#authorize(headers, Role.Client, (p) =>
      p.role === Role.Client ? null : "worker token cannot be used for client requests",
    );
  }

  verifyWorkerHeaders(headers: HeaderBag): Promise<Principal> {
    return this.#authorize(headers, Role.Worker, (p) =>
      p.role === Role.Worker ? null : "client token cannot be used for worker requests",
    );
  }

  async #authorizeWs(
    headers: HeaderBag,
    anonymousRole: Role,
    deny: (principal: Principal) => string | null,
  ): Promise<WsAuthResult> {
    const { selectedProtocol, bearerToken: subprotocolToken } =
      parseWsSubprotocols(headers);
    if (!this.isEnabled()) {
      return {
        principal: this.#unauthenticatedPrincipal(anonymousRole, headers),
        selectedProtocol,
      };
    }
    const token = subprotocolToken ?? extractBearerToken(headers);
    const principal = token === null ? null : await this.principalForTokenAsync(token);
    if (principal === null) {
      throw GrpcStatusError.unauthenticated("missing or invalid bearer token");
    }
    const reason = deny(principal);
    if (reason !== null) {
      throw GrpcStatusError.permissionDenied(reason);
    }
    return { principal, selectedProtocol };
  }

  verifyWsClientHeaders(headers: HeaderBag): Promise<WsAuthResult> {
    return this.#authorizeWs(headers, Role.Client, (p) =>
      p.role === Role.Client ? null : "worker token cannot be used for client requests",
    );
  }

  verifyWsWorkerHeaders(headers: HeaderBag): Promise<WsAuthResult> {
    return this.#authorizeWs(headers, Role.Worker, (p) =>
      p.role === Role.Worker ? null : "client token cannot be used for worker requests",
    );
  }

  /**
   * Verifies management/operator HTTP access (`/api/*`): when auth is enabled it
   * requires an operator-level client token (`client_tokens`); worker and
   * tenant-scoped tokens (even in `"default"`) throw 403/7.
   */
  authorizeOperatorHeaders(headers: HeaderBag): Promise<Principal> {
    return this.#authorize(headers, Role.Client, (p) =>
      p.role === Role.Client && p.isOperator
        ? null
        : "management API requires an operator client token",
    );
  }

  /**
   * Verifies Console `/api/*` access: allows both operator client tokens and
   * tenant-scoped client tokens, returning `Principal` so handlers enforce
   * namespace isolation.
   */
  authorizeConsoleHeaders(headers: HeaderBag): Promise<Principal> {
    return this.#authorize(headers, Role.Client, (p) =>
      p.role === Role.Client ? null : "worker token cannot access console API",
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
