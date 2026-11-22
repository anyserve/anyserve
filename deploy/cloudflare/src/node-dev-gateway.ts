import { mkdirSync } from "node:fs";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { dirname } from "node:path";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import {
  JournalStorage,
  type DurableObjectStorageLike,
  type JournalStorageOptions,
  type R2BucketLike,
  type R2ObjectBodyLike,
  type R2RangeLike,
  type SqlCursorLike,
  type SqlStorageLike,
} from "./storage";

export const CLOUDFLARE_SQL_MAX_PARAMS = 100;
export const CLOUDFLARE_SQL_MAX_BYTES = 100 * 1024;

export interface InstrumentedStorageStats {
  queriesExecuted: number;
  transactionsExecuted: number;
  maxBoundParams: number;
  maxSqlBytes: number;
}

export interface InstrumentedStorageOptions {
  enforceCloudflareLimits?: boolean;
}

function isThenable(value: unknown): boolean {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

function toSqlInputValue(value: unknown): SQLInputValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint"
  ) {
    return value;
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  throw new TypeError(`Unsupported SQL binding type: ${typeof value}`);
}

export function createNodeSqliteStorage(
  db: DatabaseSync,
  options: InstrumentedStorageOptions = {},
): {
  storage: DurableObjectStorageLike;
  stats: InstrumentedStorageStats;
} {
  const enforce = options.enforceCloudflareLimits ?? true;
  const stats: InstrumentedStorageStats = {
    queriesExecuted: 0,
    transactionsExecuted: 0,
    maxBoundParams: 0,
    maxSqlBytes: 0,
  };
  let inTransaction = false;

  const sql: SqlStorageLike = {
    exec<T extends Record<string, any>>(
      query: string,
      ...bindings: any[]
    ): SqlCursorLike<T> {
      const sqlBytes = Buffer.byteLength(query, "utf8");
      stats.queriesExecuted += 1;
      stats.maxBoundParams = Math.max(stats.maxBoundParams, bindings.length);
      stats.maxSqlBytes = Math.max(stats.maxSqlBytes, sqlBytes);

      if (enforce) {
        if (bindings.length > CLOUDFLARE_SQL_MAX_PARAMS) {
          throw new Error(
            `Cloudflare DO SQLite limit exceeded: ${bindings.length} bound parameters (max ${CLOUDFLARE_SQL_MAX_PARAMS})`,
          );
        }
        if (sqlBytes > CLOUDFLARE_SQL_MAX_BYTES) {
          throw new Error(
            `Cloudflare DO SQLite limit exceeded: ${sqlBytes} SQL bytes (max ${CLOUDFLARE_SQL_MAX_BYTES})`,
          );
        }
      }

      const stmt = db.prepare(query);
      const normalized = bindings.map(toSqlInputValue);
      const rows = stmt.all(...normalized) as T[];
      return {
        toArray: () => rows,
        [Symbol.iterator]: () => rows[Symbol.iterator](),
      };
    },
  };

  const storage: DurableObjectStorageLike = {
    sql,
    transactionSync<T>(closure: () => T): T {
      if (inTransaction) {
        throw new Error("Nested transactionSync is not supported");
      }
      stats.transactionsExecuted += 1;
      inTransaction = true;
      db.exec("BEGIN IMMEDIATE");
      try {
        const result = closure();
        if (isThenable(result)) {
          throw new Error(
            "transactionSync callback returned a Promise/thenable; async/await inside transactionSync is forbidden",
          );
        }
        db.exec("COMMIT");
        return result;
      } catch (error) {
        try {
          db.exec("ROLLBACK");
        } catch {
          // Ignore rollback error if transaction already aborted.
        }
        throw error;
      } finally {
        inTransaction = false;
      }
    },
  };

  return { storage, stats };
}

export interface R2GetCall {
  key: string;
  range?: R2RangeLike;
}

export class InMemoryR2Bucket implements R2BucketLike {
  private readonly objects = new Map<string, Uint8Array>();
  readonly getCalls: R2GetCall[] = [];
  readonly putKeys: string[] = [];
  readonly deleteCalls: string[][] = [];

  get size(): number {
    return this.objects.size;
  }

  has(key: string): boolean {
    return this.objects.has(key);
  }

  keys(): string[] {
    return [...this.objects.keys()].sort();
  }

  async get(
    key: string,
    options?: { range?: R2RangeLike },
  ): Promise<R2ObjectBodyLike | null> {
    this.getCalls.push({ key, range: options?.range });
    const data = this.objects.get(key);
    if (!data) {
      return null;
    }
    let slice = data;
    if (options?.range) {
      const offset = Math.max(0, options.range.offset ?? 0);
      const end =
        options.range.length !== undefined
          ? Math.min(data.byteLength, offset + Math.max(0, options.range.length))
          : data.byteLength;
      slice = data.subarray(offset, end);
    }
    const copy = new Uint8Array(slice.byteLength);
    copy.set(slice);
    return {
      arrayBuffer: async () => copy.buffer,
    };
  }

  async put(
    key: string,
    value:
      | ReadableStream
      | ArrayBuffer
      | ArrayBufferView
      | string
      | Blob
      | null,
  ): Promise<unknown> {
    this.putKeys.push(key);
    if (value === null) {
      this.objects.set(key, new Uint8Array(0));
      return { key };
    }
    if (typeof value === "string") {
      this.objects.set(key, new TextEncoder().encode(value));
      return { key };
    }
    if (value instanceof Uint8Array) {
      this.objects.set(key, new Uint8Array(value));
      return { key };
    }
    if (value instanceof ArrayBuffer) {
      this.objects.set(key, new Uint8Array(value.slice(0)));
      return { key };
    }
    if (ArrayBuffer.isView(value)) {
      const view = new Uint8Array(
        value.buffer,
        value.byteOffset,
        value.byteLength,
      );
      this.objects.set(key, new Uint8Array(view));
      return { key };
    }
    if (value instanceof Blob) {
      this.objects.set(key, new Uint8Array(await value.arrayBuffer()));
      return { key };
    }
    const resp = new Response(value);
    this.objects.set(key, new Uint8Array(await resp.arrayBuffer()));
    return { key };
  }

  async delete(keys: string | string[]): Promise<void> {
    const list = Array.isArray(keys) ? keys : [keys];
    this.deleteCalls.push(list);
    for (const key of list) {
      this.objects.delete(key);
    }
  }
}

export interface NodeJournalGatewayOptions extends JournalStorageOptions {
  dbPath?: string;
  host?: string;
  port?: number;
  enforceCloudflareLimits?: boolean;
}

export interface NodeJournalGatewayHandle {
  server: Server;
  url: string;
  port: number;
  db: DatabaseSync;
  r2: InMemoryR2Bucket;
  storage: JournalStorage;
  stats: InstrumentedStorageStats;
  close: () => Promise<void>;
}

async function readNodeRequestBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(typeof chunk === "string" ? Buffer.from(chunk) : chunk);
  }
  return Buffer.concat(chunks);
}

async function writeNodeResponse(
  res: ServerResponse,
  webRes: Response,
): Promise<void> {
  res.statusCode = webRes.status;
  webRes.headers.forEach((value, key) => {
    res.setHeader(key, value);
  });
  const body = Buffer.from(await webRes.arrayBuffer());
  res.end(body);
}

export async function startNodeJournalGatewayServer(
  options: NodeJournalGatewayOptions = {},
): Promise<NodeJournalGatewayHandle> {
  const dbPath = options.dbPath ?? ":memory:";
  if (dbPath !== ":memory:") {
    mkdirSync(dirname(dbPath), { recursive: true });
  }
  const db = new DatabaseSync(dbPath);
  const { storage: sqlStorage, stats } = createNodeSqliteStorage(db, {
    enforceCloudflareLimits: options.enforceCloudflareLimits ?? true,
  });
  const r2 = new InMemoryR2Bucket();
  const journalStorage = new JournalStorage(sqlStorage, r2, options);
  journalStorage.ensureSchema();

  const host = options.host ?? "127.0.0.1";
  const port = options.port ?? 0;

  const server = createServer(async (req, res) => {
    try {
      const url = `http://${req.headers.host ?? `${host}:${port}`}${req.url ?? "/"}`;
      const headers = new Headers();
      for (const [key, value] of Object.entries(req.headers)) {
        if (typeof value === "string") {
          headers.set(key, value);
        } else if (Array.isArray(value)) {
          headers.set(key, value.join(", "));
        }
      }
      const method = req.method ?? "GET";
      const body =
        method === "GET" || method === "HEAD"
          ? undefined
          : new Uint8Array(await readNodeRequestBody(req));
      const webReq = new Request(url, {
        method,
        headers,
        body,
      });
      const webRes = await journalStorage.handleRequest(webReq);
      await writeNodeResponse(res, webRes);
    } catch (error) {
      res.statusCode = 500;
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify({
          error: error instanceof Error ? error.message : String(error),
        }),
      );
    }
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.removeListener("error", reject);
      resolve();
    });
  });

  const addr = server.address();
  if (!addr || typeof addr === "string") {
    throw new Error("Unexpected server address format");
  }
  const actualPort = addr.port;
  const url = `http://${host}:${actualPort}`;

  return {
    server,
    url,
    port: actualPort,
    db,
    r2,
    storage: journalStorage,
    stats,
    close: async () => {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      db.close();
    },
  };
}

if (
  typeof process !== "undefined" &&
  process.argv[1] &&
  import.meta.url.endsWith(process.argv[1].replace(/\\/g, "/"))
) {
  const port = Number(process.env.PORT ?? "8788");
  const dbPath = process.env.ANYSERVE_DEV_SQLITE ?? ":memory:";
  const authSecret = process.env.JOURNAL_INTERNAL_SECRET;
  const handle = await startNodeJournalGatewayServer({
    port,
    dbPath,
    authSecret,
  });
  console.log(`Anyserve Cloudflare dev journal gateway listening at ${handle.url}`);
}
