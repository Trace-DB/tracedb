import {
  TraceDbClient,
  TraceDbRequestError,
  type BranchesResponse,
  type CompactResponse,
  type DatabasesResponse,
  type DeleteResponse,
  type EpochResponse,
  type GetRecordResponse,
  type GraphQlError,
  type GraphQlQueryRequest,
  type GraphQlResponse,
  type GraphQlSchemaResponse,
  type HealthResponse,
  type HybridExplain,
  type HybridQuery,
  type JsonObject,
  type JsonValue,
  type JobsResponse,
  type MetricsResponse,
  type PutBatchResponse,
  type QueryResponse,
  type RecordInput,
  type RecordPatchRequest,
  type RecordPutBatchRequest,
  type RecordScanOutput,
  type ReadyResponse,
  type RestoreRequest,
  type RestoreResponse,
  type SnapshotRequest,
  type SnapshotResponse,
  type TableSchema,
  type TraceQlQueryRequest,
  type TraceDbClientConfig,
  type TraceDbActorContext,
  type TraceDbFetch,
  type TraceDbFetchInit,
  type TraceDbRequestOptions,
} from "./client.ts";

export {
  TraceDbClient,
  TraceDbHttpError,
  TraceDbRequestError,
} from "./client.ts";
export type {
  BranchesResponse,
  CompactResponse,
  DatabasesResponse,
  DeleteResponse,
  EpochResponse,
  GetRecordResponse,
  GraphQlError,
  GraphQlQueryRequest,
  GraphQlResponse,
  GraphQlSchemaResponse,
  HealthResponse,
  HybridExplain,
  HybridQuery,
  HybridQueryRow,
  JsonObject,
  JsonValue,
  JobsResponse,
  MetricsResponse,
  PutBatchResponse,
  QueryResponse,
  ReadyResponse,
  RecordInput,
  RecordOutput,
  RecordPatchRequest,
  RecordPutBatchRequest,
  RecordScanOutput,
  RestoreRequest,
  RestoreResponse,
  SnapshotRequest,
  SnapshotResponse,
  TableSchema,
  TraceQlQueryRequest,
  TraceDbActorContext,
  TraceDbFetch,
  TraceDbFetchInit,
  TraceDbRequestOptions,
} from "./client.ts";

export type TraceDBConfig = Omit<TraceDbClientConfig, "baseUrl"> & {
  url?: string;
  baseUrl?: string;
  timeoutMs?: number;
  safeRetries?: number;
  idempotencyRetries?: number;
};

export type TraceDBEnv = Partial<
  Record<
    | "TRACEDB_URL"
    | "TRACEDB_TOKEN"
    | "TRACEDB_DATABASE_ID"
    | "TRACEDB_BRANCH_ID"
    | "TRACEDB_TIMEOUT_MS"
    | "TRACEDB_SAFE_RETRIES"
    | "TRACEDB_IDEMPOTENCY_RETRIES",
    string | undefined
  >
>;

export type TraceDBFromEnvOptions = Omit<
  TraceDBConfig,
  | "url"
  | "baseUrl"
  | "token"
  | "databaseId"
  | "branchId"
  | "timeoutMs"
  | "safeRetries"
  | "idempotencyRetries"
> & {
  env?: TraceDBEnv;
  url?: string;
  baseUrl?: string;
  token?: string;
  databaseId?: string;
  branchId?: string;
  timeoutMs?: number;
  safeRetries?: number;
  idempotencyRetries?: number;
};

export type TableRecordInput = {
  id: string;
  fields: JsonObject;
};

export type TableRowInput = JsonObject;

export type TraceDBInsertRowsOptions = TraceDbRequestOptions & {
  idField?: string;
};

export type TraceDBDeleteOptions = TraceDbRequestOptions & {
  tombstone?: string;
};

export type TraceDBQueryOptions = {
  explain?: boolean;
  freshness?: string;
};

export class TraceDB {
  private readonly transport: TraceDbClient;

  constructor(config: TraceDBConfig) {
    const baseUrl = config.baseUrl ?? config.url;
    if (baseUrl === undefined || baseUrl.trim().length === 0) {
      throw new TraceDbRequestError(
        "CONFIG",
        "url",
        "TraceDB requires config.url or config.baseUrl",
      );
    }
    const timeoutMs = validateTimeoutMs(config.timeoutMs, "timeoutMs");
    const safeRetries = validateNonNegativeInteger(
      config.safeRetries,
      "safeRetries",
    );
    const idempotencyRetries = validateNonNegativeInteger(
      config.idempotencyRetries,
      "idempotencyRetries",
    );
    const fetchImpl = fetchWithRetries(
      fetchWithTimeout(config.fetchImpl, timeoutMs),
      safeRetries ?? 0,
      idempotencyRetries ?? 0,
    );
    this.transport = new TraceDbClient({
      baseUrl,
      token: config.token,
      databaseId: config.databaseId,
      branchId: config.branchId,
      actorContext: config.actorContext,
      fetchImpl,
    });
  }

  static fromEnv(options: TraceDBFromEnvOptions = {}): TraceDB {
    const env = options.env ?? defaultTraceDBEnv();
    const url = options.baseUrl ?? options.url ?? env.TRACEDB_URL;
    if (url === undefined || url.trim().length === 0) {
      throw new TraceDbRequestError(
        "CONFIG",
        "TRACEDB_URL",
        "TraceDB.fromEnv requires TRACEDB_URL",
      );
    }
    const timeoutMs =
      options.timeoutMs === undefined
        ? parseTimeoutMsFromEnv(env.TRACEDB_TIMEOUT_MS)
        : validateTimeoutMs(options.timeoutMs, "timeoutMs");
    const safeRetries =
      options.safeRetries === undefined
        ? parseNonNegativeIntegerFromEnv(
            env.TRACEDB_SAFE_RETRIES,
            "TRACEDB_SAFE_RETRIES",
          )
        : validateNonNegativeInteger(options.safeRetries, "safeRetries");
    const idempotencyRetries =
      options.idempotencyRetries === undefined
        ? parseNonNegativeIntegerFromEnv(
            env.TRACEDB_IDEMPOTENCY_RETRIES,
            "TRACEDB_IDEMPOTENCY_RETRIES",
          )
        : validateNonNegativeInteger(
            options.idempotencyRetries,
            "idempotencyRetries",
          );
    return new TraceDB({
      url,
      token: options.token ?? env.TRACEDB_TOKEN,
      databaseId: options.databaseId ?? env.TRACEDB_DATABASE_ID,
      branchId: options.branchId ?? env.TRACEDB_BRANCH_ID,
      fetchImpl: options.fetchImpl,
      timeoutMs,
      safeRetries,
      idempotencyRetries,
    });
  }

  get client(): TraceDbClient {
    return this.transport;
  }

  async ready(options: TraceDbRequestOptions = {}) {
    return this.transport.ready(options);
  }

  async health(options: TraceDbRequestOptions = {}) {
    return this.transport.health(options);
  }

  async applySchema(schema: TableSchema, options: TraceDbRequestOptions = {}) {
    return this.transport.applySchema(schema, options);
  }

  async traceql(
    query: string,
    options: TraceDbRequestOptions = {},
  ): Promise<QueryResponse> {
    return this.traceqlRequest({ query }, options);
  }

  async traceqlRequest(
    request: TraceQlQueryRequest,
    options: TraceDbRequestOptions = {},
  ): Promise<QueryResponse> {
    return this.transport.traceql({ ...request }, options);
  }

  async graphql(query: string, options: TraceDbRequestOptions = {}): Promise<GraphQlResponse> {
    return this.graphqlRequest({ query }, options);
  }

  async graphqlRequest(
    request: GraphQlQueryRequest,
    options: TraceDbRequestOptions = {},
  ): Promise<GraphQlResponse> {
    return this.transport.graphql({ ...request }, options);
  }

  async boundedGraphql(
    query: string,
    options: TraceDbRequestOptions = {},
  ): Promise<QueryResponse> {
    return this.boundedGraphqlRequest({ query }, options);
  }

  async boundedGraphqlRequest(
    request: GraphQlQueryRequest,
    options: TraceDbRequestOptions = {},
  ): Promise<QueryResponse> {
    return this.transport.boundedGraphql({ ...request }, options);
  }

  async graphqlSchema(
    options: TraceDbRequestOptions = {},
  ): Promise<GraphQlSchemaResponse> {
    return this.transport.graphqlSchema(options);
  }

  async listDatabases(
    options: TraceDbRequestOptions = {},
  ): Promise<DatabasesResponse> {
    return this.transport.listDatabases(options);
  }

  async listBranches(
    options: TraceDbRequestOptions = {},
  ): Promise<BranchesResponse> {
    return this.transport.listBranches(options);
  }

  async publicSafeMetrics(
    options: TraceDbRequestOptions = {},
  ): Promise<MetricsResponse> {
    return this.transport.publicSafeMetrics(options);
  }

  async compact(options: TraceDbRequestOptions = {}): Promise<CompactResponse> {
    return this.transport.compact({}, options);
  }

  async snapshot(
    request: SnapshotRequest,
    options: TraceDbRequestOptions = {},
  ): Promise<SnapshotResponse> {
    return this.transport.snapshot(request, options);
  }

  async restore(
    request: RestoreRequest,
    options: TraceDbRequestOptions = {},
  ): Promise<RestoreResponse> {
    return this.transport.restore(request, options);
  }

  async listAdminJobs(
    options: TraceDbRequestOptions = {},
  ): Promise<JobsResponse> {
    return this.transport.listAdminJobs(options);
  }

  table(name: string): TraceDBTable {
    return new TraceDBTable(this.transport, name);
  }
}

export class TraceDBTable {
  private readonly transport: TraceDbClient;
  private readonly name: string;
  private readonly tenantId?: string;
  private readonly scanLimit: number;
  private readonly scanCursor?: string;

  constructor(
    transport: TraceDbClient,
    name: string,
    tenantId?: string,
    scanLimit = 100,
    scanCursor?: string,
  ) {
    this.transport = transport;
    this.name = name;
    this.tenantId = tenantId;
    this.scanLimit = scanLimit;
    this.scanCursor = scanCursor;
  }

  tenant(tenantId: string): TraceDBTable {
    return new TraceDBTable(
      this.transport,
      this.name,
      tenantId,
      this.scanLimit,
      this.scanCursor,
    );
  }

  limit(limit: number): TraceDBTable {
    return new TraceDBTable(
      this.transport,
      this.name,
      this.tenantId,
      limit,
      this.scanCursor,
    );
  }

  cursor(cursor: string): TraceDBTable {
    return new TraceDBTable(
      this.transport,
      this.name,
      this.tenantId,
      this.scanLimit,
      cursor,
    );
  }

  async insert(
    id: string,
    fields: JsonObject,
    options: TraceDbRequestOptions = {},
  ): Promise<EpochResponse> {
    return this.transport.putRecord(
      this.recordInput(id, fields, "POST", "/v1/records/put"),
      options,
    );
  }

  async insertBatch(
    records: TableRecordInput[],
    options: TraceDbRequestOptions = {},
  ): Promise<PutBatchResponse> {
    const tenantId = this.requiredTenantId("POST", "/v1/records/put-batch");
    const request: RecordPutBatchRequest = {
      records: records.map((record) =>
        this.recordInputWithTenant(record.id, record.fields, tenantId),
      ),
    };
    return this.transport.putBatch(request, options);
  }

  async insertRows(
    rows: TableRowInput[],
    options: TraceDBInsertRowsOptions = {},
  ): Promise<PutBatchResponse> {
    const { idField = "id", ...requestOptions } = options;
    if (idField.length === 0) {
      throw new TraceDbRequestError(
        "POST",
        "/v1/records/put-batch",
        "idField cannot be empty",
      );
    }
    const tenantId = this.requiredTenantId("POST", "/v1/records/put-batch");
    const request: RecordPutBatchRequest = {
      records: rows.map((row, index) => {
        const fields: JsonObject = { ...row };
        if (!Object.prototype.hasOwnProperty.call(fields, idField)) {
          throw new TraceDbRequestError(
            "POST",
            "/v1/records/put-batch",
            `row ${index} missing id field '${idField}'`,
          );
        }
        return this.recordInputWithTenant(
          String(fields[idField]),
          fields,
          tenantId,
        );
      }),
    };
    return this.transport.putBatch(request, requestOptions);
  }

  async patch(
    id: string,
    fields: JsonObject,
    options: TraceDbRequestOptions = {},
  ): Promise<EpochResponse> {
    const request: RecordPatchRequest = {
      table: this.name,
      tenant_id: this.requiredTenantId("POST", "/v1/records/patch"),
      id,
      fields: { ...fields },
    };
    return this.transport.patchRecord(request, options);
  }

  async get(
    id: string,
    options: TraceDbRequestOptions = {},
  ): Promise<GetRecordResponse> {
    return this.transport.getRecord(
      {
        table: this.name,
        tenant_id: this.requiredTenantId("POST", "/v1/records/get"),
        id,
      },
      options,
    );
  }

  async scan(options: TraceDbRequestOptions = {}): Promise<RecordScanOutput> {
    const request = {
      table: this.name,
      tenant_id: this.requiredTenantId("POST", "/v1/records/scan"),
      limit: this.scanLimit,
      ...(this.scanCursor === undefined ? {} : { cursor: this.scanCursor }),
    };
    return this.transport.scanRecords(request, options);
  }

  async delete(
    id: string,
    options: TraceDBDeleteOptions = {},
  ): Promise<DeleteResponse> {
    const { tombstone, ...requestOptions } = options;
    return this.transport.deleteRecord(
      {
        table: this.name,
        tenant_id: this.requiredTenantId("POST", "/v1/records/delete"),
        id,
        tombstone,
      },
      requestOptions,
    );
  }

  query(): TraceDBQueryBuilder {
    return new TraceDBQueryBuilder(this.transport, this.name, this.tenantId);
  }

  where(filters: JsonObject): TraceDBQueryBuilder {
    return this.query().where(filters);
  }

  whereEq(field: string, value: JsonValue): TraceDBQueryBuilder {
    return this.query().whereEq(field, value);
  }

  match(field: string, query: string): TraceDBQueryBuilder {
    return this.query().match(field, query);
  }

  near(field: string, vector: number[]): TraceDBQueryBuilder {
    return this.query().near(field, vector);
  }

  with(options: TraceDBQueryOptions): TraceDBQueryBuilder {
    return this.query().with(options);
  }

  all(): Promise<QueryResponse> {
    return this.query().all();
  }

  private recordInput(
    id: string,
    fields: JsonObject,
    method: "POST",
    path: string,
  ): RecordInput {
    return this.recordInputWithTenant(
      id,
      fields,
      this.requiredTenantId(method, path),
    );
  }

  private recordInputWithTenant(
    id: string,
    fields: JsonObject,
    tenantId: string,
  ): RecordInput {
    const recordFields: JsonObject = { ...fields };
    recordFields.id = recordFields.id ?? id;
    recordFields.tenant = recordFields.tenant ?? tenantId;
    return {
      table: this.name,
      id,
      tenant_id: tenantId,
      fields: recordFields,
    };
  }

  private requiredTenantId(method: "POST", path: string): string {
    if (this.tenantId !== undefined && this.tenantId.length > 0) {
      return this.tenantId;
    }
    throw new TraceDbRequestError(
      method,
      path,
      "table handle execution requires tenant(...)",
    );
  }
}

export class TraceDBQueryBuilder {
  private readonly transport: TraceDbClient;
  private readonly tableName: string;
  private readonly tenantId?: string;
  private readonly scalarEq: JsonObject;
  private readonly textField?: string;
  private readonly textQuery?: string;
  private readonly vectorField?: string;
  private readonly vectorQuery?: number[];
  private readonly topK: number;
  private readonly cursorToken?: string;
  private readonly freshness: string;
  private readonly explain: boolean;

  constructor(
    transport: TraceDbClient,
    tableName: string,
    tenantId?: string,
    scalarEq: JsonObject = {},
    textField?: string,
    textQuery?: string,
    vectorField?: string,
    vectorQuery?: number[],
    topK = 10,
    cursorToken?: string,
    freshness = "Strict",
    explain = true,
  ) {
    this.transport = transport;
    this.tableName = tableName;
    this.tenantId = tenantId;
    this.scalarEq = scalarEq;
    this.textField = textField;
    this.textQuery = textQuery;
    this.vectorField = vectorField;
    this.vectorQuery = vectorQuery;
    this.topK = topK;
    this.cursorToken = cursorToken;
    this.freshness = freshness;
    this.explain = explain;
  }

  tenant(tenantId: string): TraceDBQueryBuilder {
    return this.copy({ tenantId });
  }

  // Supports the public DX form: table.where({ tenant_id, status: "published" }).
  where(filters: JsonObject): TraceDBQueryBuilder {
    let tenantId = this.tenantId;
    const scalarEq: JsonObject = { ...this.scalarEq };
    for (const [key, value] of Object.entries(filters)) {
      if (key === "tenant_id" && typeof value === "string") {
        tenantId = value;
      } else {
        scalarEq[key] = value;
      }
    }
    return this.copy({ scalarEq, tenantId });
  }

  whereEq(field: string, value: JsonValue): TraceDBQueryBuilder {
    return this.copy({ scalarEq: { ...this.scalarEq, [field]: value } });
  }

  match(field: string, query: string): TraceDBQueryBuilder {
    return this.copy({ textField: field, textQuery: query });
  }

  near(field: string, vector: number[]): TraceDBQueryBuilder {
    return this.copy({ vectorField: field, vectorQuery: [...vector] });
  }

  with(options: TraceDBQueryOptions): TraceDBQueryBuilder {
    return this.copy({
      explain: options.explain ?? this.explain,
      freshness:
        options.freshness === undefined
          ? this.freshness
          : normalizeFreshness(options.freshness),
    });
  }

  limit(limit: number): TraceDBQueryBuilder {
    return this.copy({ topK: limit });
  }

  cursor(cursor: string): TraceDBQueryBuilder {
    return this.copy({ cursorToken: cursor });
  }

  async all(options: TraceDbRequestOptions = {}): Promise<QueryResponse> {
    return this.transport.query(this.toHybridQuery("/v1/query"), options);
  }

  async explainPlan(
    options: TraceDbRequestOptions = {},
  ): Promise<HybridExplain> {
    return this.transport.explain(this.toHybridQuery("/v1/explain"), options);
  }

  private copy(overrides: {
    tenantId?: string;
    scalarEq?: JsonObject;
    textField?: string;
    textQuery?: string;
    vectorField?: string;
    vectorQuery?: number[];
    topK?: number;
    cursorToken?: string;
    freshness?: string;
    explain?: boolean;
  }): TraceDBQueryBuilder {
    return new TraceDBQueryBuilder(
      this.transport,
      this.tableName,
      overrides.tenantId ?? this.tenantId,
      overrides.scalarEq ?? this.scalarEq,
      overrides.textField ?? this.textField,
      overrides.textQuery ?? this.textQuery,
      overrides.vectorField ?? this.vectorField,
      overrides.vectorQuery ?? this.vectorQuery,
      overrides.topK ?? this.topK,
      overrides.cursorToken ?? this.cursorToken,
      overrides.freshness ?? this.freshness,
      overrides.explain ?? this.explain,
    );
  }

  private requiredTenantId(method: "POST", path: string): string {
    if (this.tenantId !== undefined && this.tenantId.length > 0) {
      return this.tenantId;
    }
    throw new TraceDbRequestError(
      method,
      path,
      "query execution requires tenant(...) or where({ tenant_id })",
    );
  }

  private toHybridQuery(path: "/v1/query" | "/v1/explain"): HybridQuery {
    const tenantId = this.requiredTenantId("POST", path);
    return {
      table: this.tableName,
      tenant_id: tenantId,
      scalar_eq: this.scalarEq,
      text_field: this.textField,
      text: this.textQuery,
      vector_field: this.vectorField,
      vector: this.vectorQuery,
      top_k: this.topK,
      cursor: this.cursorToken,
      freshness: this.freshness,
      explain: this.explain,
    };
  }
}

function normalizeFreshness(freshness: string): string {
  const normalized = freshness.trim().toLowerCase();
  if (normalized === "strict") {
    return "Strict";
  }
  if (
    normalized === "lazy" ||
    normalized === "onread" ||
    normalized === "on_read" ||
    normalized === "allowstale" ||
    normalized === "allow_stale"
  ) {
    return "Lazy";
  }
  if (
    normalized === "allowdirty" ||
    normalized === "allow_dirty" ||
    normalized === "allow-dirty"
  ) {
    return "AllowDirty";
  }
  return freshness;
}

function defaultTraceDBEnv(): TraceDBEnv {
  const maybeProcess = globalThis as typeof globalThis & {
    process?: { env?: TraceDBEnv };
  };
  return maybeProcess.process?.env ?? {};
}

function parseTimeoutMsFromEnv(value: string | undefined): number | undefined {
  if (value === undefined || value.trim().length === 0) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new TraceDbRequestError(
      "CONFIG",
      "TRACEDB_TIMEOUT_MS",
      "TRACEDB_TIMEOUT_MS must be a positive number",
    );
  }
  return parsed;
}

function validateTimeoutMs(
  value: number | undefined,
  path: string,
): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isFinite(value) || value <= 0) {
    throw new TraceDbRequestError(
      "CONFIG",
      path,
      "timeoutMs must be a positive number",
    );
  }
  return value;
}

function parseNonNegativeIntegerFromEnv(
  value: string | undefined,
  path: string,
): number | undefined {
  if (value === undefined || value.trim().length === 0) {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new TraceDbRequestError(
      "CONFIG",
      path,
      `${path} must be a non-negative integer`,
    );
  }
  return parsed;
}

function validateNonNegativeInteger(
  value: number | undefined,
  path: string,
): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value < 0) {
    throw new TraceDbRequestError(
      "CONFIG",
      path,
      `${path} must be a non-negative integer`,
    );
  }
  return value;
}

function fetchWithTimeout(
  fetchImpl: TraceDbFetch | undefined,
  timeoutMs: number | undefined,
): TraceDbFetch | undefined {
  if (timeoutMs === undefined) {
    return fetchImpl;
  }
  const defaultFetch = (
    globalThis as typeof globalThis & { fetch?: TraceDbFetch }
  ).fetch;
  const resolvedFetch = fetchImpl ?? defaultFetch;
  if (typeof resolvedFetch !== "function") {
    return undefined;
  }
  return async (input: string, init: TraceDbFetchInit) => {
    if (init.signal !== undefined) {
      return resolvedFetch(input, init);
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await resolvedFetch(input, { ...init, signal: controller.signal });
    } finally {
      clearTimeout(timeout);
    }
  };
}

function fetchWithRetries(
  fetchImpl: TraceDbFetch | undefined,
  safeRetries: number,
  idempotencyRetries: number,
): TraceDbFetch | undefined {
  if (safeRetries === 0 && idempotencyRetries === 0) {
    return fetchImpl;
  }
  const defaultFetch = (
    globalThis as typeof globalThis & { fetch?: TraceDbFetch }
  ).fetch;
  const resolvedFetch = fetchImpl ?? defaultFetch;
  if (typeof resolvedFetch !== "function") {
    return undefined;
  }
  return async (input: string, init: TraceDbFetchInit) => {
    const attempts = retryAttemptCount(
      input,
      init,
      safeRetries,
      idempotencyRetries,
    );
    let lastError: unknown;
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      try {
        const response = await resolvedFetch(input, init);
        if (response.ok || response.status < 500 || attempt + 1 >= attempts) {
          return response;
        }
      } catch (error) {
        if (isCallerAbort(error, init) || attempt + 1 >= attempts) {
          throw error;
        }
        lastError = error;
      }
      await sleepBeforeRetry(attempt);
    }
    if (lastError !== undefined) {
      throw lastError;
    }
    throw new TraceDbRequestError(
      "CONFIG",
      "retry",
      "request retry loop exhausted",
    );
  };
}

function sleepBeforeRetry(attempt: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, retryDelayMs(attempt)));
}

function retryDelayMs(attempt: number): number {
  const base = Math.min(5_000, 100 * 2 ** Math.min(attempt, 30));
  const jitter = 0.75 + Math.random() * 0.5;
  return Math.min(5_000, Math.max(1, Math.round(base * jitter)));
}

function isCallerAbort(error: unknown, init: TraceDbFetchInit): boolean {
  const signal = init.signal as { aborted?: unknown } | undefined;
  return signal?.aborted === true && isAbortError(error);
}

function isAbortError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "name" in error &&
    (error as { name?: unknown }).name === "AbortError"
  );
}

function retryAttemptCount(
  input: string,
  init: TraceDbFetchInit,
  safeRetries: number,
  idempotencyRetries: number,
): number {
  if (isIdempotentRetryRequest(input, init) && hasIdempotencyKey(init)) {
    return idempotencyRetries + 1;
  }
  if (isRetrySafeRequest(input, init)) {
    return safeRetries + 1;
  }
  return 1;
}

function isRetrySafeRequest(input: string, init: TraceDbFetchInit): boolean {
  const path = requestPath(input);
  if (
    init.method === "GET" &&
    (path === "/v1/health" ||
      path === "/v1/ready" ||
      path === "/v1/graphql/schema")
  ) {
    return true;
  }
  if (init.method !== "POST") {
    return false;
  }
  if (
    path === "/v1/records/get" ||
    path === "/v1/records/scan" ||
    path === "/v1/query" ||
    path === "/v1/graphql/bounded" ||
    path === "/v1/explain"
  ) {
    return true;
  }
  if (path === "/v1/traceql") {
    return isTraceQlReadOnlyBody(init.body);
  }
  if (path === "/v1/graphql") {
    return isGraphQlReadOnlyBody(init.body);
  }
  return false;
}

function isTraceQlReadOnlyBody(body: string | undefined): boolean {
  const query = requestBodyQuery(body);
  if (query === undefined) {
    return false;
  }
  const command = traceQlCommand(query);
  if (command === undefined) {
    return true;
  }
  return [
    "RECORD GET",
    "GET",
    "RECORD SCAN",
    "SCAN",
    "QUERY",
    "EXPLAIN",
    "JOBS LIST",
  ].includes(command);
}

function traceQlCommand(query: string): string | undefined {
  const trimmed = query.trimStart();
  for (const command of [
    "SCHEMA APPLY",
    "RECORD PUT",
    "RECORD BATCH",
    "RECORD PATCH",
    "RECORD DELETE",
    "RECORD GET",
    "RECORD SCAN",
    "ADMIN COMPACT",
    "ADMIN SNAPSHOT",
    "ADMIN RESTORE",
    "JOBS LIST",
    "JOBS RUN",
    "EXPLAIN",
    "QUERY",
    "PUT",
    "BATCH",
    "PATCH",
    "DELETE",
    "GET",
    "SCAN",
    "COMPACT",
    "SNAPSHOT",
    "RESTORE",
  ]) {
    if (startsWithCommand(trimmed, command)) {
      return command;
    }
  }
  return undefined;
}

function startsWithCommand(input: string, command: string): boolean {
  return (
    input.slice(0, command.length).toUpperCase() === command &&
    (input.length === command.length || /\s/.test(input[command.length] ?? ""))
  );
}

function isGraphQlReadOnlyBody(body: string | undefined): boolean {
  const query = requestBodyQuery(body);
  if (query === undefined) {
    return false;
  }
  const field = graphQlRootField(query);
  return (
    field === "get" ||
    field === "scan" ||
    field === "query" ||
    field === "explain" ||
    field === "jobs"
  );
}

function graphQlRootField(query: string): string | undefined {
  const trimmed = query.trimStart();
  if (
    wordStartsWith(trimmed, "mutation") ||
    wordStartsWith(trimmed, "subscription")
  ) {
    return undefined;
  }
  let root: string;
  if (wordStartsWith(trimmed, "query")) {
    const start = trimmed.indexOf("{");
    if (start < 0) {
      return undefined;
    }
    root = trimmed.slice(start + 1);
  } else if (trimmed.startsWith("{")) {
    root = trimmed.slice(1);
  } else {
    return undefined;
  }
  const first = parseGraphQlName(root);
  if (first === undefined) {
    return undefined;
  }
  const rest = first.rest.trimStart();
  if (rest.startsWith(":")) {
    return parseGraphQlName(rest.slice(1))?.name;
  }
  return first.name;
}

function parseGraphQlName(
  input: string,
): { name: string; rest: string } | undefined {
  const match = /^\s*([_A-Za-z][_0-9A-Za-z]*)([\s\S]*)$/.exec(input);
  if (match === null) {
    return undefined;
  }
  return { name: match[1], rest: match[2] };
}

function wordStartsWith(input: string, word: string): boolean {
  const next = input[word.length];
  return (
    input.slice(0, word.length).toLowerCase() === word &&
    (next === undefined || !/[_0-9A-Za-z]/.test(next))
  );
}

function requestBodyQuery(body: string | undefined): string | undefined {
  if (body === undefined) {
    return undefined;
  }
  try {
    const parsed = JSON.parse(body) as unknown;
    if (
      parsed !== null &&
      typeof parsed === "object" &&
      !Array.isArray(parsed)
    ) {
      const query = (parsed as { query?: unknown }).query;
      return typeof query === "string" ? query : undefined;
    }
  } catch {
    return undefined;
  }
  return undefined;
}

function isIdempotentRetryRequest(
  input: string,
  init: TraceDbFetchInit,
): boolean {
  return (
    init.method === "POST" &&
    (requestPath(input) === "/v1/schema/apply" ||
      requestPath(input) === "/v1/insert" ||
      requestPath(input) === "/v1/records/put" ||
      requestPath(input) === "/v1/records/put-batch" ||
      requestPath(input) === "/v1/records/patch" ||
      requestPath(input) === "/v1/records/delete" ||
      requestPath(input) === "/v1/admin/compact" ||
      requestPath(input) === "/v1/admin/snapshot" ||
      requestPath(input) === "/v1/admin/restore" ||
      requestPath(input) === "/v1/graphql" ||
      requestPath(input) === "/v1/traceql")
  );
}

function hasIdempotencyKey(init: TraceDbFetchInit): boolean {
  const key = init.headers["Idempotency-Key"];
  return key !== undefined && key.length > 0;
}

function requestPath(input: string): string {
  try {
    return new URL(input).pathname;
  } catch {
    return input.split("?", 1)[0];
  }
}
