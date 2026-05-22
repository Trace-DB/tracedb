import {
  TraceDbClient,
  TraceDbRequestError,
  type DeleteResponse,
  type EpochResponse,
  type GetRecordResponse,
  type HybridQuery,
  type JsonObject,
  type JsonValue,
  type PutBatchResponse,
  type QueryResponse,
  type RecordInput,
  type RecordPutBatchRequest,
  type RecordScanOutput,
  type TableSchema,
  type TraceDbClientConfig,
  type TraceDbFetch,
  type TraceDbFetchInit,
  type TraceDbRequestOptions,
} from "./client.ts";

export { TraceDbClient, TraceDbHttpError, TraceDbRequestError } from "./client.ts";
export type {
  DeleteResponse,
  EpochResponse,
  GetRecordResponse,
  HealthResponse,
  HybridExplain,
  HybridQuery,
  HybridQueryRow,
  JsonObject,
  JsonValue,
  PutBatchResponse,
  QueryResponse,
  ReadyResponse,
  RecordInput,
  RecordOutput,
  RecordPutBatchRequest,
  RecordScanOutput,
  TableSchema,
  TraceDbFetch,
  TraceDbFetchInit,
  TraceDbRequestOptions,
} from "./client.ts";

export type TraceDBConfig = Omit<TraceDbClientConfig, "baseUrl"> & {
  url?: string;
  baseUrl?: string;
};

export type TableRecordInput = {
  id: string;
  fields: JsonObject;
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
      throw new Error("TraceDB requires config.url or config.baseUrl");
    }
    this.transport = new TraceDbClient({
      baseUrl,
      token: config.token,
      databaseId: config.databaseId,
      branchId: config.branchId,
      fetchImpl: config.fetchImpl,
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

  table(name: string): TraceDBTable {
    return new TraceDBTable(this.transport, name);
  }
}

export class TraceDBTable {
  private readonly transport: TraceDbClient;
  private readonly name: string;
  private readonly tenantId?: string;
  private readonly scanLimit: number;

  constructor(transport: TraceDbClient, name: string, tenantId?: string, scanLimit = 100) {
    this.transport = transport;
    this.name = name;
    this.tenantId = tenantId;
    this.scanLimit = scanLimit;
  }

  tenant(tenantId: string): TraceDBTable {
    return new TraceDBTable(this.transport, this.name, tenantId, this.scanLimit);
  }

  limit(limit: number): TraceDBTable {
    return new TraceDBTable(this.transport, this.name, this.tenantId, limit);
  }

  async insert(
    id: string,
    fields: JsonObject,
    options: TraceDbRequestOptions = {},
  ): Promise<EpochResponse> {
    return this.transport.putRecord(this.recordInput(id, fields, "POST", "/v1/records/put"), options);
  }

  async insertBatch(
    records: TableRecordInput[],
    options: TraceDbRequestOptions = {},
  ): Promise<PutBatchResponse> {
    const tenantId = this.requiredTenantId("POST", "/v1/records/put-batch");
    const request: RecordPutBatchRequest = {
      records: records.map((record) => this.recordInputWithTenant(record.id, record.fields, tenantId)),
    };
    return this.transport.putBatch(request, options);
  }

  async get(id: string, options: TraceDbRequestOptions = {}): Promise<GetRecordResponse> {
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
    return this.transport.scanRecords(
      {
        table: this.name,
        tenant_id: this.requiredTenantId("POST", "/v1/records/scan"),
        limit: this.scanLimit,
      },
      options,
    );
  }

  async delete(id: string, options: TraceDBDeleteOptions = {}): Promise<DeleteResponse> {
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
    return this.recordInputWithTenant(id, fields, this.requiredTenantId(method, path));
  }

  private recordInputWithTenant(id: string, fields: JsonObject, tenantId: string): RecordInput {
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
    throw new TraceDbRequestError(method, path, "table handle execution requires tenant(...)");
  }
}

export class TraceDBQueryBuilder {
  private readonly transport: TraceDbClient;
  private readonly tableName: string;
  private readonly tenantId?: string;
  private readonly scalarEq: JsonObject;
  private readonly textQuery?: string;
  private readonly vectorQuery?: number[];
  private readonly topK: number;
  private readonly freshness: string;
  private readonly explain: boolean;

  constructor(
    transport: TraceDbClient,
    tableName: string,
    tenantId?: string,
    scalarEq: JsonObject = {},
    textQuery?: string,
    vectorQuery?: number[],
    topK = 10,
    freshness = "Strict",
    explain = true,
  ) {
    this.transport = transport;
    this.tableName = tableName;
    this.tenantId = tenantId;
    this.scalarEq = scalarEq;
    this.textQuery = textQuery;
    this.vectorQuery = vectorQuery;
    this.topK = topK;
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

  match(_field: string, query: string): TraceDBQueryBuilder {
    return this.copy({ textQuery: query });
  }

  near(_field: string, vector: number[]): TraceDBQueryBuilder {
    return this.copy({ vectorQuery: [...vector] });
  }

  with(options: TraceDBQueryOptions): TraceDBQueryBuilder {
    return this.copy({
      explain: options.explain ?? this.explain,
      freshness:
        options.freshness === undefined ? this.freshness : normalizeFreshness(options.freshness),
    });
  }

  limit(limit: number): TraceDBQueryBuilder {
    return this.copy({ topK: limit });
  }

  async all(options: TraceDbRequestOptions = {}): Promise<QueryResponse> {
    const path = "/v1/query";
    const tenantId = this.requiredTenantId("POST", path);
    const query: HybridQuery = {
      table: this.tableName,
      tenant_id: tenantId,
      scalar_eq: this.scalarEq,
      text: this.textQuery,
      vector: this.vectorQuery,
      top_k: this.topK,
      freshness: this.freshness,
      explain: this.explain,
    };
    return this.transport.query(query, options);
  }

  private copy(overrides: {
    tenantId?: string;
    scalarEq?: JsonObject;
    textQuery?: string;
    vectorQuery?: number[];
    topK?: number;
    freshness?: string;
    explain?: boolean;
  }): TraceDBQueryBuilder {
    return new TraceDBQueryBuilder(
      this.transport,
      this.tableName,
      overrides.tenantId ?? this.tenantId,
      overrides.scalarEq ?? this.scalarEq,
      overrides.textQuery ?? this.textQuery,
      overrides.vectorQuery ?? this.vectorQuery,
      overrides.topK ?? this.topK,
      overrides.freshness ?? this.freshness,
      overrides.explain ?? this.explain,
    );
  }

  private requiredTenantId(method: "POST", path: string): string {
    if (this.tenantId !== undefined && this.tenantId.length > 0) {
      return this.tenantId;
    }
    throw new TraceDbRequestError(method, path, "query execution requires tenant(...) or where({ tenant_id })");
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
  return freshness;
}
