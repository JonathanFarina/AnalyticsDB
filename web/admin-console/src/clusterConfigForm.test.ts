import { describe, expect, it } from "vitest";
import type { ClusterConfig } from "./adminClient";
import {
  DEFAULT_QUERY_LOG,
  buildSavePayload,
  configsEqual,
  isDirty,
  withDisplayDefaults,
} from "./clusterConfigForm";

const MINIMAL_CONFIG: ClusterConfig = {
  base_postgres_port: 5432,
  base_flight_sql_port: 50051,
  catalog_path: "cluster-catalog.db",
  tls_cert_path: "certs/server.crt",
  tls_key_path: "certs/server.key",
  next_available_port_offset: 1,
};

const CONFIG_WITH_QUERY_LOG: ClusterConfig = {
  ...MINIMAL_CONFIG,
  base_node_port: 60051,
  query_log: { ...DEFAULT_QUERY_LOG, sample_rate: 0.5 },
};

describe("isDirty", () => {
  it("returns false on initial load even when the file omits query_log", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    expect(isDirty(MINIMAL_CONFIG, draft)).toBe(false);
  });

  it("returns false when the file already had query_log and the form mirrors it", () => {
    const draft = withDisplayDefaults(CONFIG_WITH_QUERY_LOG);
    expect(isDirty(CONFIG_WITH_QUERY_LOG, draft)).toBe(false);
  });

  it("returns true once the user edits a top-level port", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.base_postgres_port = 5500;
    expect(isDirty(MINIMAL_CONFIG, draft)).toBe(true);
  });

  it("returns true when the user changes a query_log field", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.query_log!.sample_rate = 0.25;
    expect(isDirty(MINIMAL_CONFIG, draft)).toBe(true);
  });

  it("returns true when the user clears an optional TLS path", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.tls_cert_path = "";
    expect(isDirty(MINIMAL_CONFIG, draft)).toBe(true);
  });
});

describe("buildSavePayload", () => {
  it("omits query_log when the file did not have it and the user didn't touch query_log fields", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.base_postgres_port = 5500;
    const payload = buildSavePayload(draft, MINIMAL_CONFIG);
    expect(payload.query_log).toBeUndefined();
    expect(payload.base_postgres_port).toBe(5500);
  });

  it("includes query_log when the user changes a query_log field", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.query_log!.sample_rate = 0.25;
    const payload = buildSavePayload(draft, MINIMAL_CONFIG);
    expect(payload.query_log).toEqual({
      ...DEFAULT_QUERY_LOG,
      sample_rate: 0.25,
    });
  });

  it("preserves query_log when the file already had it", () => {
    const draft = withDisplayDefaults(CONFIG_WITH_QUERY_LOG);
    draft.base_postgres_port = 5500;
    const payload = buildSavePayload(draft, CONFIG_WITH_QUERY_LOG);
    expect(payload.query_log).toEqual(CONFIG_WITH_QUERY_LOG.query_log);
  });

  it("clamps query_log.sample_rate to the [0, 1] range", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.query_log!.sample_rate = -2;
    const payload = buildSavePayload(draft, MINIMAL_CONFIG);
    expect(payload.query_log?.sample_rate).toBe(0);
  });

  it("sends optional TLS paths as null when the user clears them", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    draft.tls_cert_path = "";
    draft.tls_key_path = "";
    const payload = buildSavePayload(draft, MINIMAL_CONFIG);
    expect(payload.tls_cert_path).toBeNull();
    expect(payload.tls_key_path).toBeNull();
  });

  it("sends optional jwt_secret as null when the user clears it", () => {
    const customConfig: ClusterConfig = {
      ...MINIMAL_CONFIG,
      jwt_secret: "secret-key",
    };
    const draft = withDisplayDefaults(customConfig);
    draft.jwt_secret = "";
    const payload = buildSavePayload(draft, customConfig);
    expect(payload.jwt_secret).toBeNull();
  });

  it("does not introduce jwt_secret when neither file nor draft had a value", () => {
    const sparse: ClusterConfig = {
      base_postgres_port: 5432,
      base_flight_sql_port: 50051,
      catalog_path: "cluster-catalog.db",
      next_available_port_offset: 0,
    };
    const draft = withDisplayDefaults(sparse);
    const payload = buildSavePayload(draft, sparse);
    expect(payload.jwt_secret).toBeUndefined();
    expect(configsEqual(sparse, payload)).toBe(true);
  });

  it("does not introduce TLS keys when neither file nor draft had a value", () => {
    const sparse: ClusterConfig = {
      base_postgres_port: 5432,
      base_flight_sql_port: 50051,
      catalog_path: "cluster-catalog.db",
      next_available_port_offset: 0,
    };
    const draft = withDisplayDefaults(sparse);
    const payload = buildSavePayload(draft, sparse);
    expect(payload.tls_cert_path).toBeUndefined();
    expect(payload.tls_key_path).toBeUndefined();
    expect(configsEqual(sparse, payload)).toBe(true);
  });

  it("sends base_node_port as null when the file had a value and the user cleared it", () => {
    const draft = withDisplayDefaults(CONFIG_WITH_QUERY_LOG);
    draft.base_node_port = undefined;
    const payload = buildSavePayload(draft, CONFIG_WITH_QUERY_LOG);
    expect(payload.base_node_port).toBeNull();
  });

  it("round-trips the loaded config when the user does not edit anything", () => {
    const draft = withDisplayDefaults(MINIMAL_CONFIG);
    const payload = buildSavePayload(draft, MINIMAL_CONFIG);
    expect(configsEqual(MINIMAL_CONFIG, payload)).toBe(true);
  });
});
