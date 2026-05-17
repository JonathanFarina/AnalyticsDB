# OpenTelemetry Collector Deployment for AnalyticsDB

This document describes how to deploy an OpenTelemetry (OTel) Collector to receive traces from AnalyticsDB and export them to Jaeger and Prometheus.

## Prerequisites

- Docker and Docker Compose installed
- AnalyticsDB server running with OpenTelemetry dependencies enabled

## Docker Compose Setup

Create a `docker-compose.yml` file for the OTel Collector, Jaeger, and Prometheus:

```yaml
version: "3.8"

services:
  # OpenTelemetry Collector
  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.102.1
    container_name: analyticsdb-otel-collector
    command: ["--config=/etc/otel-collector-config.yaml"]
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    ports:
      - "4317:4317"   # OTLP gRPC receiver
      - "4318:4318"   # OTLP HTTP receiver
      - "8888:8888"   # Metrics endpoint
    depends_on:
      - jaeger
      - prometheus
    networks:
      - analyticsdb-otel

  # Jaeger for trace storage and visualization
  jaeger:
    image: jaegertracing/all-in-one:1.52.0
    container_name: analyticsdb-jaeger
    ports:
      - "16686:16686"  # Jaeger UI
      - "14250:14250"  # Jaeger gRPC (for OTel Collector)
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    networks:
      - analyticsdb-otel

  # Prometheus for metrics (optional, if collecting OTel metrics)
  prometheus:
    image: prom/prometheus:v2.48.1
    container_name: analyticsdb-prometheus
    volumes:
      - ./prometheus-config.yaml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"
    networks:
      - analyticsdb-otel

networks:
  analyticsdb-otel:
    driver: bridge
```

## OTel Collector Configuration

Create `otel-collector-config.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 1s
    send_batch_size: 1024

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true

  prometheus:
    endpoint: 0.0.0.0:8888
    namespace: analyticsdb

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [prometheus]
```

## Prometheus Configuration

Create `prometheus-config.yaml` (optional, if using Prometheus to scrape OTel Collector metrics):

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "otel-collector"
    static_configs:
      - targets: ["otel-collector:8888"]
```

## Configuring AnalyticsDB to Send Traces

AnalyticsDB uses the OpenTelemetry SDK to send traces via OTLP. Configure the following environment variables when starting the AnalyticsDB server:

| Variable | Description | Default |
|----------|-------------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint of the collector | `http://localhost:4317` |
| `OTEL_SERVICE_NAME` | Service name for traces (overrides default "analyticsdb") | `analyticsdb` |
| `OTEL_SERVICE_VERSION` | Service version (overrides default from CARGO_PKG_VERSION) | (from build) |
| `OTEL_RESOURCE_ATTRIBUTES` | Additional resource attributes (key=value pairs) | (none) |
| `OTEL_LOG_LEVEL` | OpenTelemetry SDK log level | `info` |

Example startup command:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
analyticsdb-server --postgres-addr 127.0.0.1:5432 --flight-sql-addr 127.0.0.1:8815
```

## Verifying Traces

1. Start the Docker Compose stack:
   ```bash
   docker compose up -d
   ```

2. Start AnalyticsDB with the OTEL environment variables as shown above.

3. Submit a query to AnalyticsDB via PostgreSQL or Flight SQL client.

4. Open the Jaeger UI at [http://localhost:16686](http://localhost:16686), select "analyticsdb" service, and click "Find Traces" to see the submitted query trace.

## Trace Structure

A single distributed query trace will contain:
- **Coordinator span**: Created at query admission in `execute_query_inner()` with `query_id` attribute
- **Worker spans**: Created when compute nodes execute `ExecutePartition` tasks, with propagated trace context
- **gRPC spans**: Automatic spans for gRPC calls between coordinator and workers (if enabled)

## Production Considerations

- For production, replace Jaeger all-in-one with a production-grade trace backend (e.g., Jaeger with Elasticsearch/OpenSearch, or a managed service like AWS X-Ray)
- Enable TLS for OTLP gRPC communication between AnalyticsDB and the collector
- Configure sampling (e.g., `OTEL_TRACES_SAMPLER=parentbased_traceidratio` with `OTEL_TRACES_SAMPLER_ARG=0.1` for 10% sampling)
- Monitor OTel Collector health using its metrics endpoint (`http://localhost:8888/metrics`)
