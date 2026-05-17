use analyticsdb_core::{QueryRequest, SessionContext};
use analyticsdb_engine::query_log::QueryLog;
use criterion::{black_box, BenchmarkId, Criterion};

fn query_log_start_probe_benchmark(c: &mut Criterion) {
    let query_log = QueryLog::disabled();
    let session = SessionContext::default();
    let request = QueryRequest {
        sql: "SELECT 1".to_string(),
        session: session.clone(),
        query_id: Some("test-query-id".to_string()),
    };
    let admission = analyticsdb_control::QueryAdmission {
        query_id: "admission-id".to_string(),
        coordinator_node_id: "node-1".to_string(),
    };

    c.bench_function("query_log_start_probe", |b| {
        b.iter(|| {
            let probe = query_log.start_probe(
                black_box(&request),
                black_box(&admission),
                black_box("SELECT 1"),
            );
            black_box(probe);
        })
    });
}

fn query_log_observe_and_finish_benchmark(c: &mut Criterion) {
    let query_log = QueryLog::disabled();
    let session = SessionContext::default();
    let request = QueryRequest {
        sql: "SELECT 1".to_string(),
        session,
        query_id: Some("test-query-id".to_string()),
    };
    let admission = analyticsdb_control::QueryAdmission {
        query_id: "admission-id".to_string(),
        coordinator_node_id: "node-1".to_string(),
    };

    c.bench_function("query_log_observe_and_finish", |b| {
        b.iter(|| {
            let probe = query_log.start_probe(&request, &admission, "SELECT 1");
            probe.observe_plan(black_box(
                &datafusion::logical_expr::LogicalPlan::EmptyRelation(
                    datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: std::sync::Arc::new(datafusion::common::DFSchema::empty()),
                    },
                ),
            ));
            probe.observe_read(black_box(100), black_box(1024));
            probe.finish_result(&Ok(analyticsdb_engine::QueryExecutionResult {
                query_id: "test".to_string(),
                coordinator_node_id: "node-1".to_string(),
                session: SessionContext::default(),
                schema: std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty()),
                batches: vec![],
                message: "OK".to_string(),
                outcome: analyticsdb_core::StatementOutcome::Rows,
                execution_time_ms: 100,
            }));
        })
    });
}

criterion_group!(
    benches,
    query_log_start_probe_benchmark,
    query_log_observe_and_finish_benchmark
);
criterion_main!(benches);
