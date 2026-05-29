use criterion::{black_box, Criterion, criterion_group, criterion_main};
use analyticsdb_engine::sql_rewriter;

fn bench_sql_rewrite(c: &mut Criterion) {
    let sql_cases = vec![
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE id > 10",
        "SELECT COUNT(*) FROM orders GROUP BY status",
        "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
    ];

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("sql_rewrite_postgres_compatibility", |b| {
        b.iter(|| {
            for sql in &sql_cases {
                let _ = rt.block_on(black_box(sql_rewriter::rewrite_sql_for_postgres_compatibility(
                    black_box(sql),
                    black_box(&analyticsdb_control::ControlPlane::new_bootstrap()),
                    black_box(&analyticsdb_core::SessionContext::default()),
                )));
            }
        })
    });
}

criterion_group!(benches, bench_sql_rewrite);
criterion_main!(benches);
