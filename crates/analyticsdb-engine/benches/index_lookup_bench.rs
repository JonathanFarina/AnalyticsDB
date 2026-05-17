use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_index_key_parsing(c: &mut Criterion) {
    c.bench_function("index_key_parse_and_compare", |b| {
        let keys: Vec<String> = (0..1000).map(|i| format!("key_{}", i)).collect();

        b.iter(|| {
            let mut sorted = black_box(keys.clone());
            sorted.sort();
            black_box(sorted);
        })
    });
}

fn bench_index_lookup_simulation(c: &mut Criterion) {
    use std::collections::BTreeMap;

    let mut index: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for i in 0..1000 {
        index.insert(format!("key_{}", i), vec![i]);
    }

    c.bench_function("index_lookup_btree", |b| {
        b.iter(|| {
            let key = format!("key_{}", black_box(500));
            let _ = index.get(&key);
        })
    });
}

criterion_group!(
    benches,
    bench_index_key_parsing,
    bench_index_lookup_simulation
);
criterion_main!(benches);
