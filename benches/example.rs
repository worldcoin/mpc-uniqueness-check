use criterion::{criterion_group, criterion_main, Criterion};

pub fn example_benchmark(c: &mut Criterion) {
    c.bench_function("Example bench", |b| {
        b.iter(|| {
            // Benchmark logic here
        })
    });
}

criterion_group!(example, example_benchmark);
criterion_main!(example);
