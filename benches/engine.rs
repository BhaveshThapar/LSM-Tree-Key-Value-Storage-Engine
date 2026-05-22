//! Microbenchmarks for the `lsm_kv` engine in isolation: write throughput,
//! hot/cold point reads, the Bloom-filter read win, and post-compaction reads.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use lsm_kv::{Db, Options};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn key(i: u64) -> Vec<u8> {
    format!("key{i:012}").into_bytes()
}

const VALUE: &[u8] = b"the-quick-brown-fox-jumps-over-payload-0123456789";

/// A database pre-loaded with `n` keys, flushed to SSTables on disk.
fn loaded_db(opts: Options, n: u64) -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open_with(dir.path(), opts).unwrap();
    for i in 0..n {
        db.put(&key(i), VALUE).unwrap();
    }
    db.flush().unwrap();
    (dir, db)
}

fn bench_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("write");
    group.throughput(Throughput::Elements(1));
    group.bench_function("put", |b| {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            sync_wal: false,
            ..Options::default()
        };
        let db = Db::open_with(dir.path(), opts).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            db.put(&key(i), VALUE).unwrap();
            i += 1;
        });
    });
    group.finish();
}

fn bench_point_reads(c: &mut Criterion) {
    let n = 100_000u64;
    let opts = Options {
        sync_wal: false,
        ..Options::default()
    };
    let (_dir, db) = loaded_db(opts, n);
    let mut rng = StdRng::seed_from_u64(42);

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("hot_point_read", |b| {
        b.iter(|| db.get(&key(rng.gen_range(0..n))).unwrap());
    });
    group.bench_function("negative_lookup_bloom_on", |b| {
        b.iter(|| db.get(&key(rng.gen_range(n..2 * n))).unwrap());
    });
    group.finish();
}

fn bench_bloom_off(c: &mut Criterion) {
    let n = 100_000u64;
    let opts = Options {
        sync_wal: false,
        bloom_enabled: false,
        ..Options::default()
    };
    let (_dir, db) = loaded_db(opts, n);
    let mut rng = StdRng::seed_from_u64(42);

    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("negative_lookup_bloom_off", |b| {
        b.iter(|| db.get(&key(rng.gen_range(n..2 * n))).unwrap());
    });
    group.finish();
}

fn bench_post_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("post_compaction_point_read", |b| {
        b.iter_batched(
            || {
                // Force several flushes so a compaction runs.
                let dir = tempfile::tempdir().unwrap();
                let opts = Options {
                    sync_wal: false,
                    compaction_threshold: 4,
                    ..Options::default()
                };
                let db = Db::open_with(dir.path(), opts).unwrap();
                for batch in 0..8u64 {
                    for i in 0..5_000u64 {
                        db.put(&key(batch * 5_000 + i), VALUE).unwrap();
                    }
                    db.flush().unwrap();
                }
                (dir, db)
            },
            |(_dir, db)| {
                for i in (0..40_000u64).step_by(97) {
                    db.get(&key(i)).unwrap();
                }
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_writes,
    bench_point_reads,
    bench_bloom_off,
    bench_post_compaction
);
criterion_main!(benches);
