//! YCSB-style workloads comparing `lsm_kv` against RocksDB as a baseline.
//!
//! Workloads: A (50/50 read/write), B (95/5), C (100% read). Each runs under a
//! uniform and a Zipfian key distribution. One harness drives both engines so
//! the throughput numbers are directly comparable.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use lsm_kv::{Db, Options};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const N_KEYS: u64 = 20_000;
const N_OPS: usize = 20_000;
const VALUE: &[u8] = b"the-quick-brown-fox-jumps-over-payload-0123456789";

fn key(i: u64) -> Vec<u8> {
    format!("key{i:012}").into_bytes()
}

/// One operation in a generated workload.
#[derive(Clone, Copy)]
enum Op {
    Read(u64),
    Write(u64),
}

/// Build a `Zipf(s=1.0)` CDF over `n` keys for `O(log n)` skewed sampling.
fn zipf_cdf(n: u64) -> Vec<f64> {
    let mut cdf = Vec::with_capacity(n as usize);
    let mut acc = 0.0;
    for i in 1..=n {
        acc += 1.0 / i as f64;
        cdf.push(acc);
    }
    let total = acc;
    for v in &mut cdf {
        *v /= total;
    }
    cdf
}

fn sample_zipf(cdf: &[f64], rng: &mut StdRng) -> u64 {
    let u: f64 = rng.gen();
    cdf.partition_point(|&c| c < u) as u64
}

/// Generate `N_OPS` operations with the given read fraction and distribution.
fn workload(read_frac: f64, zipfian: bool, seed: u64) -> Vec<Op> {
    let mut rng = StdRng::seed_from_u64(seed);
    let cdf = if zipfian { zipf_cdf(N_KEYS) } else { Vec::new() };
    (0..N_OPS)
        .map(|_| {
            let k = if zipfian {
                sample_zipf(&cdf, &mut rng)
            } else {
                rng.gen_range(0..N_KEYS)
            };
            if rng.gen::<f64>() < read_frac {
                Op::Read(k)
            } else {
                Op::Write(k)
            }
        })
        .collect()
}

fn fresh_lsm() -> (tempfile::TempDir, Db) {
    let dir = tempfile::tempdir().unwrap();
    let opts = Options {
        sync_wal: false,
        ..Options::default()
    };
    let mut db = Db::open_with(dir.path(), opts).unwrap();
    for i in 0..N_KEYS {
        db.put(&key(i), VALUE).unwrap();
    }
    db.flush().unwrap();
    (dir, db)
}

fn fresh_rocks() -> (tempfile::TempDir, rocksdb::DB) {
    let dir = tempfile::tempdir().unwrap();
    let db = rocksdb::DB::open_default(dir.path()).unwrap();
    for i in 0..N_KEYS {
        db.put(key(i), VALUE).unwrap();
    }
    (dir, db)
}

fn run_lsm(db: &mut Db, ops: &[Op]) {
    for op in ops {
        match *op {
            Op::Read(k) => {
                std::hint::black_box(db.get(&key(k)).unwrap());
            }
            Op::Write(k) => db.put(&key(k), VALUE).unwrap(),
        }
    }
}

fn run_rocks(db: &rocksdb::DB, ops: &[Op]) {
    for op in ops {
        match *op {
            Op::Read(k) => {
                std::hint::black_box(db.get(key(k)).unwrap());
            }
            Op::Write(k) => db.put(key(k), VALUE).unwrap(),
        }
    }
}

fn bench_ycsb(c: &mut Criterion) {
    let workloads = [("A", 0.5), ("B", 0.95), ("C", 1.0)];
    let dists = [("uniform", false), ("zipfian", true)];

    for (seed, (wname, read_frac)) in workloads.into_iter().enumerate() {
        for (dname, zipf) in dists {
            let ops = workload(read_frac, zipf, seed as u64);
            let mut group = c.benchmark_group(format!("ycsb_{wname}_{dname}"));
            group.throughput(Throughput::Elements(N_OPS as u64));

            group.bench_function("lsm_kv", |b| {
                b.iter_batched(
                    fresh_lsm,
                    |(_dir, mut db)| run_lsm(&mut db, &ops),
                    BatchSize::PerIteration,
                );
            });
            group.bench_function("rocksdb", |b| {
                b.iter_batched(
                    fresh_rocks,
                    |(_dir, db)| run_rocks(&db, &ops),
                    BatchSize::PerIteration,
                );
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench_ycsb);
criterion_main!(benches);
