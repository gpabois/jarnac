use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use jarnac::bplus_tree::{BPlusTree, IRefBPlusTree};
use jarnac::prelude::*;

/// Benchmark la capacité de recherche d'un arbre B+
fn benchmark_b_plus_tree_search(c: &mut Criterion) {
    let pager = jarnac::pager::fixtures::fixture_new_pager();
    let mut tree = BPlusTree::new::<u64, u64>(&pager).expect("cannot create B+ tree");

    let size: u64 = 500;

    for i in 0..size {
        tree.insert(
            &i.into_value_buf(),
            &1234u64.into_value_buf()
        ).expect("cannot insert value");
    }

    let mut group = c.benchmark_group("BPlusTree::search");
    group.sample_size(100);
    for i in 0..size {
        group.bench_with_input(
            BenchmarkId::new("item", i), 
            &i, 
            |b, &i| {
                b.iter(|| {
                    tree.search(&i.into_value_buf()).unwrap();
                })
            }
        );
    }
    group.finish();
}

criterion_group!(benches, benchmark_b_plus_tree_search);
criterion_main!(benches);