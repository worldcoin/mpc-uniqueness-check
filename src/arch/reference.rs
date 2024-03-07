#![allow(unused)]

use crate::distance::{Bits, ROTATIONS};
use crate::encoded_bits::EncodedBits;

pub fn distances(
    query_rotations: Vec<EncodedBits>,
    db: &[EncodedBits],
) -> impl Iterator<Item = [u16; 31]> + '_ {
    db.iter().map(move |entry| {
        let mut result = [0_u16; 31];
        for (d, r) in result.iter_mut().zip(0..=31) {
            *d = query_rotations[r].dot(entry);
        }
        result
    })
}

pub fn denominators<'a>(
    query: &'a Bits,
    db: &'a [Bits],
) -> impl Iterator<Item = [u16; 31]> + 'a {
    db.iter().map(|entry| {
        let mut result = [0_u16; 31];
        for (d, r) in result.iter_mut().zip(ROTATIONS) {
            *d = query.rotated(r).dot(entry);
        }
        result
    })
}

//TODO: move this to the benches file
#[cfg(feature = "bench")]
pub mod benches {
    use core::hint::black_box;

    use criterion::Criterion;
    use rand::{thread_rng, Rng};

    use super::*;

    pub fn group(c: &mut Criterion) {
        let mut rng = thread_rng();
        let mut g = c.benchmark_group("reference");

        g.bench_function("distances 31x1000", |bench| {
            let a: EncodedBits = rng.gen();
            let b: Box<[EncodedBits]> = (0..1000).map(|_| rng.gen()).collect();
            bench.iter(|| {
                black_box(distances(black_box(&a), black_box(&b)))
                    .for_each(|_| {})
            })
        });

        g.bench_function("denominators 31x1000", |bench| {
            let a: Bits = rng.gen();
            let b: Box<[Bits]> = (0..1000).map(|_| rng.gen()).collect();
            bench.iter(|| {
                black_box(denominators(black_box(&a), black_box(&b)))
                    .for_each(|_| {})
            })
        });
    }
}
