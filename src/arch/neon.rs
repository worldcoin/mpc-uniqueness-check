#![cfg(target_feature = "neon")]
#![allow(unused)]

// Rust + LLVM already generates good NEON code for the generic implementation.

//TODO: move this to the benches file
#[cfg(feature = "bench")]
pub mod benches {
    use core::hint::black_box;

    use criterion::Criterion;
    use rand::{thread_rng, Rng};

    use super::*;

    pub fn group(c: &mut Criterion) {
        let mut g = c.benchmark_group("neon");
        let mut rng = thread_rng();
    }
}
