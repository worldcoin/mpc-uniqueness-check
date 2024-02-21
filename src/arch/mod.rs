mod generic; // Optimized generic implementation
mod reference; // Simple generic implementations

pub use generic::{denominators, distances};

//TODO: move this to the benches file
#[cfg(feature = "bench")]
pub mod benches {
    use criterion::Criterion;

    use super::*;

    pub fn group(c: &mut Criterion) {
        reference::benches::group(c);

        generic::benches::group(c);
    }
}
