use std::ops::RangeInclusive;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::arch;
pub use crate::bits::Bits;
pub use crate::encoded_bits::EncodedBits;
pub use crate::template::Template;

pub const COLS: usize = 200;
pub const ROWS: usize = 4 * 16;
pub const BITS: usize = ROWS * COLS;

pub const ROTATIONS: RangeInclusive<i32> = -15..=15;

/// Generate a [`EncodedBits`] such that values are $\{-1,0,1\}$, representing
/// unset, masked and set.
pub fn encode(template: &Template) -> EncodedBits {
    // Make sure masked-out pattern bits are zero;
    let pattern = &template.code & &template.mask;

    // Convert to u16s
    let pattern = EncodedBits::from(&pattern);
    let mask = EncodedBits::from(&template.mask);

    // Preprocessed is (mask - 2 * pattern)
    mask - &pattern - &pattern
}

pub struct DistanceEngine {
    rotations: Box<[EncodedBits; 31]>,
}

impl DistanceEngine {
    pub fn new(rotations: impl Iterator<Item = EncodedBits>) -> Self {
        Self {
            rotations: rotations.collect::<Box<[_]>>().try_into().unwrap(),
        }
    }

    #[tracing::instrument(skip(self, out, db), level = "debug")]
    pub fn batch_process(&self, out: &mut [[u16; 31]], db: &[EncodedBits]) {
        assert_eq!(out.len(), db.len());
        out.par_iter_mut()
            .zip(db.par_iter())
            .for_each(|(result, entry)| {
                // Compute dot product for each rotation
                for (d, rotation) in
                    result.iter_mut().zip(self.rotations.iter())
                {
                    *d = rotation.dot(entry);
                }
            });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Distance {
    pub distance: f64,
    pub serial_id: u64,
}

impl Distance {
    pub fn new(serial_id: u64, distance: f64) -> Self {
        Self {
            distance,
            serial_id,
        }
    }
}

impl Eq for Distance {}

impl PartialEq for Distance {
    fn eq(&self, other: &Self) -> bool {
        self.distance.eq(&other.distance) && self.serial_id.eq(&other.serial_id)
    }
}

/// Result data for a uniqueness check
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistanceResults {
    /// The lowest serial id known across all nodes
    pub serial_id: u64,
    /// The distances to the query
    pub matches: Vec<Distance>,
}

impl DistanceResults {
    pub fn new(serial_id: u64, matches: Vec<Distance>) -> Self {
        Self { serial_id, matches }
    }
}

pub struct MasksEngine {
    rotations: Box<[Bits; 31]>,
}

impl MasksEngine {
    pub fn new(query: &Bits) -> Self {
        let rotations =
            query.rotations().collect::<Box<[_]>>().try_into().unwrap();
        Self { rotations }
    }

    #[tracing::instrument(skip(self, out, db), level = "debug")]
    pub fn batch_process(&self, out: &mut [[u16; 31]], db: &[Bits]) {
        assert_eq!(out.len(), db.len());
        out.par_iter_mut()
            .zip(db.par_iter())
            .for_each(|(result, entry)| {
                // Compute dot product for each rotation
                for (d, rotation) in
                    result.iter_mut().zip(self.rotations.iter())
                {
                    *d = rotation.dot(entry);
                }
            });
    }
}

/// Compute encoded distances for each rotation, iterating over a database
pub fn distances<'a>(
    query: &'a Template,
    db: &'a [EncodedBits],
) -> impl Iterator<Item = [u16; 31]> + 'a {
    let query_rotations: Vec<_> =
        query.rotations().map(|query| encode(&query)).collect();

    arch::distances(query_rotations, db)
}

/// Compute the 31 rotated mask popcounts.
pub fn denominators<'a>(
    query: &'a Bits,
    db: &'a [Bits],
) -> impl Iterator<Item = [u16; 31]> + 'a {
    arch::denominators(query, db)
}

/// Decode a distances. Takes the minimum over the rotations
pub fn decode_distance(distances: &[u16; 31], denominators: &[u16; 31]) -> f64 {
    // TODO: Detect errors.
    // (d - n) must be an even number in range

    distances
        .iter()
        .zip(denominators.iter())
        .map(|(&n, &d)| (d.wrapping_sub(n) / 2, d))
        .map(|(n, d)| (n as f64) / (d as f64))
        .fold(f64::INFINITY, f64::min)
}

#[cfg(test)]
mod tests {
    use proptest::bits::u16;
    use rand::{thread_rng, Rng};

    use super::*;

    #[test]
    fn test_preprocess() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let entry = rng.gen();
            let encrypted = encode(&entry);
            for (i, v) in encrypted.0.iter().enumerate() {
                match *v {
                    u16::MAX => assert!(entry.mask[i] && entry.code[i]),
                    0 => assert!(!entry.mask[i]),
                    1 => assert!(entry.mask[i] && !entry.code[i]),
                    _ => panic!(),
                }
            }
        }
    }

    #[test]
    fn test_dotproduct() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let a = rng.gen();
            let b = rng.gen();
            let pre_a = encode(&a);
            let pre_b = encode(&b);

            let mut equal = 0;
            let mut uneq = 0;
            let mut denominator = 0;
            for i in 0..BITS {
                if a.mask[i] && b.mask[i] {
                    denominator += 1;
                    if a.code[i] == b.code[i] {
                        equal += 1;
                    } else {
                        uneq += 1;
                    }
                }
            }

            let sum = (pre_a * &pre_b).sum() as i16;
            assert_eq!(equal - uneq, sum);
            assert_eq!(equal + uneq, denominator);
            assert_eq!((denominator - sum) % 2, 0);
            assert_eq!(uneq, (denominator - sum) / 2);
        }
    }
}

// TODO: move this to benches
#[cfg(feature = "bench")]
pub mod benches {
    use criterion::Criterion;

    use super::*;

    pub fn group(c: &mut Criterion) {
        arch::benches::group(c);
    }
}
