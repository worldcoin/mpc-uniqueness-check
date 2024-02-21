use std::fmt::Debug;

use bytemuck::{Pod, Zeroable};
use itertools::izip;
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use serde::{Deserialize, Serialize};

pub use crate::bits::Bits;
use crate::distance::ROTATIONS;

#[repr(C)]
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Debug,
    Default,
    Serialize,
    Deserialize,
    Pod,
    Zeroable,
)]
pub struct Template {
    pub code: Bits,
    pub mask: Bits,
}

impl Template {
    pub fn rotated(&self, amount: i32) -> Self {
        Self {
            code: self.code.rotated(amount),
            mask: self.mask.rotated(amount),
        }
    }

    pub fn rotations(&self) -> impl Iterator<Item = Template> + '_ {
        ROTATIONS.map(move |r| self.rotated(r))
    }

    pub fn distance(&self, other: &Self) -> f64 {
        self.rotations()
            .map(|r| r.fraction_hamming(other))
            .fold(f64::INFINITY, |a, b| a.min(b))
    }

    pub fn fraction_hamming(&self, other: &Self) -> f64 {
        let mut num = 0;
        let mut den = 0;

        for (ap, am, bp, bm) in izip!(
            self.code.0.iter(),
            self.mask.0.iter(),
            other.code.0.iter(),
            other.mask.0.iter()
        ) {
            let m = am & bm;
            let p = (ap ^ bp) & m;
            num += p.count_ones();
            den += m.count_ones();
        }

        // tmp_dist
        let d = (num as f64) / (den as f64);

        println!("d = {d}");

        d
    }
}

impl Distribution<Template> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Template {
        Template {
            code: rng.gen(),
            mask: rng.gen(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = indoc::indoc! { r#"{
        "code": "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",
        "mask":"/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////w=="
    }"# };

    const B: &str = indoc::indoc! { r#"{
        "code": "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",
        "mask":"/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////w=="
    }"# };

    #[test]
    fn distance_check() {
        let a: Template = serde_json::from_str(A).unwrap();
        let b: Template = serde_json::from_str(B).unwrap();

        let d = a.distance(&b);

        assert_eq!(d, 0.493125);
    }
}
