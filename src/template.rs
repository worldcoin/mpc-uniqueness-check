use std::fmt::Debug;

use bytemuck::{Pod, Zeroable};
use itertools::izip;
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use serde::{Deserialize, Serialize};

pub use crate::bits::Bits;

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
    pub fn rotate(&mut self, amount: i32) {
        self.mask.rotate(amount);
        self.code.rotate(amount);
    }

    pub fn rotated(&self, amount: i32) -> Self {
        let mut copy = *self;
        copy.rotate(amount);
        copy
    }

    pub fn distance(&self, other: &Self) -> f64 {
        (-15..=15)
            .map(|r| self.rotated(r).fraction_hamming(other))
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
        (num as f64) / (den as f64)
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
