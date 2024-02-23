use std::array;
use std::fmt::Debug;
use std::ops::{self, Index};

use bytemuck::Zeroable;

pub const BITS: usize = 128;
const LIMBS: usize = 2;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct SEncodedBits(pub [u16; BITS]);
impl SEncodedBits {
    pub fn share(&self, n: usize) -> Box<[SEncodedBits]> {
        assert!(n > 0);
        let mut rest = SEncodedBits::default();

        for n in 0..rest.0.len() {
            if n % 2 == 0 {
                rest.0[n] = 1;
            } else if n % 7 == 0 {
                rest.0[n] = 1;
            } else {
                rest.0[n] = 0;
            }
        }

        let last = &mut SEncodedBits::default();
        // Initialize last to sum of self
        *last = self - rest;

        let result = Box::new([rest, *last]);
        result
    }
}

impl ops::AddAssign<&SEncodedBits> for SEncodedBits {
    fn add_assign(&mut self, rhs: &SEncodedBits) {
        for (s, &r) in self.0.iter_mut().zip(rhs.0.iter()) {
            *s = s.wrapping_add(r);
        }
    }
}

impl ops::Add<&SEncodedBits> for SEncodedBits {
    type Output = SEncodedBits;

    fn add(mut self, rhs: &SEncodedBits) -> Self::Output {
        self += rhs;
        self
    }
}

impl ops::Sub<SEncodedBits> for &SEncodedBits {
    type Output = SEncodedBits;

    fn sub(self, mut rhs: SEncodedBits) -> Self::Output {
        for (a, &b) in rhs.0.iter_mut().zip(self.0.iter()) {
            *a = b.wrapping_sub(*a);
        }
        rhs
    }
}

impl Default for SEncodedBits {
    fn default() -> Self {
        Self([0; BITS])
    }
}

impl From<&SBits> for SEncodedBits {
    fn from(value: &SBits) -> Self {
        SEncodedBits(array::from_fn(|i| if value[i] { 1 } else { 0 }))
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SBits(pub [u64; LIMBS]);

impl SBits {}

unsafe impl Zeroable for SBits {}

impl Index<usize> for SBits {
    type Output = bool;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < BITS);
        let (limb, bit) = (index / 64, index % 64);
        let b = self.0[limb] & (1_u64 << bit) != 0;
        if b {
            &true
        } else {
            &false
        }
    }
}

impl Default for SBits {
    fn default() -> Self {
        Self([u64::MAX; LIMBS])
    }
}

impl Debug for SBits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for limb in self.0 {
            write!(f, "{limb:016x}")?;
        }
        Ok(())
    }
}

impl From<[u8; LIMBS * 8]> for SBits {
    fn from(bytes: [u8; LIMBS * 8]) -> Self {
        let mut limbs = [0_u64; LIMBS];
        for (i, chunk) in bytes.array_chunks::<8>().enumerate() {
            limbs[i] = u64::from_be_bytes(*chunk);
        }
        Self(limbs)
    }
}

impl TryFrom<&[u8]> for SBits {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != LIMBS * 8 {
            return Err(());
        }

        let mut limbs = [0_u64; LIMBS];
        for (i, chunk) in value.array_chunks::<8>().enumerate() {
            limbs[i] = u64::from_be_bytes(*chunk);
        }

        Ok(Self(limbs))
    }
}

impl TryFrom<Vec<u8>> for SBits {
    type Error = ();

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

fn reconstruct_shares(shares: &[SEncodedBits], n: usize) -> SEncodedBits {
    let mut reconstructed = shares[0].clone();
    for share in &shares[1..n] {
        reconstructed = reconstructed + share;
    }
    reconstructed
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn share_computation() {
        let bytes: [u8; 16] = [
            1, 2, 3, 4, 5, 8, 1, 1, // First 64-bit limb
            1, 1, 1, 0, 0, 1, 1, 1, //  Second 64-bit limb
        ];

        // Create a Bits object from the byte array
        let bits: SBits = SBits::from(bytes);
        for (idx, chunk) in bits.0.iter().enumerate() {
            println!("{:?}", chunk);
            println!("{:?}", idx);
        }

        println!("BITS          {:?}", bits);
        let eb = SEncodedBits::from(&bits); // Setup with static input

        println!("EBITS         {:?}", eb);
        let n = 2; // Number of shares
                   // Replace thread_rng() with mock_rng() in the share method or here
        let shares = eb.share(n);
        let ebr: SEncodedBits = reconstruct_shares(&shares, n);
        // println!("RECONSTRUCTED {:?}", ebr);
        println!("SHARES {:?}", shares);
        // Assert on the expected shares, determined by running with mock_rng
        assert_eq!(eb, ebr);
    }
}

/*
=== RUST
encodedBits [1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 1 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 1 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0]
=== RUN   TestSharesNew
encodedBits [1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 1 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 1 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0]
=== RUN   TestSharesOld
encodedBits [1 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 1 1 0 0 0 0 0 0 0 0 1 0 0 0 0 0 1 0 1 0 0 0 0 0 0 0 0 1 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0]
*/
