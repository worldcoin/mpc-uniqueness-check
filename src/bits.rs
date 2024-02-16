use std::borrow::Cow;
use std::fmt::Debug;
use std::ops;
use std::ops::Index;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytemuck::{cast_slice_mut, try_cast_slice_mut, Pod, Zeroable};
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};

pub const COLS: usize = 200;
pub const ROWS: usize = 4 * 16;
pub const BITS: usize = ROWS * COLS;
const LIMBS: usize = BITS / 64;
const BYTES_PER_COL: usize = COLS / 8;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bits(pub [u64; LIMBS]);

impl Bits {
    pub fn rotate(&mut self, amount: i32) {
        let bytes: &mut [u8] =
            try_cast_slice_mut(self.0.as_mut_slice()).unwrap();
        for chunk in bytes.chunks_exact_mut(BYTES_PER_COL) {
            rotate_row(chunk.try_into().unwrap(), amount)
        }
    }

    pub fn rotated(&self, amount: i32) -> Self {
        let mut copy = *self;
        copy.rotate(amount);
        copy
    }

    pub fn count_ones(&self) -> u16 {
        self.0.iter().map(|n| n.count_ones() as u16).sum()
    }

    pub fn dot(&self, other: &Self) -> u16 {
        self.0
            .iter()
            .zip(other.0.iter())
            .map(|(&a, &b)| (a & b).count_ones() as u16)
            .sum()
    }
}

unsafe impl Zeroable for Bits {}

unsafe impl Pod for Bits {}

impl Index<usize> for Bits {
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

impl Default for Bits {
    fn default() -> Self {
        Self([0; LIMBS])
    }
}

impl Debug for Bits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for limb in self.0 {
            write!(f, "{limb:016x}")?;
        }
        Ok(())
    }
}

impl Serialize for Bits {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = [0_u8; std::mem::size_of::<Self>()];

        for (i, limb) in self.0.iter().enumerate() {
            let limb_bytes = limb.to_be_bytes();
            bytes[i * 8..(i + 1) * 8].copy_from_slice(&limb_bytes);
        }

        let s = BASE64_STANDARD.encode(bytes);

        s.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Bits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: Cow<'static, str> = Deserialize::deserialize(deserializer)?;

        let bytes = BASE64_STANDARD
            .decode(s.as_bytes())
            .map_err(D::Error::custom)?;

        let mut limbs = [0_u64; LIMBS];
        for (i, chunk) in bytes.array_chunks::<8>().enumerate() {
            limbs[i] = u64::from_be_bytes(*chunk);
        }

        Ok(Self(limbs))
    }
}

impl Distribution<Bits> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Bits {
        let mut values = [0_u64; LIMBS];
        rng.fill_bytes(cast_slice_mut(values.as_mut_slice()));
        Bits(values)
    }
}

impl ops::Not for &Bits {
    type Output = Bits;

    fn not(self) -> Self::Output {
        let mut result = Bits::default();
        for (r, s) in result.0.iter_mut().zip(self.0.iter()) {
            *r = !s;
        }
        result
    }
}

impl ops::BitAnd for &Bits {
    type Output = Bits;

    fn bitand(self, rhs: Self) -> Self::Output {
        let mut result = *self;
        result &= rhs;
        result
    }
}

impl ops::BitAnd<&Bits> for Bits {
    type Output = Bits;

    fn bitand(mut self, rhs: &Self) -> Self::Output {
        self &= rhs;
        self
    }
}

impl ops::BitOr for &Bits {
    type Output = Bits;

    fn bitor(self, rhs: Self) -> Self::Output {
        let mut result = *self;
        result |= rhs;
        result
    }
}

impl ops::BitXor for &Bits {
    type Output = Bits;

    fn bitxor(self, rhs: Self) -> Self::Output {
        let mut result = *self;
        result ^= rhs;
        result
    }
}

impl ops::BitAndAssign<&Bits> for Bits {
    fn bitand_assign(&mut self, rhs: &Self) {
        for (s, r) in self.0.iter_mut().zip(rhs.0.iter()) {
            s.bitand_assign(r);
        }
    }
}

impl ops::BitOrAssign<&Bits> for Bits {
    fn bitor_assign(&mut self, rhs: &Self) {
        for (s, r) in self.0.iter_mut().zip(rhs.0.iter()) {
            s.bitor_assign(r);
        }
    }
}

impl ops::BitXorAssign<&Bits> for Bits {
    fn bitxor_assign(&mut self, rhs: &Self) {
        for (s, r) in self.0.iter_mut().zip(rhs.0.iter()) {
            s.bitxor_assign(r);
        }
    }
}

fn rotate_row(a: &mut [u8; BYTES_PER_COL], mut amount: i32) {
    if amount <= -8 {
        a.rotate_left((amount.unsigned_abs() as usize) / 8);
        amount %= 8;
    } else if amount >= 8 {
        a.rotate_right((amount as usize) / 8);
        amount %= 8;
    }
    if amount < 0 {
        let r = amount.abs();
        let l = 8 - r;
        let mut carry = a[0] << l;
        for b in a.iter_mut().rev() {
            let old = *b;
            *b = (old >> r) | carry;
            carry = old << l;
        }
    } else if amount > 0 {
        let l = amount.abs();
        let r = 8 - l;
        let mut carry = a[24] >> r;
        for b in a.iter_mut() {
            let old = *b;
            *b = (old << l) | carry;
            carry = old >> r;
        }
    }
}

#[cfg(test)]
mod tests {
    use bytemuck::bytes_of;
    use rand::{thread_rng, Rng};

    use super::*;

    #[test]
    fn limbs_exact() {
        assert_eq!(LIMBS * 64, BITS);
        assert_eq!(BYTES_PER_COL * 8, COLS);
    }

    // #[test]
    // fn random_bits() {
    //     let mut rng = thread_rng();
    //     let bits: Bits = rng.gen();

    //     println!("bits num bytes = {}", BITS / 8);
    //     println!("{:?}", bits);
    // }

    #[test]
    fn test_index() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let bits: Bits = rng.gen();
            for location in 0..BITS {
                let actual = bits[location];

                let (byte, bit) = (location / 8, location % 8);
                let expected = bytes_of(&bits)[byte] & (1_u8 << bit) != 0;

                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn test_rotated_inverse() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let bits: Bits = rng.gen();
            for amount in -15..=15 {
                assert_eq!(
                    bits.rotated(amount).rotated(-amount),
                    bits,
                    "Rotation failed for {amount}"
                )
            }
        }
    }

    #[test]
    fn bits_serialization() -> eyre::Result<()> {
        let mut bits = Bits::default();

        // Random changes so that we don't convert all zeros
        let mut rng = thread_rng();
        for _ in 0..100 {
            let index = rng.gen_range(0..bits.0.len());

            bits.0[index] = rng.gen();
        }

        let serialized = serde_json::to_string(&bits)?;

        let deserialized: Bits = serde_json::from_str(&serialized)?;

        assert_eq!(bits, deserialized);

        Ok(())
    }
}
