use std::array;
use std::borrow::Cow;
use std::iter::{self, Sum};
use std::ops::{self, MulAssign};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytemuck::{cast_slice_mut, Pod, Zeroable};
use rand::distributions::{Distribution, Standard};
use rand::{thread_rng, Rng};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use crate::bits::{Bits, BITS};

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct EncodedBits(pub [u16; BITS]);

unsafe impl Zeroable for EncodedBits {}

unsafe impl Pod for EncodedBits {}

impl EncodedBits {
    /// Generate secret shares from this bitvector.
    pub fn share(&self, n: usize) -> Box<[EncodedBits]> {
        assert!(n > 0);

        // Create `n - 1` random shares.
        let mut rng = thread_rng();
        let mut result: Box<[EncodedBits]> =
            iter::repeat_with(|| rng.gen::<EncodedBits>())
                .take(n - 1)
                .chain(iter::once(EncodedBits([0_u16; BITS])))
                .collect();
        let (last, rest) = result.split_last_mut().unwrap();

        // Initialize last to sum of self
        *last = self - rest.iter().sum::<EncodedBits>();

        result
    }

    pub fn sum(&self) -> u16 {
        self.0.iter().copied().fold(0_u16, u16::wrapping_add)
    }

    pub fn dot(&self, other: &Self) -> u16 {
        self.0
            .iter()
            .zip(other.0.iter())
            .map(|(&a, &b)| u16::wrapping_mul(a, b))
            .fold(0_u16, u16::wrapping_add)
    }
}

impl Default for EncodedBits {
    fn default() -> Self {
        Self([0; BITS])
    }
}

impl From<&Bits> for EncodedBits {
    fn from(value: &Bits) -> Self {
        EncodedBits(array::from_fn(|i| if value[i] { 1 } else { 0 }))
    }
}

impl Distribution<EncodedBits> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> EncodedBits {
        let mut values = [0_u16; BITS];
        rng.fill_bytes(cast_slice_mut(values.as_mut_slice()));
        EncodedBits(values)
    }
}

impl ops::Neg for EncodedBits {
    type Output = EncodedBits;

    fn neg(mut self) -> Self::Output {
        for r in self.0.iter_mut() {
            *r = 0_u16.wrapping_sub(*r);
        }
        self
    }
}

impl ops::Neg for &EncodedBits {
    type Output = EncodedBits;

    fn neg(self) -> Self::Output {
        let mut result = *self;
        for r in result.0.iter_mut() {
            *r = 0_u16.wrapping_sub(*r);
        }
        result
    }
}

impl<'a> Sum<&'a EncodedBits> for EncodedBits {
    fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
        let mut result = Self::default();
        for i in iter {
            result += i;
        }
        result
    }
}

impl ops::Sub<EncodedBits> for &EncodedBits {
    type Output = EncodedBits;

    fn sub(self, mut rhs: EncodedBits) -> Self::Output {
        for (a, &b) in rhs.0.iter_mut().zip(self.0.iter()) {
            *a = b.wrapping_sub(*a);
        }
        rhs
    }
}

impl ops::Sub<&EncodedBits> for EncodedBits {
    type Output = EncodedBits;

    fn sub(mut self, rhs: &EncodedBits) -> Self::Output {
        self -= rhs;
        self
    }
}

impl ops::Mul for &EncodedBits {
    type Output = EncodedBits;

    fn mul(self, rhs: Self) -> Self::Output {
        let mut copy = *self;
        copy.mul_assign(rhs);
        copy
    }
}

impl ops::Mul<&EncodedBits> for EncodedBits {
    type Output = EncodedBits;

    fn mul(mut self, rhs: &EncodedBits) -> Self::Output {
        self.mul_assign(rhs);
        self
    }
}

impl ops::AddAssign<&EncodedBits> for EncodedBits {
    fn add_assign(&mut self, rhs: &EncodedBits) {
        for (s, &r) in self.0.iter_mut().zip(rhs.0.iter()) {
            *s = s.wrapping_add(r);
        }
    }
}

impl ops::SubAssign<&EncodedBits> for EncodedBits {
    fn sub_assign(&mut self, rhs: &EncodedBits) {
        for (s, &r) in self.0.iter_mut().zip(rhs.0.iter()) {
            *s = s.wrapping_sub(r);
        }
    }
}

impl ops::MulAssign<&EncodedBits> for EncodedBits {
    fn mul_assign(&mut self, rhs: &EncodedBits) {
        for (s, &r) in self.0.iter_mut().zip(rhs.0.iter()) {
            *s = s.wrapping_mul(r);
        }
    }
}

impl<'de> Deserialize<'de> for EncodedBits {
    fn deserialize<D>(deserializer: D) -> Result<EncodedBits, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Cow<'static, str> = Deserialize::deserialize(deserializer)?;

        let bytes = BASE64_STANDARD
            .decode(s.as_bytes())
            .map_err(D::Error::custom)?;

        let mut limbs = [0_u16; BITS];
        for (i, chunk) in bytes.array_chunks::<2>().enumerate() {
            limbs[i] = u16::from_be_bytes(*chunk);
        }

        Ok(Self(limbs))
    }
}

impl Serialize for EncodedBits {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut bytes = [0_u8; std::mem::size_of::<Self>()];

        for (i, limb) in self.0.iter().enumerate() {
            let limb_bytes = limb.to_be_bytes();
            bytes[i * 2..(i + 1) * 2].copy_from_slice(&limb_bytes);
        }

        let s = BASE64_STANDARD.encode(bytes);

        s.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_bits_serialization() {
        let mut encoded_bits = EncodedBits::default();

        // Random changes
        let mut rng = thread_rng();
        for _ in 0..100 {
            let index = rng.gen_range(0..encoded_bits.0.len());

            encoded_bits.0[index] = rng.gen();
        }

        let serialized = serde_json::to_string(&encoded_bits).unwrap();

        let deserialized = serde_json::from_str::<EncodedBits>(&serialized)
            .expect("Failed to deserialize EncodedBits");

        assert_eq!(deserialized, encoded_bits);
    }

    use crate::bits::tests::*;

    #[test]
    fn encoded_bits_deserialization_known_pattern() -> eyre::Result<()> {
        let unpacked_bits_u16_first_half_set_pattern =
            [[1_u16; 32], [0_u16; 32]].concat().repeat(BITS / 64);

        assert_eq!(unpacked_bits_u16_first_half_set_pattern.len(), BITS);

        let base64_code = format!(
            "\"{}\"",
            BASE64_STANDARD.encode(u16_slice_to_u8_vec(
                &unpacked_bits_u16_first_half_set_pattern
            ))
        );

        let deserialized = serde_json::from_str::<EncodedBits>(&base64_code)
            .expect("Failed to deserialize EncodedBits");

        assert_eq!(
            binary_string_unpacked_u16(&deserialized, false),
            FIRST_HALF_BITS_SET_PATTERN_U64_STR.repeat(BITS / 64)
        );

        Ok(())
    }

    #[test]
    fn encoded_bits_serialization_known_pattern() -> eyre::Result<()> {
        let bits: Bits = Bits(FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE);
        assert_eq!(bits.0.len(), BITS / 64); // BITS = 12800 /64 => 200

        let encoded_bits = EncodedBits::from(&bits);
        assert_eq!(encoded_bits.0.len(), BITS);

        let serialized = serde_json::to_string(&encoded_bits)?;

        let bits_u16_pattern =
            [[1_u16; 32], [0_u16; 32]].concat().repeat(BITS / 64);

        assert_eq!(bits_u16_pattern.len(), BITS);

        let base64_code = format!(
            "\"{}\"",
            BASE64_STANDARD.encode(u16_slice_to_u8_vec(&bits_u16_pattern))
        );

        assert_eq!(serialized, base64_code);

        Ok(())
    }

    #[test]
    fn unpack_from_bits_to_encoded_bits() -> eyre::Result<()> {
        let bits_u64 = Bits::from(FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES);
        let bits_u16_unpacked = EncodedBits::from(&bits_u64);

        print_binary_representation_u8(
            &FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES,
        );
        print_binary_representation_u64(&bits_u64.0);
        print_binary_u16_unpacked(&bits_u16_unpacked);
        assert_eq!(
            binary_string_u8(
                &FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES,
                false,
            ),
            binary_string_unpacked_u16(&bits_u16_unpacked, false)
        );
        print_binary_u16_unpacked(&bits_u16_unpacked);

        Ok(())
    }

    fn u16_slice_to_u8_vec(s: &[u16]) -> Vec<u8> {
        s.iter().flat_map(|x| x.to_be_bytes()).collect()
    }

    fn print_binary_u16_unpacked(bits: &EncodedBits) {
        println!("{}", binary_string_unpacked_u16(bits, false));
    }

    fn binary_string_unpacked_u16(
        bits: &EncodedBits,
        separated: bool,
    ) -> String {
        bits.0
            .iter()
            .map(|byte| {
                if *byte == 1 {
                    String::from("1")
                } else {
                    String::from("0")
                }
            })
            .collect::<Vec<String>>()
            .join(if separated { " " } else { "" })
    }
}
