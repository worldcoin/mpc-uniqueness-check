use std::borrow::Cow;
use std::fmt::{self, Debug};
use std::ops::{self, Index};

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bitvec::prelude::*;
use bytemuck::{cast_slice_mut, Pod, Zeroable};
use rand::distributions::{Distribution, Standard};
use rand::Rng;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use crate::distance::{EncodedBits, ROTATIONS};

mod all_bit_patterns_test;

pub const COLS: usize = 200;
pub const STEP_MULTI: usize = 4;
pub const ROWS: usize = 4 * 16;
pub const BITS: usize = ROWS * COLS;
pub const LIMBS: usize = BITS / 64;

#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bits(pub [u64; LIMBS]);

impl Bits {
    pub const ZERO: Self = Self([0; LIMBS]);

    /// Returns an unordered iterator over the 31 possible rotations
    pub fn rotations(&self) -> impl Iterator<Item = Self> + '_ {
        ROTATIONS.map(|rot| {
            let mut x = *self;

            if rot < 0 {
                x.rotate_left(rot.unsigned_abs() as usize * 4)
            } else {
                x.rotate_right(rot as usize * 4)
            }

            x
        })
    }

    pub fn rotate_right(&mut self, by: usize) {
        BitSlice::<_, Msb0>::from_slice_mut(&mut self.0)
            .chunks_exact_mut(COLS * 4)
            .for_each(|chunk| chunk.rotate_right(by));
    }

    pub fn rotate_left(&mut self, by: usize) {
        BitSlice::<_, Msb0>::from_slice_mut(&mut self.0)
            .chunks_exact_mut(COLS * 4)
            .for_each(|chunk| chunk.rotate_left(by));
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

    pub fn get(&self, index: usize) -> bool {
        assert!(index < BITS);
        let (limb, bit) = (index / 64, index % 64);
        self.0[limb] & (1_u64 << (63 - bit)) != 0
    }

    pub fn set(&mut self, index: usize, value: bool) {
        assert!(index < BITS);
        let (limb, bit) = (index / 64, index % 64);
        if value {
            self.0[limb] |= 1_u64 << (63 - bit);
        } else {
            self.0[limb] &= !(1_u64 << (63 - bit));
        }
    }
}

unsafe impl Zeroable for Bits {}

unsafe impl Pod for Bits {}

impl Index<usize> for Bits {
    type Output = bool;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < BITS);
        let (limb, bit) = (index / 64, index % 64);
        let b = self.0[limb] & (1_u64 << (63 - bit)) != 0;
        if b {
            &true
        } else {
            &false
        }
    }
}

impl Default for Bits {
    fn default() -> Self {
        Self([u64::MAX; LIMBS])
    }
}

impl Debug for Bits {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            for limb in self.0 {
                write!(f, "{limb:016x}")?;
            }
        } else {
            let s = serde_json::to_string(self).unwrap();
            let s = s.trim_matches('"');
            write!(f, "{}", s)?;
        }
        Ok(())
    }
}

impl From<&EncodedBits> for Bits {
    fn from(value: &EncodedBits) -> Self {
        let mut bits = [0_u64; LIMBS];
        for i in 0..BITS {
            let limb = i / 64;
            let bit = i % 64;
            if value.0[i] == 1 {
                bits[limb] |= 1_u64 << (63 - bit);
            }
        }
        Bits(bits)
    }
}

impl Serialize for Bits
where
    [(); std::mem::size_of::<Self>()]:,
{
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

        s.parse().map_err(D::Error::custom)
    }
}

impl From<[u8; LIMBS * 8]> for Bits {
    fn from(bytes: [u8; LIMBS * 8]) -> Self {
        let mut limbs = [0_u64; LIMBS];
        for (i, chunk) in bytes.array_chunks::<8>().enumerate() {
            limbs[i] = u64::from_be_bytes(*chunk);
        }
        Self(limbs)
    }
}

impl From<Bits> for [u8; LIMBS * 8] {
    fn from(bits: Bits) -> Self {
        let mut bytes = [0_u8; LIMBS * 8];
        for (i, limb) in bits.0.iter().enumerate() {
            let limb_bytes = limb.to_be_bytes();
            bytes[i * 8..(i + 1) * 8].copy_from_slice(&limb_bytes);
        }
        bytes
    }
}

impl TryFrom<&[u8]> for Bits {
    type Error = base64::DecodeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != LIMBS * 8 {
            return Err(base64::DecodeError::InvalidLength);
        }

        let mut limbs = [0_u64; LIMBS];
        for (i, chunk) in value.array_chunks::<8>().enumerate() {
            limbs[i] = u64::from_be_bytes(*chunk);
        }

        Ok(Self(limbs))
    }
}

impl TryFrom<Vec<u8>> for Bits {
    type Error = base64::DecodeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

fn u64_slice_to_u8_vec(s: &[u64]) -> Vec<u8> {
    s.iter().flat_map(|x| x.to_be_bytes()).collect()
}

impl fmt::Display for Bits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = u64_slice_to_u8_vec(&self.0);
        let s = BASE64_STANDARD.encode(bytes);
        write!(f, "{}", s)
    }
}

impl std::str::FromStr for Bits {
    type Err = base64::DecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = BASE64_STANDARD.decode(s)?;

        Self::try_from(bytes)
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

#[cfg(test)]
pub mod tests {
    use rand::thread_rng;

    use super::*;

    // u8 - 10101010
    pub const ODD_BITS_SET_PATTERN_SINGLE_BYTE_STR: &str = "10101010";
    pub const ODD_BITS_SET_PATTERN_SINGLE_BYTE: [u8; 1] = [170_u8; 1];
    pub const ODD_BITS_SET_PATTERN_SINGLE_BYTE_4_BYTES: [u8; 4] = [170_u8; 4];
    pub const ODD_BITS_SET_PATTERN_IRIS_CODE_BYTES: [u8; LIMBS * 8] =
        [170_u8; LIMBS * 8];

    // u8 - 11110000
    pub const FIRST_HALF_BITS_SET_PATTERN_BYTE_STR: &str = "11110000";
    pub const FIRST_HALF_BITS_SET_PATTERN_BYTE: [u8; 1] = [240_u8; 1];
    pub const FIRST_HALF_BITS_SET_PATTERN_4_BYTES: [u8; 4] = [240_u8; 4];
    pub const FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES: [u8; LIMBS * 8] =
        [240_u8; LIMBS * 8];

    // u64 - 1111111111111111111111111111111100000000000000000000000000000000
    pub const FIRST_HALF_BITS_SET_PATTERN_U64_STR: &str =
        "1111111111111111111111111111111100000000000000000000000000000000";
    pub const FIRST_HALF_BITS_SET_PATTERN_U64_SINGLE: [u64; 1] =
        [18446744069414584320_u64; 1];
    pub const FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE: [u64; LIMBS] =
        [18446744069414584320_u64; LIMBS];

    #[test]
    fn limbs_exact() {
        assert_eq!(LIMBS * 64, BITS);
    }

    #[test]
    fn test_index_random_pattern() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let bits: Bits = rng.gen();
            for location in 0..BITS {
                let actual = bits[location];
                let (byte, bit) = (location / 8, location % 8);
                let bytes = u64_slice_to_u8_vec(&bits.0);
                let expected = (bytes[byte] & (1_u8 << (7 - bit))) != 0;

                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn test_index_known_pattern() {
        let bits = Bits(FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE);

        for location in 0..BITS {
            let actual = bits[location];
            let (byte, bit) = (location / 8, location % 8);
            let bytes = u64_slice_to_u8_vec(&bits.0);
            let expected = bytes[byte] & (1_u8 << (7 - bit)) != 0;

            assert_eq!(actual, expected);
        }
    }

    fn u64_slice_to_u8_vec(s: &[u64]) -> Vec<u8> {
        s.iter().flat_map(|x| x.to_be_bytes()).collect()
    }

    #[test]
    fn test_rotated_inverse() {
        let mut rng = thread_rng();
        let bits: Bits = rng.gen();
        let mut other = bits;
        other.rotate_left(1);
        other.rotate_right(1);

        assert_eq!(bits, other)
    }

    #[test]
    fn bits_serialization_random_pattern() -> eyre::Result<()> {
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

    #[test]
    fn bits_deserialization_known_pattern() -> eyre::Result<()> {
        let bytes_code =
            u64_slice_to_u8_vec(&FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE);
        let base64_code = format!("\"{}\"", BASE64_STANDARD.encode(bytes_code));

        let deserialized: Bits = serde_json::from_str(&base64_code)?;

        assert_eq!(
            binary_string_u64(&deserialized.0, false),
            FIRST_HALF_BITS_SET_PATTERN_U64_STR.repeat(LIMBS)
        );

        Ok(())
    }

    #[test]
    fn bits_serialization_known_pattern() -> eyre::Result<()> {
        let bits: Bits = Bits(FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE);

        let serialized = serde_json::to_string(&bits)?;

        let bytes_code =
            u64_slice_to_u8_vec(&FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE);
        let base64_code = format!("\"{}\"", BASE64_STANDARD.encode(bytes_code));

        assert_eq!(serialized, base64_code);

        Ok(())
    }

    #[test]
    fn odd_bits_pattern() -> eyre::Result<()> {
        assert_eq!(
            binary_string_u8(&ODD_BITS_SET_PATTERN_SINGLE_BYTE, false),
            ODD_BITS_SET_PATTERN_SINGLE_BYTE_STR
        );
        assert_eq!(
            binary_string_u8(&ODD_BITS_SET_PATTERN_SINGLE_BYTE_4_BYTES, false),
            ODD_BITS_SET_PATTERN_SINGLE_BYTE_STR.repeat(4)
        );
        assert_eq!(
            binary_string_u8(&ODD_BITS_SET_PATTERN_IRIS_CODE_BYTES, false),
            ODD_BITS_SET_PATTERN_SINGLE_BYTE_STR.repeat(LIMBS * 8)
        );

        Ok(())
    }

    #[test]
    fn first_half_bits_pattern_u8() -> eyre::Result<()> {
        assert_eq!(
            binary_string_u8(&FIRST_HALF_BITS_SET_PATTERN_BYTE, false),
            FIRST_HALF_BITS_SET_PATTERN_BYTE_STR
        );
        assert_eq!(
            binary_string_u8(&FIRST_HALF_BITS_SET_PATTERN_4_BYTES, false),
            FIRST_HALF_BITS_SET_PATTERN_BYTE_STR.repeat(4)
        );
        assert_eq!(
            binary_string_u8(
                &FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES,
                false,
            ),
            FIRST_HALF_BITS_SET_PATTERN_BYTE_STR.repeat(LIMBS * 8)
        );

        Ok(())
    }

    #[test]
    fn first_half_bits_pattern_u64() -> eyre::Result<()> {
        assert_eq!(
            binary_string_u64(&FIRST_HALF_BITS_SET_PATTERN_U64_SINGLE, true),
            FIRST_HALF_BITS_SET_PATTERN_U64_STR
        );
        assert_eq!(
            binary_string_u64(
                &FIRST_HALF_BITS_SET_PATTERN_U64_IRIS_CODE,
                false,
            ),
            FIRST_HALF_BITS_SET_PATTERN_U64_STR.repeat(LIMBS)
        );

        Ok(())
    }

    #[test]
    fn bits_from_u8() -> eyre::Result<()> {
        let bits_u64 = Bits::from(FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES);

        assert_eq!(
            binary_string_u8(
                &FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES,
                false,
            ),
            binary_string_u64(&bits_u64.0, false)
        );

        print_binary_representation_u8(
            &FIRST_HALF_BITS_SET_PATTERN_IRIS_CODE_BYTES,
        );
        print_binary_representation_u64(&bits_u64.0);

        Ok(())
    }

    #[test]
    fn test_bytemuck_is_le_and_reverses_bit_pattern() {
        print_binary_representation_u64(
            &FIRST_HALF_BITS_SET_PATTERN_U64_SINGLE,
        );
        let bytes = bytemuck::bytes_of(&FIRST_HALF_BITS_SET_PATTERN_U64_SINGLE);
        print_binary_representation_u8(bytes);

        assert_eq!(
            binary_string_u8(bytes, false),
            "0000000000000000000000000000000011111111111111111111111111111111"
        )
    }

    pub fn binary_string_u8(pattern: &[u8], separated: bool) -> String {
        pattern
            .iter()
            .map(|byte| format!("{:08b}", byte))
            .collect::<Vec<String>>()
            .join(if separated { " " } else { "" })
    }

    pub fn print_binary_representation_u8(pattern: &[u8]) {
        println!("{}", binary_string_u8(pattern, true));
    }

    pub fn print_binary_representation_u64(pattern: &[u64]) {
        println!("{}", binary_string_u64(pattern, true));
    }

    pub fn binary_string_u64(pattern: &[u64], separated: bool) -> String {
        pattern
            .iter()
            .map(|v| format!("{:064b}", v))
            .collect::<Vec<String>>()
            .join(if separated { " " } else { "" })
    }

    #[test]
    fn test_from_encoded_bits() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let bits: Bits = rng.gen();

            let encoded_bits = EncodedBits::from(&bits);
            let decoded_bits = Bits::from(&encoded_bits);

            assert_eq!(bits, decoded_bits);
        }
    }

    #[test]
    fn test_serialization_round_trip() -> eyre::Result<()> {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let bits: Bits = rng.gen();

            let serialized = serde_json::to_string(&bits)?;
            let deserialized: Bits = serde_json::from_str(&serialized)?;

            assert_eq!(bits, deserialized);
        }

        Ok(())
    }

    #[test]
    fn test_parsing_round_trip() -> eyre::Result<()> {
        let mut rng = thread_rng();
        for _ in 0..100 {
            let bits: Bits = rng.gen();

            let serialized = bits.to_string();
            let deserialized: Bits = serialized.parse()?;

            assert_eq!(bits, deserialized);
        }

        Ok(())
    }

    #[test]
    fn get_set() {
        let mut bits = Bits::ZERO;

        // All bits are unset
        for bit in 0..BITS {
            assert!(!bits.get(bit));
            assert!(!bits[bit]);
        }

        let mut rng = thread_rng();
        let mut set_indexes = vec![];
        for _ in 0..100 {
            set_indexes.push(rng.gen::<usize>() % BITS);
        }

        for index in &set_indexes {
            bits.set(*index, true);
        }

        for index in &set_indexes {
            assert!(bits.get(*index));
            assert!(bits[*index]);
        }
    }
}
