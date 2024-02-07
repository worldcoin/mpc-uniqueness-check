use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct HexByteArray<const N: usize>(pub [u8; N]);

impl<const N: usize> fmt::Debug for HexByteArray<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = hex::encode(&self.0);

        if f.alternate() {
            write!(f, "{:?}", hex)
        } else {
            write!(f, "0x{:?}", hex)
        }
    }
}

impl<const N: usize> fmt::Display for HexByteArray<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hex = hex::encode(&self.0);

        if f.alternate() {
            write!(f, "{}", hex)
        } else {
            write!(f, "0x{}", hex)
        }
    }
}

impl<const N: usize> FromStr for HexByteArray<N> {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim_start_matches("0x");

        let bytes = hex::decode(s)?;

        if bytes.len() != N {
            return Err(hex::FromHexError::InvalidStringLength);
        }

        let mut array = [0u8; N];
        array.copy_from_slice(&bytes);
        Ok(Self(array))
    }
}

impl<const N: usize> Serialize for HexByteArray<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let hex = hex::encode(&self.0);
        serializer.serialize_str(&hex)
    }
}

impl<'de, const N: usize> Deserialize<'de> for HexByteArray<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex_str = String::deserialize(deserializer)?;
        let bytes = hex::decode(&hex_str).map_err(serde::de::Error::custom)?;
        if bytes.len() != N {
            return Err(serde::de::Error::custom("Invalid hex string length"));
        }
        let mut array = [0u8; N];
        array.copy_from_slice(&bytes);
        Ok(Self(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_serialization() {
        let hex = HexByteArray::<4>([0x12, 0x34, 0x56, 0x78]);

        let serialized = serde_json::to_string(&hex).unwrap();
        let expected = "\"12345678\"";
        assert_eq!(serialized, expected);

        let deserialized: HexByteArray<4> =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, hex);
    }

    #[test]
    fn test_hex_display() {
        let hex = HexByteArray::<4>([0x12, 0x34, 0x56, 0x78]);

        let displayed = format!("{}", hex);
        let expected = "0x12345678";
        assert_eq!(displayed, expected);
    }

    #[test]
    fn test_hex_from_str() {
        let hex_str = "12345678";
        let expected = HexByteArray::<4>([0x12, 0x34, 0x56, 0x78]);

        let parsed: HexByteArray<4> = hex_str.parse().unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn test_hex_from_str_with_prefix() {
        let hex_str = "0x12345678";
        let expected = HexByteArray::<4>([0x12, 0x34, 0x56, 0x78]);

        let parsed: HexByteArray<4> = hex_str.parse().unwrap();
        assert_eq!(parsed, expected);
    }
}
