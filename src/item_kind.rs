use strum::EnumString;

use crate::bits::Bits;
use crate::encoded_bits::EncodedBits;

pub trait ItemKindMarker {
    const PARQUET_COLUMN_NAME: &'static str;

    type Type;
}

pub struct Shares;
pub struct Masks;

impl ItemKindMarker for Shares {
    const PARQUET_COLUMN_NAME: &'static str = "share";

    type Type = EncodedBits;
}

impl ItemKindMarker for Masks {
    const PARQUET_COLUMN_NAME: &'static str = "mask";

    type Type = Bits;
}

#[derive(Debug, Clone, Copy, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ItemKind {
    Shares,
    Masks,
}
