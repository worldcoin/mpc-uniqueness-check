use std::fmt;

use crate::bits::Bits;
use crate::distance::EncodedBits;

pub trait DbKind {
    const TABLE_NAME: &'static str;
    const COLUMN_NAME: &'static str;

    type Item: for<'a> sqlx::Decode<'a, sqlx::Postgres>
        + sqlx::Type<sqlx::Postgres>
        + for<'a> sqlx::Encode<'a, sqlx::Postgres>
        + Send
        + Sync
        + Unpin
        + fmt::Debug;
}

pub struct Masks;

pub struct Shares;

impl DbKind for Masks {
    const TABLE_NAME: &'static str = "masks";
    const COLUMN_NAME: &'static str = "mask";

    type Item = Bits;
}

impl DbKind for Shares {
    const TABLE_NAME: &'static str = "shares";
    const COLUMN_NAME: &'static str = "share";

    type Item = EncodedBits;
}
