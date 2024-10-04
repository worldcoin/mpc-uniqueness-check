use crate::bits::{Bits, BITS};
use crate::distance::EncodedBits;

const BYTES_PER_BITS: usize = BITS / 8;
const BYTES_PER_ENCODED_BITS: usize = BITS * 2;

impl<DB> sqlx::Type<DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_BITS]: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <[u8; BYTES_PER_BITS] as sqlx::Type<DB>>::type_info()
    }
}

impl<'r, DB> sqlx::Decode<'r, DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_BITS]: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <[u8; BYTES_PER_BITS] as sqlx::Decode<DB>>::decode(value)?;

        let bits: Vec<u64> = bytes
            .array_chunks::<8>()
            .map(|x| u64::from_be_bytes(*x))
            .collect();

        let bits = bits.try_into().expect("Wrong size");

        Ok(Self(bits))
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_BITS]: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let mut bytes: [u8; BYTES_PER_BITS] = [0; BYTES_PER_BITS];

        for (i, v) in self.0.into_iter().flat_map(u64::to_be_bytes).enumerate()
        {
            bytes[i] = v;
        }

        <[u8; BYTES_PER_BITS] as sqlx::Encode<DB>>::encode(bytes, buf)
    }
}

impl<DB> sqlx::Type<DB> for EncodedBits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_ENCODED_BITS]: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <[u8; BYTES_PER_ENCODED_BITS] as sqlx::Type<DB>>::type_info()
    }
}

impl<'r, DB> sqlx::Decode<'r, DB> for EncodedBits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_ENCODED_BITS]: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes =
            <[u8; BYTES_PER_ENCODED_BITS] as sqlx::Decode<DB>>::decode(value)?;

        let bits: Vec<u16> = bytes
            .array_chunks::<2>()
            .map(|x| u16::from_be_bytes(*x))
            .collect();
        let bits = bits.try_into().expect("Wrong size");

        Ok(Self(bits))
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for EncodedBits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_ENCODED_BITS]: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::Database>::ArgumentBuffer<'q>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let mut bytes: [u8; BYTES_PER_ENCODED_BITS] =
            [0; BYTES_PER_ENCODED_BITS];

        for (i, v) in self.0.into_iter().flat_map(u16::to_be_bytes).enumerate()
        {
            bytes[i] = v;
        }

        <[u8; BYTES_PER_ENCODED_BITS] as sqlx::Encode<DB>>::encode(bytes, buf)
    }
}
