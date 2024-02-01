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
        value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <[u8; BYTES_PER_BITS] as sqlx::Decode<DB>>::decode(value)?;

        Ok(bytemuck::pod_read_unaligned(&bytes))
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_BITS]: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        // The size of the underlying data makes it unaligned
        let bytes = bytemuck::bytes_of(self);
        let bytes: [u8; BYTES_PER_BITS] = bytes.try_into().expect("Wrong size");

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
        value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes =
            <[u8; BYTES_PER_ENCODED_BITS] as sqlx::Decode<DB>>::decode(value)?;

        Ok(bytemuck::pod_read_unaligned(&bytes))
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for EncodedBits
where
    DB: sqlx::Database,
    [u8; BYTES_PER_ENCODED_BITS]: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        // The size of the underlying data makes it unaligned
        let bytes = bytemuck::bytes_of(self);
        let bytes: [u8; BYTES_PER_ENCODED_BITS] =
            bytes.try_into().expect("Wrong size");

        <[u8; BYTES_PER_ENCODED_BITS] as sqlx::Encode<DB>>::encode(bytes, buf)
    }
}
