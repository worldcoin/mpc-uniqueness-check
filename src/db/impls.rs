use crate::bits::{Bits, BITS};

const BYTES: usize = BITS / 8;

impl<DB> sqlx::Type<DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES]: sqlx::Type<DB>,
{
    fn type_info() -> DB::TypeInfo {
        <[u8; BYTES] as sqlx::Type<DB>>::type_info()
    }
}

impl<'r, DB> sqlx::Decode<'r, DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES]: sqlx::Decode<'r, DB>,
{
    fn decode(
        value: <DB as sqlx::database::HasValueRef<'r>>::ValueRef,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let bytes = <[u8; BYTES] as sqlx::Decode<DB>>::decode(value)?;

        Ok(bytemuck::pod_read_unaligned(&bytes))
    }
}

impl<'q, DB> sqlx::Encode<'q, DB> for Bits
where
    DB: sqlx::Database,
    [u8; BYTES]: sqlx::Encode<'q, DB>,
{
    fn encode_by_ref(
        &self,
        buf: &mut <DB as sqlx::database::HasArguments<'q>>::ArgumentBuffer,
    ) -> sqlx::encode::IsNull {
        // The size of the underlying data makes it unaligned
        let bytes = bytemuck::bytes_of(self);
        let bytes: [u8; BYTES] = bytes.try_into().expect("Wrong size");

        <[u8; BYTES] as sqlx::Encode<DB>>::encode(bytes, buf)
    }
}
