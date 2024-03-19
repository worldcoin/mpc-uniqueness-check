use thiserror::Error;

#[derive(Debug, Clone, Copy, Error)]
#[error("Failed to convert Vec to Array")]
pub struct VecToArrayConvertError;
