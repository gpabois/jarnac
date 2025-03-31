use super::error::KnackError;

pub type KnackResult<T> = std::result::Result<T, KnackError>;