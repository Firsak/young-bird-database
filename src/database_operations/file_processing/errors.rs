use std::fmt;

/// Unified error type for all database operations.
#[derive(Debug)]
pub enum DatabaseError {
    /// File system I/O failure (open, read, write, seek).
    Io(std::io::Error),
    /// The target page doesn't have enough free space for this record.
    PageFull,
    /// The record is too large to fit in any single page.
    RecordTooLarge,
    /// No record found with the given ID.
    RecordNotFound(u64),
    /// Invalid argument (out of bounds, bad parameters).
    InvalidArgument(String),
    /// Binary serialization/deserialization failure.
    Serialization(String),
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseError::Io(err) => write!(f, "I/O error: {}", err),
            DatabaseError::PageFull => write!(f, "Page is full"),
            DatabaseError::RecordTooLarge => write!(f, "Record too large for a single page"),
            DatabaseError::RecordNotFound(id) => write!(f, "Record not found: id {}", id),
            DatabaseError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            DatabaseError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for DatabaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DatabaseError::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// Converts std::io::Error into DatabaseError::Io automatically.
/// This enables using `?` on I/O operations in functions that return Result<_, DatabaseError>.
impl From<std::io::Error> for DatabaseError {
    fn from(err: std::io::Error) -> Self {
        DatabaseError::Io(err)
    }
}

impl From<String> for DatabaseError {
    fn from(msg: String) -> Self {
        DatabaseError::Serialization(msg)
    }
}

impl From<Box<dyn std::error::Error>> for DatabaseError {
    fn from(err: Box<dyn std::error::Error>) -> Self {
        match err.downcast::<std::io::Error>() {
            Ok(io_err) => DatabaseError::Io(*io_err),
            Err(other) => DatabaseError::Serialization(other.to_string()),
        }
    }
}
