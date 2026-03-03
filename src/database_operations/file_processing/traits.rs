use std::fs::File;

/// In-memory serialization: struct ↔ byte array.
/// All implementations use little-endian byte order.
pub trait BinarySerde {
    type Output;

    /// Serializes the struct into bytes.
    fn to_bytes(&self) -> Self::Output;

    /// Deserializes from a byte slice. Returns Err on invalid/wrong-sized input.
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized;
}

/// File-level I/O: read/write a struct at a specific byte offset in a file.
pub trait ReadWrite {
    type RWError;

    /// Writes this struct to `file` at the given absolute byte offset.
    ///
    /// # Arguments
    /// * `file` - Open file handle with write permission
    /// * `absolute_file_start_offset` - Byte position in the file to start writing at
    /// * `filename` - File path, used only for error messages
    fn write_to_file(
        &self,
        file: &mut File,
        absolute_file_start_offset: u64,
        filename: &str,
    ) -> Result<(), Self::RWError>;

    /// Reads `size` bytes from `file` at the given offset and deserializes.
    ///
    /// # Arguments
    /// * `file` - Open file handle with read permission
    /// * `absolute_file_start_offset` - Byte position in the file to start reading from
    /// * `size` - Number of bytes to read (must match the struct's expected binary size)
    /// * `filename` - File path, used only for error messages
    fn read_from_file(
        file: &mut File,
        absolute_file_start_offset: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized;
}
