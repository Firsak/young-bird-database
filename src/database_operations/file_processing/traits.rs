use std::fs::File;

pub trait BinarySerde {
    type Output;

    fn to_bytes(&self) -> Self::Output;
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized;
}

pub trait ReadWrite {
    type RWError;

    fn write_to_file(
        &self,
        file: &mut File,
        absolute_file_start_offset: u64,
        filename: &str,
    ) -> Result<(), Self::RWError>;

    fn read_from_file(
        file: &mut File,
        absolute_file_start_offset: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized;
}
