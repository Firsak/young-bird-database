pub trait BinarySerde {
    type Output;

    fn to_bytes(&self) -> Self::Output;
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized;
}
