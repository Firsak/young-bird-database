pub mod errors;
pub mod page;
pub mod table;
pub mod traits;
pub mod types;

pub const KBYTES: usize = 1024;
pub(crate) const HEADER_SIZE: usize = 20;
pub(crate) const PAGE_RECORD_METADATA_SIZE: usize = 20;
