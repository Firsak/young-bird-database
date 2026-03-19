pub mod buffer_pool;
pub mod errors;
pub mod index;
pub mod overflow;
pub mod page;
pub mod table;
pub mod traits;
pub mod types;

pub const KBYTES: usize = 1024;
pub(crate) const HEADER_SIZE: usize = 20;
pub(crate) const PAGE_RECORD_METADATA_SIZE: usize = 20;
pub(crate) const INDEX_HEADER_SIZE: usize = 24;
pub(crate) const INDEX_ENTRY_SIZE: usize = 20;
pub(crate) const OVERFLOW_HEADER_SIZE: usize = 16;
pub(crate) const OVERFLOW_REF_SIZE: usize = 16;
pub(crate) const OVERFLOW_THRESHOLD: usize = 256;
