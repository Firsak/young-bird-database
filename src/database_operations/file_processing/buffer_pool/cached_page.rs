use crate::database_operations::file_processing::page::page::Page;

/// A page held in the buffer pool, with a flag indicating
/// whether it has been modified since it was read from disk.
#[derive(Debug)]
pub struct CachedPage {
    pub page: Page,
    pub dirty: bool,
}

impl CachedPage {
    pub fn new(page: Page) -> Self {
        Self { page, dirty: false }
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }
}
