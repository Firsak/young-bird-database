use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};

use super::cached_page::CachedPage;
use crate::database_operations::file_processing::page::page::Page;

pub const DEFAULT_CACHE_SIZE: usize = 64;

/// Buffer pool that caches pages in memory using LRU eviction.
///
/// Pages are tracked in a HashMap for O(1) lookup by page number,
/// and a VecDeque for LRU ordering (front = least recent, back = most recent).
#[derive(Debug)]
pub struct BufferPool {
    capacity: usize,
    pages: HashMap<u64, CachedPage>,
    lru_order: VecDeque<u64>,
}

impl BufferPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pages: HashMap::with_capacity(capacity),
            lru_order: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns the number of cached pages.
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Returns true if the buffer pool has no cached pages.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Returns the buffer pool capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Moves a page to the back of lru_order (most recently used).
    fn touch(&mut self, page_number: u64) {
        let cached_page = self.pages.get(&page_number);
        if cached_page.is_none() {
            return;
        }

        if *self.lru_order.iter().last().unwrap() == page_number {
            return;
        }

        // Note: this front case is logically redundant — the position + remove path
        // below handles index 0 correctly. However, VecDeque::remove(0) is optimized
        // internally to behave like pop_front (O(1)), so keeping this explicit branch
        // is purely a readability choice, not a performance one.
        if *self.lru_order.iter().next().unwrap() == page_number {
            let index = self.lru_order.pop_front().unwrap();
            self.lru_order.push_back(index);
            return;
        }

        let pos = self
            .lru_order
            .iter()
            .position(|order| *order == page_number)
            .unwrap();
        let index = self.lru_order.remove(pos).unwrap();
        self.lru_order.push_back(index);
    }
    /// Looks up a page by number. If found, refreshes its LRU position and
    /// returns an immutable reference. Returns None on cache miss.
    ///
    /// # Arguments
    /// * `page_number` - Global page number to look up
    pub fn get(&mut self, page_number: u64) -> Option<&Page> {
        self.touch(page_number);
        let cached_page = self.pages.get(&page_number);
        Some(&cached_page?.page)
    }
    /// Looks up a page by number. If found, refreshes its LRU position and
    /// returns a mutable reference to the CachedPage. The caller can modify
    /// the page and call `mark_dirty()`.
    ///
    /// # Arguments
    /// * `page_number` - Global page number to look up
    pub fn get_mut(&mut self, page_number: u64) -> Option<&mut CachedPage> {
        self.touch(page_number);
        self.pages.get_mut(&page_number)
    }
    /// Inserts a page into the cache. If the page already exists, updates it
    /// and refreshes its LRU position. If the cache is full, evicts the LRU
    /// page and returns it so the caller can flush it to disk if dirty.
    ///
    /// # Arguments
    /// * `page_number` - Global page number
    /// * `page` - The Page to cache
    ///
    /// # Returns
    /// `Some((evicted_page_number, evicted_cached_page))` if eviction occurred,
    /// `None` otherwise.
    pub fn put(&mut self, page_number: u64, page: Page) -> Option<(u64, CachedPage)> {
        match self.pages.entry(page_number) {
            Entry::Occupied(mut entry) => {
                // Already cached — update the page, refresh LRU position
                entry.insert(CachedPage { page, dirty: false });
                self.touch(page_number);
                return None;
            }
            Entry::Vacant(entry) => {
                entry.insert(CachedPage { page, dirty: false });
                self.lru_order.push_back(page_number);
            }
        }

        if self.len() > self.capacity() {
            let least_used_index = self.lru_order.pop_front().unwrap();
            Some((
                least_used_index,
                self.pages.remove(&least_used_index).unwrap(),
            ))
        } else {
            None
        }
    }
    /// Removes a page from the cache. Used when a page is explicitly invalidated.
    ///
    /// # Arguments
    /// * `page_number` - Global page number to remove
    ///
    /// # Returns
    /// The removed CachedPage if it was in the cache, None otherwise.
    pub fn remove(&mut self, page_number: u64) -> Option<CachedPage> {
        match self.pages.remove(&page_number) {
            None => None,
            Some(cached_page) => {
                self.lru_order.retain(|index| *index != page_number);
                Some(cached_page)
            }
        }
    }
    /// Returns the page numbers of all dirty pages in the cache.
    /// The caller can then flush each one individually via `get_mut` + write.
    pub fn dirty_page_numbers(&self) -> Vec<u64> {
        let mut res: Vec<u64> = vec![];
        for index in &self.lru_order {
            let cached_page = self.pages.get(index).unwrap();
            if cached_page.dirty {
                res.push(*index);
            }
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_operations::file_processing::page::header::PageHeader;
    use crate::database_operations::file_processing::page::page::Page;

    fn make_test_page(page_number: u64) -> Page {
        let free_space = 8192 - 20; // page_size - header
        Page::new(
            PageHeader::new(page_number, 0, 0, free_space, 0),
            Vec::new(),
            Vec::new(),
        )
    }

    #[test]
    fn test_new_buffer_pool() {
        let pool = BufferPool::new(4);
        assert_eq!(pool.capacity(), 4);
        assert_eq!(pool.len(), 0);
        assert!(pool.is_empty());
    }

    #[test]
    fn test_put_and_get() {
        let mut pool = BufferPool::new(4);
        let evicted = pool.put(0, make_test_page(0));
        assert!(evicted.is_none());
        assert_eq!(pool.len(), 1);

        let page = pool.get(0);
        assert!(page.is_some());

        let missing = pool.get(99);
        assert!(missing.is_none());
    }

    #[test]
    fn test_eviction_when_full() {
        let mut pool = BufferPool::new(3);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Cache is full — putting page 3 should evict page 0 (LRU)
        let evicted = pool.put(3, make_test_page(3));
        assert!(evicted.is_some());
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 0);

        assert!(pool.get(0).is_none());
        assert!(pool.get(3).is_some());
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn test_lru_order_updated_on_get() {
        let mut pool = BufferPool::new(3);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Access page 0 — makes it most recently used
        pool.get(0);

        // Eviction should now remove page 1 (least recently used)
        let evicted = pool.put(3, make_test_page(3));
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 1);
    }

    #[test]
    fn test_lru_order_updated_on_get_mut() {
        let mut pool = BufferPool::new(3);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Mutably access page 0 — refreshes its LRU position
        pool.get_mut(0);

        let evicted = pool.put(3, make_test_page(3));
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 1);
    }

    #[test]
    fn test_mark_dirty_and_flush() {
        let mut pool = BufferPool::new(4);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Mark pages 0 and 2 as dirty
        pool.get_mut(0).unwrap().mark_dirty();
        pool.get_mut(2).unwrap().mark_dirty();

        let dirty = pool.dirty_page_numbers();
        assert_eq!(dirty.len(), 2);
        assert!(dirty.contains(&0));
        assert!(dirty.contains(&2));
    }

    #[test]
    fn test_remove() {
        let mut pool = BufferPool::new(4);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        let removed = pool.remove(1);
        assert!(removed.is_some());
        assert_eq!(pool.len(), 2);
        assert!(pool.get(1).is_none());

        // Other pages unaffected
        assert!(pool.get(0).is_some());
        assert!(pool.get(2).is_some());
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut pool = BufferPool::new(4);
        pool.put(0, make_test_page(0));

        let removed = pool.remove(99);
        assert!(removed.is_none());
        assert_eq!(pool.len(), 1);
    }

    #[test]
    fn test_put_existing_page_updates_without_duplicate() {
        let mut pool = BufferPool::new(3);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Re-put page 0 — should update, not create duplicate in LRU
        let evicted = pool.put(0, make_test_page(0));
        assert!(evicted.is_none()); // No eviction since page already existed
        assert_eq!(pool.len(), 3);

        // Now insert page 3 — should evict page 1 (oldest non-updated)
        let evicted = pool.put(3, make_test_page(3));
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 1);
    }

    #[test]
    fn test_eviction_returns_dirty_page() {
        let mut pool = BufferPool::new(2);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));

        // Mark page 0 dirty, then evict it
        pool.get_mut(0).unwrap().mark_dirty();

        // Access page 1 so page 0 becomes LRU
        pool.get(1);

        let evicted = pool.put(2, make_test_page(2));
        let (evicted_num, evicted_page) = evicted.unwrap();
        assert_eq!(evicted_num, 0);
        assert!(evicted_page.dirty); // Caller knows to flush this
    }

    #[test]
    fn test_touch_already_at_back() {
        let mut pool = BufferPool::new(3);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Page 2 is already most recent — touching should be a no-op
        pool.get(2);

        // Eviction should still remove page 0
        let evicted = pool.put(3, make_test_page(3));
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 0);
    }

    #[test]
    fn test_touch_middle_element() {
        let mut pool = BufferPool::new(3);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));
        pool.put(2, make_test_page(2));

        // Touch page 1 (middle) — LRU order becomes [0, 2, 1]
        pool.get(1);

        // Eviction should remove page 0
        let evicted = pool.put(3, make_test_page(3));
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 0);

        // Eviction should now remove page 2
        let evicted = pool.put(4, make_test_page(4));
        let (evicted_num, _) = evicted.unwrap();
        assert_eq!(evicted_num, 2);
    }

    #[test]
    fn test_flush_all_empty_pool() {
        let mut pool: BufferPool = BufferPool::new(4);
        let dirty = pool.dirty_page_numbers();
        assert!(dirty.is_empty());
    }

    #[test]
    fn test_flush_all_no_dirty_pages() {
        let mut pool = BufferPool::new(4);
        pool.put(0, make_test_page(0));
        pool.put(1, make_test_page(1));

        let dirty = pool.dirty_page_numbers();
        assert!(dirty.is_empty());
    }
}
