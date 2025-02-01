use std::{alloc::{alloc_zeroed, dealloc, Layout}, collections::HashMap, mem::MaybeUninit, ptr::NonNull};

use super::{page::PageId, PagerError, PagerResult};

pub(super) struct PagerCache {
  /// The memory layout of the allocated space
  layout: Layout,
  /// The allocated cache space
  ptr: NonNull<u8>,
  /// The size of the cache
  size: usize,
  /// The tail of allocated space
  tail: usize,
  /// The size of a page
  page_size: usize,
  /// Free page cells
  free_list: Vec<NonNull<PageCell>>,
  /// Current cached pages
  stored: HashMap<PageId, NonNull<PageCell>>
}

impl Drop for PagerCache {
    fn drop(&mut self) {
        unsafe {
          dealloc(self.ptr.as_mut(), self.layout);
        }
    }
}

impl PagerCache {
  /// Instantiate a new page cache
  pub fn new(cache_size: usize, page_size: usize) -> Self {
    unsafe {
      let layout = Layout::from_size_align(
        cache_size, 
        (page_size + size_of::<PageCell>()).next_power_of_two()
      ).unwrap();

      let ptr = NonNull::new(alloc_zeroed(layout)).unwrap();

      return Self {
        layout,
        ptr,
        size: cache_size,
        tail: 0,
        page_size,
        free_list: Vec::default(),
        stored: HashMap::default()
      }
    }
  }
  /// Le cache est plein.
  pub fn is_full(&self) -> bool {
    self.tail >= self.size
    && self.free_list.len() == 0
  }

  /// Trouve un candidat à la libération
  pub(super) fn find_freeable_candidate(&self) -> Option<PageId> {
    self
      .stored
      .values()
      .map(|cell| unsafe {cell.as_ref()})
      .filter(|cell| cell.borrowed == false)
      .map(|cell| cell.id)
      .next()
  }

  /// Libère une entrée du cache
  pub fn free(&mut self, id: &PageId) {
    let cell = self.stored.remove(id).unwrap();
    self.free_list.push(cell);
  }

  /// Récupère la page si elle est cachée, et non déjà empruntée.
  pub fn get(&self, id: &PageId) -> Option<NonNull<PageCell>> {
    self.stored.get(id)
    .and_then(|cell| {
      unsafe {
        (!cell.as_ref().borrowed).then_some(cell)
      }
    }).copied()
  }

  /// Réserve de l'espace pour stocker une page.
  pub fn reserve(&mut self, id: PageId) -> PagerResult<NonNull<PageCell>> {
    if let Some(stored) = self.stored.get(&id).copied() {
      return Ok(stored);
    }

    if let Some(mut free) = self.free_list.pop() {
      unsafe {
        let cell = free.as_mut();
        cell.dirty = false;
        cell.use_counter = 0;
        cell.borrowed = false;
        self.stored.insert(id, free);
        return Ok(free);
      }
    }

    let new_tail = self.page_size + size_of::<PageCell>();
    
    if new_tail >= self.size {
      return Err(PagerError::CacheFull)
    }

    unsafe {
      let ptr = self.ptr.add(self.size);
      let mut cell_ptr = ptr.cast::<MaybeUninit<PageCell>>();
      let content = NonNull::slice_from_raw_parts(ptr.add(size_of::<PageCell>()), self.page_size);
      let cell_ptr = NonNull::new_unchecked(cell_ptr.as_mut().write(PageCell { id, content, dirty: false, use_counter: 0, borrowed: false }));
      self.stored.insert(id, cell_ptr);

      return Ok(cell_ptr);
    }
  }
}

pub(super) struct PageCell {
  pub id: PageId,
  pub content: NonNull<[u8]>,
  pub dirty: bool,
  pub use_counter: usize,
  pub borrowed: bool
}
