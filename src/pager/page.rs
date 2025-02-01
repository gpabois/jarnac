use std::{marker::PhantomData, ops::{Deref, DerefMut}, ptr::NonNull};

use super::{cache::PageCell, PagerResult};

pub type PageId = usize;

pub struct Page<'pager> {
  _pht: PhantomData<&'pager()>,
  cell: NonNull<PageCell>
}

impl Deref for Page<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
      unsafe {
        self.cell.as_ref().content.as_ref()
      }
    }
}

impl DerefMut for Page<'_> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    unsafe {
      self.cell.as_mut().content.as_mut()
    }
  }
}

impl Drop for Page<'_> {
  fn drop(&mut self) {
    unsafe {
      self.cell.as_mut().borrowed = false;
    }
  }
}

impl<'pager> Page<'pager> {
  pub(super) fn try_acquire(mut cell: NonNull<PageCell>) -> PagerResult<Self> {
    unsafe {
      if cell.as_ref().borrowed {
        return Err(crate::pager::PagerError::PageAlreadyBorrowed);
      }

      cell.as_mut().borrowed = true;
      cell.as_mut().use_counter += 1;

      Ok(Self {
        _pht: PhantomData,
        cell
      })
    }
  }

  pub(super) fn drop_dirty_flag(&self) {
    unsafe {
      self.cell.as_ptr().as_mut().unwrap().dirty = false;
    }
  }

  pub fn is_dirty(&self) -> bool {
    unsafe {
      self.cell.as_ref().dirty
    }
  }

  pub fn id(&self) -> PageId {
    unsafe {
      self.cell.as_ref().id
    }
  }
}
