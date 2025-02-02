use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use super::{cache::PageCell, PagerResult};

pub type PageId = usize;

pub struct RefPage<'pager> {
    _pht: PhantomData<&'pager ()>,
    cell: NonNull<PageCell>,
}

impl Deref for RefPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.cell.as_ref().content.as_ref() }
    }
}

impl Drop for RefPage<'_> {
    fn drop(&mut self) {
        unsafe {
            self.cell.as_mut().rw_counter -= 1;
        }
    }
}

impl<'pager> RefPage<'pager> {
    pub(super) fn try_acquire(mut cell: NonNull<PageCell>) -> PagerResult<Self> {
        unsafe {
            if cell.as_ref().rw_counter < 0 {
                return Err(crate::pager::PagerError::PageAlreadyBorrowed);
            }

            cell.as_mut().rw_counter += 1;
            cell.as_mut().use_counter += 1;

            Ok(Self {
                _pht: PhantomData,
                cell,
            })
        }
    }

    pub(super) fn drop_dirty_flag(&self) {
        unsafe {
            self.cell.as_ptr().as_mut().unwrap().dirty = false;
        }
    }

    pub fn is_dirty(&self) -> bool {
        unsafe { self.cell.as_ref().dirty }
    }

    pub fn id(&self) -> PageId {
        unsafe { self.cell.as_ref().id }
    }
}

pub struct MutPage<'pager> {
    _pht: PhantomData<&'pager ()>,
    cell: NonNull<PageCell>,
}

impl Deref for MutPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.cell.as_ref().content.as_ref() }
    }
}

impl DerefMut for MutPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            let mut_cell = self.cell.as_mut();
            mut_cell.dirty = true;
            mut_cell.content.as_mut()
        }
    }
}

impl Drop for MutPage<'_> {
    fn drop(&mut self) {
        unsafe {
            self.cell.as_mut().rw_counter += 1;
        }
    }
}

impl<'pager> MutPage<'pager> {
    pub(super) fn try_acquire(mut cell: NonNull<PageCell>) -> PagerResult<Self> {
        unsafe {
            if cell.as_ref().rw_counter != 0 {
                return Err(crate::pager::PagerError::PageAlreadyBorrowed);
            }

            cell.as_mut().rw_counter = -1;
            cell.as_mut().use_counter += 1;

            Ok(Self {
                _pht: PhantomData,
                cell,
            })
        }
    }

    pub(super) fn drop_dirty_flag(&self) {
        unsafe {
            self.cell.as_ptr().as_mut().unwrap().dirty = false;
        }
    }

    pub(super) fn into_cell(self) -> NonNull<PageCell> {
        let cell = self.cell;
        cell
    }

    pub fn is_dirty(&self) -> bool {
        unsafe { self.cell.as_ref().dirty }
    }

    pub fn id(&self) -> PageId {
        unsafe { self.cell.as_ref().id }
    }
}
