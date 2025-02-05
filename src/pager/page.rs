use std::{
    fmt::Display, io::Cursor, marker::PhantomData, ops::{Deref, DerefMut}, ptr::NonNull
};

use super::{cache::{CachedPage, CachedPageData}, error::{PagerError, PagerErrorKind}, PagerResult};

pub type PageId = u64;
pub type PageLocation = u64;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Free = 0x00
}

impl Display for PageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageKind::Free => write!(f, "free"),
        }
    }
}

impl PageKind {
    pub fn assert(&self, to: PageKind) -> PagerResult<()> {
        (*self == to)
            .then(|| ())
            .ok_or_else(|| PagerError::new(PagerErrorKind::WrongPageKind {expected: to, got: *self}))
    }
}

impl TryFrom<u8> for PageKind {
    type Error = PagerError;
    
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Free),
            _ => Err(PagerError::new(PagerErrorKind::InvalidPageKind))
        }
    }

    
}

pub struct RefPage<'pager> {
    _pht: PhantomData<&'pager ()>,
    ptr: NonNull<CachedPageData>,
}

impl Deref for RefPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref().content.as_ref() }
    }
}

impl Drop for RefPage<'_> {
    fn drop(&mut self) {
        unsafe {
            self.ptr.as_mut().rw_counter -= 1;
        }
    }
}

impl<'pager> RefPage<'pager> {
    pub(super) fn try_acquire(mut cpage: CachedPage<'pager>) -> PagerResult<Self> {
        unsafe {
            if cpage.rw_counter < 0 {
                return Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed));
            }

            cpage.rw_counter += 1;
            cpage.use_counter += 1;

            Ok(Self {
                _pht: PhantomData,
                ptr: cpage.leak(),
            })
        }
    }

    pub fn open_cursor(&self) -> Cursor<& [u8]> {
        Cursor::new(self.deref())
    }

    pub fn is_dirty(&self) -> bool {
        unsafe { self.ptr.as_ref().is_dirty() }
    }

    pub fn id(&self) -> PageId {
        unsafe { self.ptr.as_ref().pid }
    }
}

pub struct MutPage<'pager> {
    _pht: PhantomData<&'pager ()>,
    cell: NonNull<CachedPageData>,
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
            mut_cell.set_diry();
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
    pub(super) fn try_acquire(mut cpage: CachedPage<'pager>) -> PagerResult<Self> {
        unsafe {
            if cpage.rw_counter != 0 {
                return Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed));
            }

            cpage.rw_counter = -1;
            cpage.use_counter += 1;

            Ok(Self {
                _pht: PhantomData,
                cell: cpage.leak(),
            })
        }
    }

    pub fn is_dirty(&self) -> bool {
        unsafe { self.cell.as_ref().is_dirty() }
    }

    pub fn id(&self) -> PageId {
        unsafe { self.cell.as_ref().pid }
    }

    pub fn open_mut_cursor(&mut self) -> Cursor<&mut [u8]> {
        Cursor::new(self.deref_mut())
    }

    pub fn open_cursor(&self) -> Cursor<& [u8]> {
        Cursor::new(self.deref())
    }
}
