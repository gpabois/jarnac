use std::{
    fmt::Display,
    io::Cursor,
    ops::{Deref, DerefMut},
};

use super::{
    cache::CachedPage,
    error::{PagerError, PagerErrorKind},
    PagerResult,
};

pub type PageId = u64;
pub type PageLocation = u64;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Free = 0x00,
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
        (*self == to).then_some(()).ok_or_else(|| {
            PagerError::new(PagerErrorKind::WrongPageKind {
                expected: to,
                got: *self,
            })
        })
    }
}

impl TryFrom<u8> for PageKind {
    type Error = PagerError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Free),
            _ => Err(PagerError::new(PagerErrorKind::InvalidPageKind)),
        }
    }
}

pub struct RefPage<'pager>(CachedPage<'pager>);

impl Deref for RefPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.content.as_ref() }
    }
}

impl Drop for RefPage<'_> {
    fn drop(&mut self) {
        self.0.rw_counter -= 1;
    }
}

impl<'pager> RefPage<'pager> {
    pub(super) fn new(mut cached: CachedPage<'pager>) -> Self {
        if cached.rw_counter < 0 {
            panic!("already mutable borrowed")
        }

        cached.rw_counter += 1;
        Self(cached)
    }

    pub(super) fn try_new(mut cached: CachedPage<'pager>) -> PagerResult<Self> {
        if cached.rw_counter < 0 {
            Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
        } else {
            cached.rw_counter += 1;
            Ok(Self(cached))
        }
    }

    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.deref())
    }

    pub fn id(&self) -> PageId {
        self.0.id()
    }
}

pub struct MutPage<'pager> {
    /// If true, dirty flag is not raised upon modification
    dry: bool,
    inner: CachedPage<'pager>,
}

impl Deref for MutPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.content.as_ref() }
    }
}

impl DerefMut for MutPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            if !self.dry {
                self.inner.set_dirty();
            }
            self.inner.content.as_mut()
        }
    }
}

impl Drop for MutPage<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter += 1;
    }
}

impl<'pager> MutPage<'pager> {
    pub(super) fn try_new(mut inner: CachedPage<'pager>) -> PagerResult<Self> {
        if inner.rw_counter != 0 {
            Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
        } else {
            inner.rw_counter -= 1;
            Ok(Self { dry: false, inner })
        }
    }

    pub(super) fn try_new_with_options(
        mut inner: CachedPage<'pager>,
        dry: bool,
    ) -> PagerResult<Self> {
        if inner.rw_counter != 0 {
            Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
        } else {
            inner.rw_counter -= 1;
            Ok(Self { dry, inner })
        }
    }

    pub fn id(&self) -> PageId {
        self.inner.id()
    }

    pub fn open_mut_cursor(&mut self) -> Cursor<&mut [u8]> {
        Cursor::new(self.deref_mut())
    }

    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.deref())
    }
}
