use std::{
    fmt::Display, io::Cursor, num::NonZero, ops::{Add, Deref, DerefMut, Mul, Sub}
};

use zerocopy::ByteSlice;
use zerocopy_derive::{FromBytes, Immutable, KnownLayout, TryFromBytes};

use super::{
    cache::CachedPage,
    error::{PagerError, PagerErrorKind},
    PagerResult,
};


#[derive(FromBytes, KnownLayout, Immutable, Clone, Copy, PartialEq, Eq)]
pub struct OptionalPageId(Option<NonZero<u64>>);

impl AsRef<Option<PageId>> for OptionalPageId {
    fn as_ref(&self) -> &Option<PageId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl AsMut<Option<PageId>> for OptionalPageId {
    fn as_mut(&mut self) -> &mut Option<PageId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl From<Option<PageId>> for OptionalPageId {
    fn from(value: Option<PageId>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl Into<Option<PageId>> for OptionalPageId {
    fn into(self) -> Option<PageId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}


#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, TryFromBytes, Immutable, KnownLayout)]
#[repr(transparent)]
pub struct PageId(pub(super)NonZero<u64>);

impl PageId {
    pub(super) fn new(value: u64) -> Self {
        Self(NonZero::new(value).expect("page id must be > 0"))
    }
}

impl PartialEq<u64> for PageId {
    fn eq(&self, other: &u64) -> bool {
        self.0.get().eq(other)
    }
}

impl PartialOrd<u64> for PageId {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.get().partial_cmp(other)
    }
}

impl From<NonZero<u64>> for PageId {
    fn from(value: NonZero<u64>) -> Self {
        Self(value)
    }
}

impl Into<NonZero<u64>> for PageId {
    fn into(self) -> NonZero<u64> {
        self.0
    }
}

impl From<u64> for PageId {
    fn from(value: u64) -> Self {
        Self(NonZero::new(value).expect(&format!("page id should be > 0, got {value}")))
    }
}

impl Into<u64> for PageId {
    fn into(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PageId {
    pub fn get_location(&self, base: u64, page_size: &PageSize) -> PageLocation {
        PageLocation((self.0.get() - 1) * page_size.0 + base)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageLocation(u64);

impl Into<u64> for PageLocation {
    fn into(self) -> u64 {
        self.0
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PageSize(u64);

impl PageSize {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

impl Add<usize> for PageSize {
    type Output = usize;

    fn add(self, rhs: usize) -> Self::Output {
        let ps_usize: usize = self.0.try_into().unwrap();
        ps_usize + rhs
    }
}

impl Sub<usize> for PageSize {
    type Output = usize;

    fn sub(self, rhs: usize) -> Self::Output {
        let ps_usize: usize = self.0.try_into().unwrap();
        ps_usize - rhs
    }
}

impl Mul<u64> for PageSize {
    type Output = u64;

    fn mul(self, rhs: u64) -> Self::Output {
        self.0 * rhs
    }
}

impl From<u64> for PageSize {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Into<u64> for PageSize {
    fn into(self) -> u64 {
        self.0
    }
}

impl From<usize> for PageSize {
    fn from(value: usize) -> Self {
        Self(value.try_into().unwrap())
    }
}

impl Into<usize> for PageSize {
    fn into(self) -> usize {
        self.0.try_into().unwrap()
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    Free = 0,
    Overflow = 1,
    BPlusTree = 2,
    BPlusTreeInterior = 3,
    BPlusTreeLeaf = 4
}

impl Display for PageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageKind::Free => write!(f, "free"),
            PageKind::Overflow => write!(f, "spill"),
            PageKind::BPlusTree => write!(f, "b+ tree"),
            PageKind::BPlusTreeInterior => write!(f, "b+ tree interior"),
            PageKind::BPlusTreeLeaf => write!(f, "b+ tree leaf"),
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
            1 => Ok(Self::Overflow),
            _ => Err(PagerError::new(PagerErrorKind::InvalidPageKind)),
        }
    }
}

pub struct RefPage<'pager>(CachedPage<'pager>);

unsafe impl ByteSlice for RefPage<'_> {}

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

unsafe impl ByteSlice for MutPage<'_> {}

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
