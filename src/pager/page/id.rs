use std::{num::NonZero, ops::Mul};

use zerocopy_derive::*;

use super::{location::PageLocation, size::PageSize};


#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, TryFromBytes, Immutable, KnownLayout)]
#[repr(transparent)]
/// Identifiant d'une page
/// 
/// Les valeurs vont de 1 à [u64::MAX]
pub struct PageId(pub(super)NonZero<u64>);

impl PageId {
    pub(crate) fn new(value: u64) -> Self {
        Self(NonZero::new(value).expect("page id must be > 0"))
    }
}

impl Mul<PageSize> for PageId {
    type Output = PageLocation;

    fn mul(self, rhs: PageSize) -> Self::Output {
        let rhs_u64: u64 = rhs.into();
        PageLocation((self.0.get() - 1) * u64::from(rhs_u64))
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

impl From<usize> for PageId {
    fn from(value: usize) -> Self {
        Self(NonZero::try_from(u64::try_from(value).unwrap()).expect("must be a non-zeroed value"))
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
        (*self) * (*page_size) + base
    }
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C, packed)]
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