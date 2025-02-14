use std::{mem::forget, ops::{Deref, DerefMut}};

use crate::pager::cache::CachedPage;

pub trait PageSliceData: AsRef<[u8]> {}

impl<'a> PageSliceData for &'a [u8] {}
impl<'a> PageSliceData for &'a mut [u8] {}

pub trait MutPageSliceData: AsRef<[u8]> {}
impl<'a> MutPageSliceData for &'a mut [u8] {}

/// Référence vers une tranche de données d'une page.
pub struct RefPageSlice<'pager>{
    pub(super) inner: CachedPage<'pager>, 
    pub(super) slice: &'pager [u8]
}

impl PageSliceData for RefPageSlice<'_> {}

impl Drop for RefPageSlice<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter -= 1;
    }
}

impl AsRef<[u8]> for RefPageSlice<'_> {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl Deref for RefPageSlice<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}


/// Une tranche mutable d'une page.
pub struct MutPageSlice<'pager>{
    pub(super) inner: CachedPage<'pager>, 
    pub(super) slice: &'pager mut [u8]
}

impl<'pager> PageSliceData for MutPageSlice<'pager> {}
impl<'pager> MutPageSliceData for MutPageSlice<'pager> {}
impl<'pager> Into<RefPageSlice<'pager>> for MutPageSlice<'pager> {
    fn into(mut self) -> RefPageSlice<'pager> {
        self.inner.rw_counter = 0;

        let slice = std::mem::take(&mut self.slice);

        let slice = RefPageSlice {
            inner: self.inner.clone(),
            slice
        };

        forget(self);

        slice
    }
}

impl<'pager> AsRef<[u8]> for MutPageSlice<'pager> {
    fn as_ref(&self) -> &[u8] {
        &self.slice
    }
}

impl<'pager> AsMut<[u8]> for MutPageSlice<'pager> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.slice
    }
}

impl<'pager> Deref for MutPageSlice<'pager> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

impl<'pager> DerefMut for MutPageSlice<'pager> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.slice
    }
}

impl Drop for MutPageSlice<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter += 1;
    }
}