use std::{mem::forget, ops::{Deref, DerefMut}, slice::SliceIndex};

use crate::pager::cache::CachedPage;

use super::{MutPage, RefPage};

pub type PageSlice = [u8];

pub trait PageSliceIndex: SliceIndex<[u8], Output = [u8]> {}
impl<Idx> PageSliceIndex for Idx where Idx: SliceIndex<[u8], Output = [u8]>{}

/// Transforme une référence sur une page en référence sur une tranche de la page.
pub trait IntoRefPageSlice {
    type RefPageSlice: PageSliceData;

    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice;
}

impl<'pager> IntoRefPageSlice for RefPage<'pager> {
    type RefPageSlice = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice {
        unsafe {
            let slice = RefPageSlice {
                inner: self.0.clone(), 
                slice: &self.0.content.as_ref()[idx]
            };
            forget(self);
            slice
        }
    }    
}

impl<'pager> IntoRefPageSlice for &RefPage<'pager> {
    type RefPageSlice = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice {
        unsafe {
            let slice = RefPageSlice {
                inner: self.0.clone(), 
                slice: &self.0.content.as_ref()[idx]
            };
            forget(self);
            slice
        }
    }    
}

impl<'pager> IntoRefPageSlice for RefPageSlice<'pager> {
    type RefPageSlice = Self;

    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice {
        let slice = Self {
            inner: self.inner.clone(), 
            slice: &self.slice.as_ref()[idx]
        };
        forget(self);
        slice
    }
}

impl<'pager> IntoRefPageSlice for MutPage<'pager> {
    type RefPageSlice = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice {
        self
        .into_ref()
        .into_page_slice(idx)
    }
}

pub trait IntoMutPageSlice {
    type MutPageSlice: MutPageSliceData;

    fn into_mut_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::MutPageSlice;
}

impl<'pager> IntoMutPageSlice for MutPage<'pager> {
    type MutPageSlice = MutPageSlice<'pager>;
    
    fn into_mut_page_slice<Idx: PageSliceIndex>(mut self, idx: Idx) -> Self::MutPageSlice {
        unsafe {
            let slice = MutPageSlice {
                inner: self.inner.clone(), 
                slice: &mut self.inner.content.as_mut()[idx]
            };
            forget(self);
            slice
        }
    }
}
impl<'pager> IntoMutPageSlice for MutPageSlice<'pager> {
    type MutPageSlice = Self;
    
    fn into_mut_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::MutPageSlice {
        unsafe {
            let slice = std::ptr::from_mut(&mut self.slice[idx]);
        
            let slice = MutPageSlice {
                inner: self.inner.clone(), 
                slice: slice.as_mut().unwrap()
            };
            forget(self);
            slice
        }
    }

}

/// Emprunte une référence sur une tranche d'une page.
pub trait BorrowPageSlice {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice;
}

impl BorrowPageSlice for RefPage<'_> {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        &self.as_ref()[idx]
    }
}

impl BorrowPageSlice for &RefPage<'_> {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        &self.as_ref()[idx]
    }
}

impl BorrowPageSlice for MutPage<'_> {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        &self.as_ref()[idx]
    }
}

impl BorrowPageSlice for &MutPage<'_> {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        &self.as_ref()[idx]
    }
}

impl BorrowPageSlice for &mut MutPage<'_> {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        &self.as_ref()[idx]
    }
}

/// Emprunte une référence sur une tranche d'une page.
pub trait BorrowMutPageSlice {
    fn borrow_mut_page_slice<Idx: PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice;
}

impl BorrowMutPageSlice for MutPage<'_> {
    fn borrow_mut_page_slice<Idx: PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice {
        &mut self.as_mut()[idx]
    }
}
impl BorrowMutPageSlice for MutPageSlice<'_> {
    fn borrow_mut_page_slice<Idx: PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice {
        &mut self.as_mut()[idx]
    }
}
impl BorrowMutPageSlice for &mut MutPageSlice<'_> {
    fn borrow_mut_page_slice<Idx: PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice {
        &mut self.as_mut()[idx]
    }
}

pub trait PageSliceData: AsRef<[u8]> {}
impl<'a> PageSliceData for &'a [u8] {}
impl<'a> PageSliceData for &'a mut [u8] {}

pub trait MutPageSliceData: AsRef<[u8]> + AsMut<[u8]> {}
impl<'a> MutPageSliceData for &'a mut [u8] {}

/// Référence vers une tranche de données d'une page.
pub struct RefPageSlice<'pager>{
    pub(super) inner: CachedPage<'pager>, 
    pub(super) slice: &'pager [u8]
}

impl PageSliceData for RefPageSlice<'_> {}
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
impl Drop for RefPageSlice<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter -= 1;
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