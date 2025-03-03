use std::{mem::forget, ops::{Deref, DerefMut, Index, IndexMut}, slice::SliceIndex};

use zerocopy::{Immutable, KnownLayout, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable};

use crate::pager::page::descriptor::PageDescriptor;

use super::{AsMutPageSlice, AsRefPageSlice, MutPage, RefPage};

#[derive(FromBytes, Immutable)]
#[repr(C)]
/// A slice of a page
pub struct PageSlice([u8]);

impl PageSlice {
    pub fn try_into_ref<T>(&self) -> Result<&T, zerocopy::ConvertError<zerocopy::AlignmentError<&[u8], T>, zerocopy::SizeError<&[u8], T>, zerocopy::ValidityError<&[u8], T>>>
    where T: TryFromBytes + KnownLayout + Immutable + ?Sized
    {
        T::try_ref_from_bytes(&self.0)
    }

    pub fn try_into_mut<T>(&mut self) ->  Result<&mut T, zerocopy::ConvertError<zerocopy::AlignmentError<&mut [u8], T>, zerocopy::SizeError<&mut [u8], T>, zerocopy::ValidityError<&mut [u8], T>>>
    where T: TryFromBytes + KnownLayout + Immutable + ?Sized
    {
        T::try_mut_from_bytes(&mut self.0)
    }
}

impl BorrowPageSlice for PageSlice {
    fn borrow_page_slice<Idx: PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        self.index(idx)
    }
}

impl BorrowMutPageSlice for PageSlice {
    fn borrow_mut_page_slice<Idx: PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice {
        self.index_mut(idx)
    }
}

impl<Idx> Index<Idx> for PageSlice where Idx: SliceIndex<[u8], Output = [u8]> {
    type Output = PageSlice;

    fn index(&self, index: Idx) -> &Self::Output {
        unsafe {
            std::mem::transmute(&self.0[index])
        }
    }
}

impl<Idx> IndexMut<Idx> for PageSlice where Idx: SliceIndex<[u8], Output = [u8]> {
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        unsafe {
            std::mem::transmute(&mut self.0[index])
        }   
    }
}

impl Deref for PageSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PageSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<&[u8]> for &PageSlice {
    fn from(value: &[u8]) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl From<&mut [u8]> for &mut PageSlice {
    fn from(value: &mut [u8]) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

pub trait PageSliceIndex: SliceIndex<[u8], Output = [u8]> {}
impl<Idx> PageSliceIndex for Idx where Idx: SliceIndex<[u8], Output = [u8]>{}

/// Transforme une référence sur une page en référence sur une tranche de la page.
pub trait IntoRefPageSlice {
    type RefPageSlice: AsRefPageSlice;

    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice;
}

impl<'pager> IntoRefPageSlice for RefPage<'pager> {
    type RefPageSlice = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice {
        unsafe {
            let slice = RefPageSlice {
                inner: self.0.clone(), 
                slice: &self.0.get_content_ptr().as_ref()[idx]
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
                slice: &self.0.get_content_ptr().as_ref()[idx]
            };
            slice
        }
    }    
}

impl<'pager> IntoRefPageSlice for RefPageSlice<'pager> {
    type RefPageSlice = Self;

    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::RefPageSlice {
        let slice = Self {
            inner: self.inner.clone(), 
            slice: &self.slice[idx]
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
    type MutPageSlice: AsMutPageSlice;

    fn into_mut_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::MutPageSlice;
}

impl<'pager> IntoMutPageSlice for MutPage<'pager> {
    type MutPageSlice = MutPageSlice<'pager>;
    
    fn into_mut_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::MutPageSlice {
        unsafe {
            let slice = MutPageSlice {
                inner: self.inner.clone(), 
                slice: &mut self.inner.get_content_ptr().as_mut()[idx]
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

/// Emprunte une référence sur une tranche d'une page.
pub trait BorrowMutPageSlice {
    fn borrow_mut_page_slice<Idx: PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice;
}

/// Référence vers une tranche de données d'une page.
pub struct RefPageSlice<'pager>{
    pub(super) inner: PageDescriptor<'pager>, 
    pub(super) slice: &'pager PageSlice
}

impl AsRef<PageSlice> for RefPageSlice<'_> {
    fn as_ref(&self) -> &PageSlice {
        self.deref()
    }
}

impl Deref for RefPageSlice<'_> {
    type Target = PageSlice;

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}
impl Drop for RefPageSlice<'_> {
    fn drop(&mut self) {
        unsafe {
            self.inner.dec_rw_counter();
        }
    }
}

/// Une tranche mutable d'une page.
pub struct MutPageSlice<'pager>{
    pub(super) inner: PageDescriptor<'pager>, 
    pub(super) slice: &'pager mut PageSlice
}

impl AsRef<PageSlice> for MutPageSlice<'_> {
    fn as_ref(&self) -> &PageSlice {
        self.slice
    }
}

impl AsMut<PageSlice> for MutPageSlice<'_> {
    fn as_mut(&mut self) -> &mut PageSlice {
        self.slice
    }
}

impl<'pager> Into<RefPageSlice<'pager>> for MutPageSlice<'pager> {
    fn into(self) -> RefPageSlice<'pager> {
        unsafe {
            self.inner.reset_rw_counter();

            let slice = std::ptr::from_mut(self.slice).as_ref().unwrap();
        
            let slice = RefPageSlice {
                inner: self.inner.clone(),
                slice
            };
    
            forget(self);
    
            slice
        }

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
        unsafe  {
            self.inner.inc_rw_counter();
        }
    }
}