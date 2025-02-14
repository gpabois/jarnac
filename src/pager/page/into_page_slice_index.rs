use std::{mem::forget, slice::SliceIndex};

use super::{page_data::PageData, page_slice_data::PageSliceData, MutPage, MutPageData, MutPageSlice, MutPageSliceData, RefPage, RefPageSlice};

pub trait PageSliceIndex: SliceIndex<[u8], Output = [u8]> {}
impl<Idx> PageSliceIndex for Idx where Idx: SliceIndex<[u8], Output = [u8]>{}

/// Transforme une référence sur une page en référence sur une tranche de la page.
pub trait IntoPageSlice {
    type Output: PageSliceData;

    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output;
}

impl<'a, U> IntoPageSlice for &'a U where U: PageData {
    type Output = &'a [u8];
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output {
        &self.as_ref()[idx]
    }
}

impl<'pager> IntoPageSlice for RefPage<'pager>
{
    type Output = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output {
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

impl<'pager> IntoPageSlice for MutPage<'pager>
{
    type Output = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output {
        self
        .into_ref()
        .into_page_slice(idx)
    }
}

impl<'a, U> IntoPageSlice for &'a mut U where U: MutPageData
{
    type Output = &'a mut [u8];
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output {
        &mut self.as_mut()[idx]
    }

}

impl<'pager> IntoPageSlice for MutPageSlice<'pager> 
{
    type Output = RefPageSlice<'pager>;
    
    fn into_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output {
        let ref_page_slice: RefPageSlice<'pager> = self.into();
        ref_page_slice.into_page_slice(idx)
    }
}

pub trait IntoMutPageSlice {
    type Output: MutPageSliceData;

    fn into_mut_page_slice<Idx: PageSliceIndex>(self, idx: Idx) -> Self::Output;
}

impl<'pager> IntoMutPageSlice for MutPage<'pager>
{
    type Output = MutPageSlice<'pager>;
    
    fn into_mut_page_slice<Idx: PageSliceIndex>(mut self, idx: Idx) -> Self::Output {
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