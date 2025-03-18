use super::{BorrowMutPageSlice, BorrowPageSlice, PageSlice};

pub trait AsRefPageSlice: AsRef<PageSlice> {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref().as_ref()
    }
}

impl<U> AsRefPageSlice for U where U: AsRef<PageSlice> {}
impl<U> BorrowPageSlice for U where U: AsRefPageSlice {
    fn borrow_page_slice<Idx: super::PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        self.as_ref().borrow_page_slice(idx)
    }
}

pub trait AsMutPageSlice: AsMut<PageSlice> + AsRefPageSlice {
    fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.as_mut().as_mut()
    }
}
impl<U> AsMutPageSlice for U where U: AsMut<PageSlice> + AsRefPageSlice {}
impl<U> BorrowMutPageSlice for U where U: AsMut<PageSlice> {
    fn borrow_mut_page_slice<Idx: super::PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice {
        self.as_mut().borrow_mut_page_slice(idx)
    }
}

