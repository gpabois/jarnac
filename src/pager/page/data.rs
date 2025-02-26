use super::{BorrowMutPageSlice, BorrowPageSlice, PageSlice};

pub trait AsRefPageSlice: AsRef<PageSlice> {}
impl AsRef<PageSlice> for &PageSlice {
    fn as_ref(&self) -> &PageSlice {
        self
    }
}
impl AsRef<PageSlice> for &mut PageSlice {
    fn as_ref(&self) -> &PageSlice {
        self
    }
}
impl<U> AsRefPageSlice for U where U: AsRef<PageSlice> {}
impl<U> BorrowPageSlice for U where U: AsRefPageSlice {
    fn borrow_page_slice<Idx: super::PageSliceIndex>(&self, idx: Idx) -> &PageSlice {
        self.as_ref().borrow_page_slice(idx)
    }
}

pub trait AsMutPageSlice: AsMut<PageSlice> + AsRefPageSlice {}
impl AsMut<PageSlice> for &mut PageSlice {
    fn as_mut(&mut self) -> &mut PageSlice {
        self
    }
}
impl<U> AsMutPageSlice for U where U: AsMut<PageSlice> + AsRefPageSlice {}
impl<U> BorrowMutPageSlice for U where U: AsMut<PageSlice> {
    fn borrow_mut_page_slice<Idx: super::PageSliceIndex>(&mut self, idx: Idx) -> &mut PageSlice {
        self.as_mut().borrow_mut_page_slice(idx)
    }
}

