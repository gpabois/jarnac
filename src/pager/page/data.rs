use super::{BorrowMutPageSlice, BorrowPageSlice, MutPage, RefPage};

pub trait RefPageData: AsRef<[u8]> + BorrowPageSlice  {}
impl RefPageData for RefPage<'_> {}
impl RefPageData for &RefPage<'_> {}

impl RefPageData for MutPage<'_> {}
impl RefPageData for &MutPage<'_> {}
impl RefPageData for &mut MutPage<'_> {}

pub trait MutPageData: AsMut<[u8]> + BorrowMutPageSlice + RefPageData {}
impl MutPageData for MutPage<'_> {}

