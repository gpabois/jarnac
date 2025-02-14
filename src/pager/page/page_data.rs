pub trait PageData: AsRef<[u8]> {}
impl<'a, U> PageData for &'a U where U: PageData {}

pub trait MutPageData: AsMut<[u8]> + PageData {}
impl<'a, U> PageData for &'a mut U where U: MutPageData {}
impl<'a, U> MutPageData for &'a mut U where U: MutPageData {}

