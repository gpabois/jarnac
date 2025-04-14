use std::ops::Add;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageLocation(pub(super) u64);

impl Add<u64> for PageLocation {
    type Output = PageLocation;

    fn add(mut self, rhs: u64) -> Self::Output {
        self.0 += rhs;
        self
    }
}

impl From<PageLocation> for u64 {
    fn from(value: PageLocation) -> Self {
        value.0
    }
}
