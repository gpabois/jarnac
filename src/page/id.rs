use std::num::NonZero;

use zerocopy_derive::*;
pub type PageId = u64;

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C, packed)]
pub struct OptionalPageId(Option<NonZero<u64>>);

impl AsRef<Option<PageId>> for OptionalPageId {
    fn as_ref(&self) -> &Option<PageId> {
        unsafe { std::mem::transmute(self) }
    }
}

impl AsMut<Option<PageId>> for OptionalPageId {
    fn as_mut(&mut self) -> &mut Option<PageId> {
        unsafe { std::mem::transmute(self) }
    }
}

impl From<Option<PageId>> for OptionalPageId {
    fn from(value: Option<PageId>) -> Self {
        unsafe { std::mem::transmute(value.and_then(NonZero::new)) }
    }
}

impl From<OptionalPageId> for Option<PageId> {
    fn from(value: OptionalPageId) -> Self {
        value.0.map(|v| v.get())
    }
}

