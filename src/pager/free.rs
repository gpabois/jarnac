use std::ops::DerefMut;

use zerocopy_derive::*;
use zerocopy::TryFromBytes;

use super::{
    page::{OptionalPageId, PageId, PageKind},
    IPagerInternals, PagerResult,
};


#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
#[allow(dead_code)]
enum FreeKind {
    Free = PageKind::Free as u8
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente les données stockée dans une page libre.
pub struct FreePage {
    kind: FreeKind,
    next: OptionalPageId,
    // le reste de la page en tant que DST
    body: [u8]
}

impl FreePage
where
{
    /// Initialise une page libre.
    pub fn new<P>(page: &mut P) -> std::io::Result<&'_ mut Self> 
    where P: DerefMut<Target = [u8]>
    {
        let slice = page.deref_mut();
        slice.fill(0);
        slice[0] = PageKind::Free as u8;

        Ok(Self::try_mut_from_bytes(slice).unwrap())
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.next.into()
    }

    pub fn set_next(&mut self, next: Option<PageId>) {
        self.next = next.into()
    }
}

/// Empile une nouvelle page libre dans la liste chaînée
pub(super) fn push_free_page<Pager: IPagerInternals>(
    pager: &Pager,
    pid: &PageId,
) -> PagerResult<()> {
    let mut page = pager.get_mut_page(pid)?;
    page.fill(0);

    FreePage::new(&mut page)?;

    pager.set_free_head(Some(*pid));

    Ok(())
}

/// Dépile une page libre dans la liste chaînée
pub(super) fn pop_free_page<Pager: IPagerInternals>(pager: &Pager) -> PagerResult<Option<PageId>> {
    if let Some(next) = pager.free_head() {
        let new_head = FreePage::try_ref_from_bytes(&pager.get_page(&next)?).unwrap().get_next();
        pager.set_free_head(new_head);
        return Ok(Some(next));
    }

    Ok(None)
}

