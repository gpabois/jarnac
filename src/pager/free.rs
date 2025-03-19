
use zerocopy_derive::*;
use zerocopy::TryFromBytes;

use super::{
    page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageId, PageKind}, IPagerInternals, PagerResult
};

pub struct FreePage<Page>(Page) where Page: AsRefPageSlice;

impl<Page> FreePage<Page> where Page: AsRefPageSlice {
    /// Initialise une page libre.
    pub fn new(mut page: Page) -> PagerResult<Self> 
    where Page: AsMutPageSlice
    {
        page.as_mut().fill(0);
        page.as_mut().as_mut_bytes()[0] = PageKind::Free as u8;

        Ok(Self(page))
    }

    /// Embarque la page en tant que page libre.
    pub fn try_from(page: Page) -> PagerResult<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::Free.assert(kind).map(|_| Self(page))
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.as_ref().next.into()
    }
}

impl<Page> FreePage<Page> where Page: AsMutPageSlice {
    pub fn set_next(&mut self, next: Option<PageId>) {
        self.as_mut().next = next.into()
    }
}

impl<Page> AsRef<FreePageData> for FreePage<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &FreePageData {
        FreePageData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}

impl<Page> AsMut<FreePageData> for FreePage<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut FreePageData {
        FreePageData::try_mut_from_bytes(self.0.as_mut()).unwrap()
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
#[allow(dead_code)]
enum FreeKind {
    Free = PageKind::Free as u8
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente les données stockée dans une page libre.
pub struct FreePageData {
    kind: FreeKind,
    next: OptionalPageId,
    // le reste de la page en tant que DST
    trailling: [u8]
}


/// Empile une nouvelle page libre dans la liste chaînée
pub(super) fn push_free_page<Pager: IPagerInternals>(
    pager: &Pager,
    pid: &PageId,
) -> PagerResult<()> {
    let mut page = pager.borrow_mut_page(pid)?;
    page.fill(0);

    FreePage::new(&mut page)?;

    pager.set_free_head(Some(*pid));

    Ok(())
}

/// Dépile une page libre dans la liste chaînée
pub(super) fn pop_free_page<Pager: IPagerInternals>(pager: &Pager) -> PagerResult<Option<PageId>> {
    if let Some(next) = pager.free_head() {
        let page = pager.borrow_page(&next).and_then(FreePage::try_from)?;
        let new_head = page.get_next();
        pager.set_free_head(new_head);
        return Ok(Some(next));
    }

    Ok(None)
}

