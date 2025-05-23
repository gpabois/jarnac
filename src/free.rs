use zerocopy::TryFromBytes;
use zerocopy_derive::*;

use crate::{
    page::{AsMutPageSlice, AsRefPageSlice, InPage, OptionalPageId, PageId, PageKind}, pager::{IPager, PagerDescription}, result::Result, tag::{DataArea, JarTag}
};


pub struct FreePage<Page>(Page)
where
    Page: AsRefPageSlice;

impl<Page> FreePage<Page>
where
    Page: AsRefPageSlice,
{
    /// Initialise une page libre.
    pub fn new(mut page: Page) -> Result<Self>
    where
        Page: AsMutPageSlice,
    {
        page.as_mut().fill(0);
        page.as_mut().as_mut_bytes()[0] = PageKind::Free as u8;

        Ok(Self(page))
    }

    /// Embarque la page en tant que page libre.
    pub fn try_from(page: Page) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::Free.assert(kind).map(|_| Self(page))
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.as_meta().next.into()
    }

    fn as_meta(&self) -> &FreePageMeta {
        FreePageMeta::try_ref_from_bytes(&self.0.as_ref()[FreePageMeta::AREA]).unwrap()
    }
}

impl<Page> FreePage<Page>
where
    Page: AsMutPageSlice,
{
    pub fn set_next(&mut self, next: Option<PageId>) {
        self.as_mut_meta().next = next.into()
    }

    fn as_mut_meta(&mut self) -> &mut FreePageMeta {
        FreePageMeta::try_mut_from_bytes(&mut self.0.as_mut()[FreePageMeta::AREA]).unwrap()
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente les données stockée dans une page libre.
pub struct FreePageMeta {
    next: OptionalPageId,
}

impl DataArea for FreePageMeta {
    const AREA: std::ops::Range<usize> = InPage::<Self>::AREA;
}

/// Empile une nouvelle page libre dans la liste chaînée
pub(super) fn push_free_page<'buf, Pager: IPager<'buf>>(
    pager: &Pager,
    desc: &mut PagerDescription,
    tag: &JarTag,
) -> Result<()> {
    let mut page = pager.borrow_mut_element(tag)?;
    page.fill(0);

    FreePage::new(&mut page)?;

    desc.set_free_head(Some(tag.page_id));

    Ok(())
}

/// Dépile une page libre dans la liste chaînée
pub(super) fn pop_free_page<'pager, Pager: IPager<'pager>>(
    pager: &Pager,
    desc: &mut PagerDescription
) -> Result<Option<JarTag>> {
    
    if let Some(next) = desc.get_free_head() {
        let next = pager.tag().in_page(next);
        let page = pager.borrow_element(&next).and_then(FreePage::try_from)?;
        let new_head = page.get_next();
        desc.set_free_head(new_head);
        return Ok(Some(next));
    }

    Ok(None)
}
