use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{error::Error, pager::{cell::{CellCapacity, CellPage, Cells}, page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, RefPage}}, result::Result, utils::Sized, value::ValueKind};

use super::descriptor::BPlusTreeDescription;

pub struct BPlusTreeInterior<Page>(CellPage<Page>);
pub type BPlusTreeInteriorMut<'page> = BPlusTreeInterior<MutPage<'page>>;
pub type BPlusTreeInteriorRef<'page> = BPlusTreeInterior<RefPage<'page>>;

impl<'buf> TryFrom<RefPage<'buf>> for BPlusTreeInterior<RefPage<'buf>> {
    type Error = Error;

    fn try_from(page: RefPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeInterior.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}

impl<'buf> TryFrom<MutPage<'buf>> for BPlusTreeInterior<MutPage<'buf>> {
    type Error = Error;

    fn try_from(page: MutPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}

impl<Page> BPlusTreeInterior<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, desc: &BPlusTreeDescription) -> Result<Self> {
        page.as_mut_bytes()[0] = PageKind::BPlusTreeInterior as u8;
        CellPage::new(
            page, 
            BPlusTreeInterior::<()>::compute_cell_content_size(desc.key_kind()),
            desc.k,
            BPlusTreeInterior::<()>::reserved_space()
        ).map(Self)
    }
}


impl BPlusTreeInterior<()> {
    pub fn compute_cell_content_size(key: Sized<ValueKind>) -> PageSize {
        u16::try_from(size_of::<PageId>()+ key.outer_size()).unwrap()
    }

    pub fn within_available_cell_space_size(page_size: PageSize, key: Sized<ValueKind>, k: CellCapacity) -> bool {
        let content_size = Self::compute_cell_content_size(key);
        Cells::within_available_cell_space_size(page_size, Self::reserved_space(), content_size, k)
    }

    pub fn reserved_space() -> u16 {
        u16::try_from(size_of::<BPTreeInteriorMeta>()).unwrap()
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug)]
#[repr(C, packed)]
/// L'entête d'un noeud d'un arbre B+
pub struct BPTreeInteriorMeta {
    /// Pointeur vers le noeud parent
    pub(super) parent: OptionalPageId,
    /// Pointeur vers le noeud enfant le plus à droite
    pub(super) tail: OptionalPageId,
}
