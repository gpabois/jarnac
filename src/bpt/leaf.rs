
use std::ops::Range;

use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    error::Error, knack::{Knack, KnackKind}, pager::{
        cell::{Cell, CellCapacity, CellId, CellPage, Cells, WithCells}, 
        page::{AsMutPageSlice, AsRefPageSlice, IntoRefPageSlice, MutPage, OptionalPageId, PageKind, PageSize, PageSlice, RefPage, RefPageSlice}, var::Var
    }, result::Result, tag::DataArea, utils::{MaybeSized, Shift, Sized}
};

use super::descriptor::BPlusTreeDescription;

pub struct BPlusTreeLeaf<Page>(CellPage<Page>);

pub type BPlusTreeLeafMut<'page> = BPlusTreeLeaf<MutPage<'page>>;
pub type BPlusTreeLeafRef<'page> = BPlusTreeLeaf<RefPage<'page>>;

impl<'buf> TryFrom<RefPage<'buf>> for BPlusTreeLeaf<RefPage<'buf>> {
    type Error = Error;

    fn try_from(page: RefPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}

impl<'buf> TryFrom<MutPage<'buf>> for BPlusTreeLeaf<MutPage<'buf>> {
    type Error = Error;

    fn try_from(page: MutPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}

impl<Page> BPlusTreeLeaf<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, desc: &BPlusTreeDescription) -> Result<Self> {
        page.as_mut_bytes()[0] = PageKind::BPlusTreeLeaf as u8;
        
        CellPage::new(
            page, 
            BPlusTreeLeaf::<()>::compute_cell_content_size(desc.key_kind(), desc.value_size),
            desc.k,
            BPlusTreeLeaf::<()>::reserved_space()
        ).map(Self)
    }
}

impl<'buf> BPlusTreeLeaf<RefPage<'buf>> {
    pub fn into_iter(self) -> impl Iterator<Item=BPTreeLeafCell<RefPageSlice<'buf>>> {
        self.0
        .into_iter()
        .map(BPTreeLeafCell)
    }

    pub fn into_cell(self, cid: &CellId) -> Option<BPTreeLeafCell<RefPageSlice<'buf>>> {
        Some(BPTreeLeafCell(self.0.into_cell(cid)?))
    }

    pub fn into_value(self, key: &Knack, key_kind: &Sized<KnackKind>, value_kind: &MaybeSized<KnackKind>) -> Option<> {
        self
        .into_iter()
        .filter(|cell| cell.borrow_key(key_kind) == key)
        .map(|cell| cell.into_value(key_kind, value_kind))
        .last()
    }
}

impl<Page> BPlusTreeLeaf<Page> where Page: AsRefPageSlice {
    pub fn iter(&self) -> impl Iterator<Item=&BPTreeLeafCell<PageSlice>> {
        self.0.iter().map(|cell| {
            <&BPTreeLeafCell<PageSlice>>::from(cell)
        })
    }
}

impl BPlusTreeLeaf<()> {
    pub fn compute_cell_content_size(key: Sized<KnackKind>, value_size: u16) -> u16 {
        u16::try_from(key.outer_size()).unwrap() + value_size
    }
    /// Calcule la taille disponible dans une cellule pour stocker une valeur.
    pub fn compute_available_value_space_size(page_size: PageSize, key: Sized<KnackKind>, k: CellCapacity) -> u16 {
        let key_size = u16::try_from(key.outer_size()).unwrap();
        let max_cell_size = Cells::compute_available_cell_content_size(page_size, Self::reserved_space(), k);
        max_cell_size - key_size
    }

    pub fn reserved_space() -> u16 {
        u16::try_from(size_of::<BPTreeLeafMeta>()).unwrap()
    }

    pub fn within_available_cell_space_size(page_size: PageSize, key: Sized<KnackKind>, value_size: u16, k: CellCapacity) -> bool {
        let content_size = Self::compute_cell_content_size(key, value_size);
        Cells::within_available_cell_space_size(page_size, Self::reserved_space(), content_size, k)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête d'une [feuille](self::BPTreeLeafPage).
pub struct BPTreeLeafMeta {
    pub(super) parent: OptionalPageId,
    pub(super) prev: OptionalPageId,
    pub(super) next: OptionalPageId,
}

impl DataArea for BPTreeLeafMeta {
    const AREA: std::ops::Range<usize> = WithCells::<Self>::AREA;
}

/// Cellule d'une feuille contenant une paire clé/valeur.
pub struct BPTreeLeafCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice + ?std::marker::Sized;

impl<'buf> BPTreeLeafCell<RefPageSlice<'buf>> {
    pub fn into_value(self, key_kind: &Sized<KnackKind>, value_kind: MaybeSized<KnackKind>) {
        match value_kind {
            MaybeSized::Sized(sized) => {
                let value_range = sized.as_area().shift(key_kind.outer_size());
                let value_bytes = self.0.into_content_slice().into_page_slice(value_range);
            },
            MaybeSized::Var(var_sized) => {
                let value_range = key_kind.1..;
                let value_bytes = self.0.into_content_slice().into_page_slice(value_range);
                let var = Var::from_owned_slice(value_bytes);
            },
        }
    }
}

impl<Slice> From<&Cell<Slice>> for &BPTreeLeafCell<Slice> where Slice: AsRefPageSlice + ?std::marker::Sized {
    fn from(value: &Cell<Slice>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl<Slice> From<&mut Cell<Slice>> for &BPTreeLeafCell<Slice> where Slice: AsMutPageSlice + ?std::marker::Sized {
    fn from(value: &mut Cell<Slice>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl<Slice> BPTreeLeafCell<Slice> 
where Slice: AsRefPageSlice + ?std::marker::Sized
{
    pub fn cid(&self) -> CellId {
        self.as_cell().id()
    }

    pub fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }

    pub fn borrow_key(&self, kind: &Sized<KnackKind>) -> &Knack {
        let slice = &self.as_cell().as_content_slice()[Self::key_area(kind)];
        Knack::from_ref(slice)
    }

    pub fn key_area(kind: &Sized<KnackKind>) -> Range<usize> {
        0..kind.outer_size()
    }
}

