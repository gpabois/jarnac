
use std::ops::{Deref, DerefMut, Div};

use zerocopy::{FromBytes, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, KnownLayout, TryFromBytes};

use crate::{pager::{cell::{Cell, CellId, CellPage, CellPageHeader, CellSize}, page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageKind, PageSlice}, spill::VarData, PagerResult}, value::numeric::Numeric};

use super::BPTreeNodeKind;

/// Représente une feuille d'un arbre B+.
pub struct BPTreeLeaf<Page>(Page) where Page: AsRefPageSlice;

impl<Page> Deref for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    type Target = BPTreeLeafData;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<Page> DerefMut for BPTreeLeaf<Page> where Page: AsMutPageSlice {

    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<Page> AsRef<Page> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &Page {
        &self.0
    }
}

impl<Page> AsMut<Page> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut Page {
        &mut self.0
    }
}

impl<Page> AsRef<BPTreeLeafData> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeLeafData {
        BPTreeLeafData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}

impl<Page> AsMut<BPTreeLeafData> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeLeafData {
        BPTreeLeafData::try_mut_from_bytes(self.0.as_mut()).unwrap()
    }
}

impl<Page> AsRef<CellPage<Page>> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &CellPage<Page> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl<Page> AsMut<CellPage<Page>> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut CellPage<Page> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl<Page> From<Page> for BPTreeLeaf<Page> where Page: AsRefPageSlice
{
    fn from(value: Page) -> Self {
        Self(value)
    }
}

impl<Page> BPTreeLeaf<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, k: u8, cell_size: CellSize) -> Self {
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTreeLeaf as u8;

        let mut leaf: Self = page.into();
        let data: &mut BPTreeLeafData = leaf.as_mut();
        data.header.cell_spec = CellPageHeader::new(
            cell_size, 
            k,
            size_of::<BPTreeLeafPageHeader>().try_into().unwrap()
        );
        leaf
    }

    #[allow(dead_code)]
    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item=BPTreeLeafCell<&'a mut PageSlice>>
    {
        let cp: &mut CellPage<_> = self.as_mut();
        cp
        .iter_ids()
        .map(|cid| unsafe {
            std::mem::transmute(cp.borrow_cell(&cid).unwrap())
        })
        .map(BPTreeLeafCell)
    }    

    pub fn borrow_mut_cell(&mut self, cid: &CellId) -> BPTreeLeafCell<&mut PageSlice> {
        let cp: &mut CellPage<_> = self.as_mut();
        let cell = cp.borrow_mut_cell(cid).unwrap();
        BPTreeLeafCell(cell)
    }

    pub fn insert_before(&mut self, before: &CellId) -> PagerResult<CellId> {
        let cp: &mut CellPage<_> = self.as_mut();
        cp.insert_before(before)
    }

    pub fn push(&mut self) -> PagerResult<CellId> {
        let cp: &mut CellPage<_> = self.as_mut();
        cp.push()     
    }

    pub fn as_mut_cells(&mut self) -> &mut CellPage<Page> {
        self.as_mut()
    }

    pub fn split_into<P2>(&mut self, dest: &mut BPTreeLeaf<P2>) -> PagerResult<Numeric> where P2: AsMutPageSlice {
        let at = self.len().div(2);
        self.as_mut_cells().split_at_into(dest.as_mut_cells(), at)?;

        let key = self.iter().last().map(|cell| cell.borrow_key().clone()).unwrap();

        Ok(key)
    }
}

impl<Page> BPTreeLeaf<Page> where Page: AsRefPageSlice {

    pub fn as_page(&self) -> &Page {
        &self.0
    }

    /// Vérifie si la feuille est pleine.
    pub fn is_full(&self) -> bool {
        let data: &BPTreeLeafData = self.as_ref();
        data.is_full()
    }

    pub fn len(&self) -> u8 {
        let data: &BPTreeLeafData = self.as_ref();
        data.header.cell_spec.len()
    }

    pub fn borrow_cell(&self, cid: &CellId) -> BPTreeLeafCell<&PageSlice> {
        let cp: &CellPage<_> = self.as_ref();
        let cell = cp.borrow_cell(cid).unwrap();
        BPTreeLeafCell(cell)
    }

    /// Itère sur les cellules du noeud.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=BPTreeLeafCell<&'a PageSlice>>
    {
        let cp: &CellPage<_> = self.as_ref();
        cp
        .iter_ids()
        .flat_map(move |cid| cp.borrow_cell(&cid))
        .map(BPTreeLeafCell)
    }

}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// En-tête d'une [feuille](self::BPTreeLeafPage).
pub struct BPTreeLeafPageHeader {
    pub(crate) kind: BPTreeNodeKind,
    cell_spec: CellPageHeader,
    pub(super) parent: OptionalPageId,
    pub(super) prev: OptionalPageId,
    pub(super) next: OptionalPageId,
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Données d'une feuille d'un arbre B+
pub struct BPTreeLeafData {
    pub(super) header: BPTreeLeafPageHeader,
    cells: [u8],
}

impl BPTreeLeafData {
    pub fn is_full(&self) -> bool {
        self.header.cell_spec.is_full()
    }
}

/// Cellule d'une feuille contenant une paire clé/valeur.
pub struct BPTreeLeafCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice;

impl<Slice> AsRef<PageSlice> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice {
    fn as_ref(&self) -> &PageSlice {
        let idx = usize::from(self.borrow_key().kind().size())..;
        &self.0.as_slice()[idx]
    }
}

impl<Slice> AsRef<BPTreeLeafCellData> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeLeafCellData {
        BPTreeLeafCellData::ref_from_bytes(self.0.as_slice()).unwrap()
    }
}

impl<Slice> AsMut<BPTreeLeafCellData> for BPTreeLeafCell<Slice> where Slice: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeLeafCellData {
        BPTreeLeafCellData::try_mut_from_bytes(self.0.as_mut_slice()).unwrap()
    }
}

impl<Slice> PartialOrd<Numeric> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice {
    fn partial_cmp(&self, other: &Numeric) -> Option<std::cmp::Ordering> {
        let data: &BPTreeLeafCellData = self.as_ref();
        data.key.partial_cmp(other)
    }
}


impl<Slice> PartialEq<Numeric> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice {
    fn eq(&self, other: &Numeric) -> bool {
        let data: &BPTreeLeafCellData = self.as_ref();
        data.key.eq(other)
    }
}

impl<Slice> BPTreeLeafCell<Slice> 
where Slice: AsRefPageSlice
{
    pub fn cid(&self) -> CellId {
        self.0.cid
    }

    pub fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }

    pub fn borrow_key(&self) -> &Numeric {
        let data: &BPTreeLeafCellData = self.as_ref();
        &data.key
    }

    #[allow(dead_code)]
    pub fn borrow_value(&self) -> &VarData {
        let data: &BPTreeLeafCellData = self.as_ref();
        &data.value      
    }
}

impl<Slice> BPTreeLeafCell<Slice> 
where Slice: AsMutPageSlice
{

    pub fn borrow_mut_key(&mut self) -> &mut Numeric {
        let data: &mut BPTreeLeafCellData = self.as_mut();
        &mut data.key
    }
    
    pub fn borrow_mut_value(&mut self) -> &mut VarData {
        let data: &mut BPTreeLeafCellData = self.as_mut();
        &mut data.value      
    }
}

#[derive(FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct BPTreeLeafCellData {
    key: Numeric,
    value: VarData
}
