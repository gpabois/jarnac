//! Module définissant la feuille d'un arbre B+.
//! 
//! # Layout d'une page feuille 
//! |-------------------|-----------|
//! | PageKind          | 1 byte    |
//! | CellPageHeader    | 9 bytes   | Entête
//! | BPTreeLeafHeader  | 24 bytes  |
//! |-------------------|-----------|
//! | CellHeader        | 2 bytes   | 
//! | Key               | 17 bytes  | Cellule d'une feuille (x K)
//! | Value             | Variable  | < définit par data_size
//! |-------------------|-----------|

use std::ops::{DerefMut, Div, Range};

use zerocopy::{FromBytes, IntoBytes, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::{pager::{cell::{Cell, CellId, CellPage, CellPageHeader, CellSize}, page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageId, PageKind, PageSlice}, var::{Var, VarData}, PagerResult}, value::numeric::Numeric};

pub const LEAF_HEADER_RANGE_BASE: usize = size_of::<CellPageHeader>() + 1;
pub const LEAF_HEADER_RANGE: Range<usize> = LEAF_HEADER_RANGE_BASE..(LEAF_HEADER_RANGE_BASE + size_of::<BPTreeLeafHeader>());

/// Représente une feuille d'un arbre B+.
pub struct BPTreeLeaf<Page>(CellPage<Page>) where Page: AsRefPageSlice;

impl<Page> BPTreeLeaf<Page> where Page: AsRefPageSlice {
    pub fn try_from(page: Page) -> PagerResult<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(|_| Self(CellPage::from(page)))
    }

    fn as_cells(&self) -> &CellPage<Page> {
        self.as_ref()
    }

}

impl<Page> BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut_page(&mut self) -> &mut Page {
        self.0.as_mut()
    }

    pub fn as_mut_header(&mut self) -> &mut BPTreeLeafHeader {
        self.as_mut()
    }

    fn as_mut_cells(&mut self) -> &mut CellPage<Page> {
        self.as_mut()
    }
}


impl<Page> AsRef<Page> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &Page {
        self.0.as_ref()
    }
}
impl<Page> AsMut<Page> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut Page {
        self.0.as_mut()
    }
}

impl<Page> AsRef<BPTreeLeafHeader> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeLeafHeader{
        BPTreeLeafHeader::ref_from_bytes(&self.as_page().as_ref()[LEAF_HEADER_RANGE]).unwrap()
    }
}

impl<Page> AsMut<BPTreeLeafHeader> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeLeafHeader {
        BPTreeLeafHeader::mut_from_bytes(&mut self.as_mut_page().as_mut()[LEAF_HEADER_RANGE]).unwrap()
    }
}

impl<Page> AsRef<CellPage<Page>> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &CellPage<Page> {
        &self.0
    }
}

impl<Page> AsMut<CellPage<Page>> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut CellPage<Page> {
        &mut self.0
    }
}


impl<Page> BPTreeLeaf<Page> where Page: AsMutPageSlice {
    /// Crée une nouvelle feuille d'un arbre B+.
    pub fn new(mut page: Page, k: u8, cell_size: CellSize) -> Self {
        // initialisation bas-niveau de la page.
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTreeLeaf as u8;

        // initialise le sous-système de cellules
        let base: u16 = (1 + size_of::<CellPageHeader>() + size_of::<BPTreeLeafHeader>()).try_into().unwrap();
        CellPage::new(&mut page, cell_size, k, base);

        Self::try_from(page).expect("not a b+ leaf node")
    }

    #[allow(dead_code)]
    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item=BPTreeLeafCell<&'a mut PageSlice>>
    {
        self
        .as_cells()
        .iter_ids()
        .map(|cid| unsafe {
            std::mem::transmute(self.borrow_cell(&cid))
        })
    }    

    pub fn borrow_mut_cell(&mut self, cid: &CellId) -> BPTreeLeafCell<&mut PageSlice> {
        self.as_mut_cells().borrow_mut_cell(cid).map(BPTreeLeafCell).unwrap()
    }

    pub fn insert_before(&mut self, before: &CellId) -> PagerResult<CellId> {
        self.as_mut_cells().insert_before(before)
    }

    pub fn push(&mut self) -> PagerResult<CellId> {
        self.as_mut_cells().push()
    }

    pub fn split_into<P2>(&mut self, dest: &mut BPTreeLeaf<P2>) -> PagerResult<Numeric> where P2: AsMutPageSlice {
        let at = self.len().div(2);
        self.as_mut_cells().split_at_into(dest.as_mut_cells(), at)?;

        let key = self.iter().last().map(|cell| cell.borrow_key().clone()).unwrap();

        Ok(key)
    }

    pub fn set_next(&mut self, next: Option<PageId>) {
        self.as_mut_header().next = next.into()
    }

    pub fn set_prev(&mut self, prev: Option<PageId>) {
        self.as_mut_header().prev = prev.into()
    }

    pub fn set_parent(&mut self, parent: Option<PageId>) {
        self.as_mut_header().parent = parent.into()
    }
    
}

impl<Page> BPTreeLeaf<Page> where Page: AsRefPageSlice {

    pub fn as_page(&self) -> &Page {
        self.0.as_ref()
    }

    pub fn as_header(&self) -> &BPTreeLeafHeader {
        self.as_ref()
    }

    pub fn get_next(&self) -> Option<PageId> {
        self.as_header().next.into()
    }

    pub fn get_prev(&self) -> Option<PageId> {
        self.as_header().prev.into()
    }

    pub fn get_parent(&self) -> Option<PageId> {
        self.as_header().parent.into()
    }

    /// Vérifie si la feuille est pleine.
    pub fn is_full(&self) -> bool {
        self.as_cells().is_full()
    }

    pub fn len(&self) -> u8 {
        self.as_cells().len()
    }

    pub fn borrow_cell(&self, cid: &CellId) -> BPTreeLeafCell<&PageSlice> {
        self.as_cells().borrow_cell(cid).map(BPTreeLeafCell).unwrap()
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

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête d'une [feuille](self::BPTreeLeafPage).
pub struct BPTreeLeafHeader {
    pub(super) parent: OptionalPageId,
    pub(super) prev: OptionalPageId,
    pub(super) next: OptionalPageId,
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
        BPTreeLeafCellData::try_ref_from_bytes(self.0.as_slice()).unwrap()
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
        self.borrow_key().eq(other)
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
        let key_bytes = &self.as_cell().borrow_content()[0..size_of::<Numeric>()];
        Numeric::ref_from_bytes(&key_bytes).unwrap()
    }

    pub fn borrow_value(&self) -> &Var<PageSlice> {
        let value_bytes = &self.as_cell().borrow_content()[size_of::<Numeric>()..];        
        unsafe {
            std::mem::transmute(value_bytes)
        }     
    }
    
}

impl<Slice> BPTreeLeafCell<Slice> 
where Slice: AsMutPageSlice
{
    pub fn as_mut_cell(&mut self) -> &mut Cell<Slice> {
        &mut self.0
    }

    pub fn borrow_mut_key(&mut self) -> &mut Numeric {
        let key_bytes = &mut self.as_mut_cell().borrow_mut_content()[0..size_of::<Numeric>()];
        Numeric::mut_from_bytes(key_bytes).unwrap()
    }
    
    pub fn borrow_mut_value(&mut self) -> &mut Var<PageSlice> {
        let value_bytes = &mut self.as_mut_cell().borrow_mut_content()[size_of::<Numeric>()..];
        unsafe {
            std::mem::transmute(value_bytes)
        }
    }
}

#[derive(TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct BPTreeLeafCellData {
    key: Numeric,
    value: VarData
}
