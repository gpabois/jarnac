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

use zerocopy::{FromBytes, IntoBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{pager::{cell::{Cell, CellCapacity, CellId, CellPage, CellPageHeader}, page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, PageSlice, RefPage}, var::{Var, VarData}, PagerResult}, value::{numeric::Numeric, Value, ValueBuf, ValueKind}};

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

impl BPTreeLeaf<RefPage<'_>> {
    pub fn id(&self) -> &PageId {
        self.as_page().id()
    }
}

impl BPTreeLeaf<MutPage<'_>> {
    pub fn id(&self) -> &PageId {
        self.as_page().id()
    }
}

impl BPTreeLeaf<&mut MutPage<'_>> {
    pub fn id(&self) -> &PageId {
        self.as_page().id()
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
    pub fn new(mut page: Page, k: u8, cell_size: PageSize) -> PagerResult<Self> {
        // initialisation bas-niveau de la page.
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTreeLeaf as u8;

        // initialise le sous-système de cellules
        let reserved: u16 = size_of::<BPTreeLeafHeader>().try_into().unwrap();
        CellPage::new(&mut page, cell_size, k.into(), reserved.into())?;

        Self::try_from(page)
    }

    #[allow(dead_code)]
    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item=BPTreeLeafCell<&'a mut PageSlice>>
    {
        self
        .as_cells()
        .iter()
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }    

    pub fn borrow_mut_cell(&mut self, cid: &CellId) -> BPTreeLeafCell<&mut PageSlice> {
        self.as_mut_cells()
        .borrow_mut_cell(cid)
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        }).unwrap()
    }

    pub fn insert_before(&mut self, before: &CellId) -> PagerResult<CellId> {
        self.as_mut_cells().insert_before(before)
    }

    pub fn push(&mut self) -> PagerResult<CellId> {
        self.as_mut_cells().push()
    }

    pub fn split_into<'a, P2>(&mut self, dest: &mut BPTreeLeaf<P2>) -> PagerResult<&Value> where P2: AsMutPageSlice {
        let at = self.len().div(2);
        self.as_mut_cells().split_at_into(dest.as_mut_cells(), at)?;
        let key = self.iter().last().map(|cell| cell.borrow_key()).unwrap();
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

    pub fn len(&self) -> CellCapacity {
        self.as_cells().len()
    }

    pub fn borrow_cell(&self, cid: &CellId) -> BPTreeLeafCell<&PageSlice> {
        self.as_cells()
        .borrow_cell(cid)
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        }).unwrap()
    }

    /// Itère sur les cellules du noeud.
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=&'a BPTreeLeafCell<PageSlice>>
    {
        self.as_cells()
        .iter()
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        })
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
pub struct BPTreeLeafCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice + ?Sized;

impl<Slice> AsRef<PageSlice> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn as_ref(&self) -> &PageSlice {
        let idx = usize::from(self.borrow_key().kind().size().unwrap())..;
        &self.0.as_slice()[idx]
    }
}

impl<Slice> PartialOrd<Value> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn partial_cmp(&self, other: &Value) -> Option<std::cmp::Ordering> {
        self.borrow_key().partial_cmp(other)
    }
}

impl<Slice> PartialEq<Value> for BPTreeLeafCell<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn eq(&self, other: &Value) -> bool {
        self.borrow_key().eq(other)
    }
}

impl<Slice> BPTreeLeafCell<Slice> 
where Slice: AsRefPageSlice + ?Sized
{
    pub fn cid(&self) -> CellId {
        self.as_cell().id()
    }

    pub fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }

    pub fn borrow_key(&self) -> &Value {
        let kind = ValueKind::from(self.as_cell().borrow_content().as_bytes()[0]);
        let bytes = kind.get_slice(self.as_cell().borrow_content());
        unsafe {
            std::mem::transmute(bytes)
        }
    }

    pub fn borrow_value(&self) -> &Var<PageSlice> {
        let value_bytes = &self.as_cell().borrow_content()[size_of::<Numeric>()..];        
        unsafe {
            std::mem::transmute(value_bytes)
        }     
    }
    
}

impl<Slice> BPTreeLeafCell<Slice> 
where Slice: AsMutPageSlice + ?Sized
{
    pub fn as_mut_cell(&mut self) -> &mut Cell<Slice> {
        &mut self.0
    }

    pub fn borrow_mut_key(&mut self) -> &mut Value {
        let kind = ValueKind::from(self.as_cell().borrow_content().as_bytes()[0]);
        unsafe {
            std::mem::transmute(kind.get_mut_slice(self.as_mut_cell().borrow_mut_content()))
        }
    }
    
    pub fn borrow_mut_value(&mut self) -> &mut Var<PageSlice> {
        let value_bytes = &mut self.as_mut_cell().borrow_mut_content()[size_of::<Numeric>()..];
        unsafe {
            std::mem::transmute(value_bytes)
        }
    }
}
