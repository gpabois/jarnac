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

use std::ops::{DerefMut, Div, Index, IndexMut, Range, RangeFrom};

use zerocopy::{FromBytes, IntoBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{pager::{cell::{Cell, CellCapacity, CellId, CellPage, CellPageHeader}, page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, PageSlice}, var::Var, IPager, PagerResult}, value::{Value, ValueKind}};

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

impl<Page> Index<&CellId> for BPTreeLeaf<Page> where Page: AsRefPageSlice {
    type Output = BPTreeLeafCell<PageSlice>;

    fn index(&self, index: &CellId) -> &Self::Output {
        self.borrow_cell(index).unwrap()
    }
}

impl<Page> IndexMut<&CellId> for BPTreeLeaf<Page> where Page: AsMutPageSlice {
    fn index_mut(&mut self, index: &CellId) -> &mut Self::Output {
        self.borrow_mut_cell(index).unwrap()
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

    pub fn borrow_mut_cell(&mut self, cid: &CellId) -> Option<&mut BPTreeLeafCell<PageSlice>> {
        self.as_mut_cells()
        .borrow_mut_cell(cid)
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }

    pub fn insert<Pager: IPager + ?Sized>(&mut self, key: &Value, value: &Value, pager: &Pager) -> PagerResult<()> {
        let before = self
        .iter()
        .filter(|&cell| cell <= &key)
        .map(|cell| cell.cid())
        .last();

        match before {
            Some(before) => self.insert_before(&before, &key, &value, pager)?,
            None => self.push(&key, &value, pager)?,
        };

        Ok(())
    }

    fn insert_before<Pager: IPager + ?Sized>(&mut self, before: &CellId, key: &Value, value: &Value, pager: &Pager) -> PagerResult<CellId> {
        let cid = self.as_mut_cells().insert_before(before)?;
        BPTreeLeafCell::initialise(&mut self[&cid], key, value, pager)?;
        Ok(cid)    
    }

    fn push<Pager: IPager + ? Sized>(&mut self, key: &Value, value: &Value, pager: &Pager) -> PagerResult<CellId> {
        let cid = self.as_mut_cells().push()?;
        BPTreeLeafCell::initialise(&mut self[&cid], key, value, pager)?;
        Ok(cid)
    }

    pub fn split_into<'a, P2>(&mut self, dest: &mut BPTreeLeaf<P2>) -> PagerResult<&Value> where P2: AsMutPageSlice {
        let at = self.len().div(2);
        println!("split node at {at}");
        self.as_mut_cells().split_at_into(dest.as_mut_cells(), at)?;

        println!("{0} | {1}", self.len(), dest.len());
        
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

    pub fn capacity(&self) -> CellCapacity {
        self.as_cells().capacity()
    }

    pub fn borrow_cell(&self, cid: &CellId) -> Option<&BPTreeLeafCell<PageSlice>> {
        self.as_cells()
        .borrow_cell(cid)
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        })
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
        let slice = &self.as_cell().borrow_content()[self.key_range()];
        Value::from_ref(slice)
    }

    pub fn borrow_value(&self) -> &Var<PageSlice> {
        let bytes = &self.as_cell().borrow_content()[self.value_range()];
        Var::from_ref_slice(bytes)   
    }

    fn value_range(&self) -> RangeFrom<usize> {
        let kind = ValueKind::from(self.as_cell().borrow_content().as_bytes()[0]);
        let full_size = kind.full_size().unwrap();
        return full_size..    
    }

    fn key_range(&self) -> Range<usize> {
        let kind = ValueKind::from(self.as_cell().borrow_content().as_bytes()[0]);
        let full_size = kind.full_size().unwrap();
        return 0..full_size
    }


}

impl<Slice> BPTreeLeafCell<Slice> where Slice: AsMutPageSlice + ?Sized {
    /// Initialise la cellule
    pub fn initialise<Pager: IPager + ?Sized>(cell: &mut Self, key: &Value, value: &Value, pager: &Pager) -> PagerResult<()> {
        cell.as_mut_cell().borrow_mut_content().as_mut_bytes()[0] = (*key.kind()).into();
        cell.borrow_mut_key().set(key);
        cell.borrow_mut_value().set(value, pager)?;
        Ok(())
    }

    pub fn as_mut_cell(&mut self) -> &mut Cell<Slice> {
        &mut self.0
    }

    pub fn borrow_mut_key(&mut self) -> &mut Value {
        let range = self.key_range();
        let bytes = &mut self.as_mut_cell().borrow_mut_content()[range];
        Value::from_mut(bytes)
    }
    
    pub fn borrow_mut_value(&mut self) -> &mut Var<PageSlice> {
        let range = self.value_range();
        let bytes = &mut self.as_mut_cell().borrow_mut_content()[range];
        Var::from_mut_slice(bytes)   
    }
}
