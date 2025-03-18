use std::{cmp::Ordering, ops::{DerefMut, Div, Index, IndexMut, Range}};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{pager::{cell::{Cell, CellId, CellPage, CellPageHeader}, page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, PageSlice}, PagerResult}, value::{Value, ValueKind}};

pub const LEAF_HEADER_RANGE_BASE: usize = size_of::<CellPageHeader>() + 1;
pub const LEAF_HEADER_RANGE: Range<usize> = LEAF_HEADER_RANGE_BASE..(LEAF_HEADER_RANGE_BASE + size_of::<BPTreeInteriorHeader>());

pub struct BPTreeInterior<Page>(CellPage<Page>) where Page: AsRefPageSlice;

impl<Page> Index<&CellId> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    type Output = BPTreeInteriorCell<PageSlice>;

    fn index(&self, index: &CellId) -> &Self::Output {
        self.borrow_cell(index).unwrap()
    }
}

impl<Page> IndexMut<&CellId> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn index_mut(&mut self, index: &CellId) -> &mut Self::Output {
        self.borrow_mut_cell(index).unwrap()
    }
}

impl<Page> BPTreeInterior<Page> where Page: AsRefPageSlice {
    pub fn try_from(page: Page) -> PagerResult<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeInterior.assert(kind).map(|_| Self(CellPage::from(page)))
    }

    /// Emprunte une cellule en mutation.
    pub fn borrow_cell(&self, cid: &CellId) -> Option<&BPTreeInteriorCell<PageSlice>> {
        self.as_cells().borrow_cell(cid).map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }
}

impl<Page> BPTreeInterior<Page> where Page: AsMutPageSlice {
    /// Initialise un nouveau noeud intérieur.
    pub fn new(mut page: Page, k: u8, cell_size: PageSize) -> PagerResult<Self> {
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTreeInterior as u8;

        // initialise le sous-système de cellules
        let reserved: u16 = size_of::<BPTreeInteriorHeader>().try_into().unwrap();
        CellPage::new(&mut page, cell_size, k.into(), reserved.into())?;

        Self::try_from(page)
    }

    /// Insère un nouveau triplet {gauche | clé | droit} dans le noeud intérieur.
    pub fn insert(&mut self, left: PageId, key: &Value, right: PageId) -> PagerResult<()> {
        let maybe_existing_cid = self.iter()
            .filter(|cell| cell.left() == Some(left))
            .map(|cell| cell.as_cell().id())
            .last();
       
        match maybe_existing_cid {
            None => {
                // Le lien de gauche est en butée de cellule, on ajoute une cellule
                if self.tail() == Some(left) {
                    let cid = self.as_mut_cells().push()?;
                    let cell = &mut self[&cid];
                    cell.initialise(key, left);       
                    self.as_mut_header().set_tail(Some(right));            
                // Le noeud est vide
                } else {
                    let cid = self.as_mut_cells().push()?;
                    let cell = &mut self[&cid];
                    cell.initialise(key, left);
                    self.as_mut_header().set_tail(Some(right))
                }
            },

            // Il existe une cellule contenant déjà le lien gauche.
            // On va intercaler une nouvelle cellule.
            Some(existing_cid) => {
                let cid = self.as_mut_cells().insert_after(&existing_cid)?;
                let cell = &mut self[&cid];
                cell.initialise(key, left);
                self[&existing_cid].set_left(Some(right));
            },

        };

        Ok(())
    }

    /// Emprunte une cellule en mutation.
    pub fn borrow_mut_cell(&mut self, cid: &CellId) -> Option<&mut BPTreeInteriorCell<PageSlice>> {
        self.as_mut_cells().borrow_mut_cell(cid).map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }
   
    pub fn split_into<P2>(&mut self, dest: &mut BPTreeInterior<P2>) -> PagerResult<&Value> where P2: AsMutPageSlice {
        let at = self.as_cells().len().div(2);
        self.as_mut_cells().split_at_into(dest.as_mut_cells(), at)?;

        let key = self.iter().last().map(|cell| cell.borrow_key()).unwrap();

        Ok(key)
    }

    pub fn set_parent(&mut self, parent: Option<PageId>) {
        self.as_mut_header().parent = parent.into()
    }

    fn as_mut_cells(&mut self) -> &mut CellPage<Page> {
        self.as_mut()
    }

    fn as_mut_page(&mut self) -> &mut Page {
        self.as_mut()
    }

    fn as_mut_header(&mut self) -> &mut BPTreeInteriorHeader {
        self.as_mut()
    }
}

impl BPTreeInterior<MutPage<'_>> {
    pub fn id(&self) -> &PageId {
        self.as_page().id()
    }
}

impl BPTreeInterior<&mut MutPage<'_>> {
    pub fn id(&self) -> &PageId {
        self.as_page().id()
    }
}

impl<Page> BPTreeInterior<Page> where Page: AsRefPageSlice {
    /// Recherche le noeud enfant à partir de la clé passée en référence.
    pub fn search_child(&self, key: &Value) -> PageId 
    {       
        let maybe_child: Option<PageId> = self.iter()
        .filter(|&interior| interior <= key)
        .last()
        .map(|interior| interior.left().clone())
        .unwrap_or_else(|| self.as_header().tail())
        .into();

        maybe_child.expect("should have a child to perform the search")
    }

    pub fn is_full(&self) -> bool {
        self.as_cells().is_full()
    }
    
    pub fn parent(&self) -> Option<PageId> {
        self.as_header().parent.into()
    }

    pub fn tail(&self) -> Option<PageId> {
        self.as_header().tail()
    }

    pub fn iter(&self) -> impl Iterator<Item = &BPTreeInteriorCell<PageSlice>> {
        self.as_cells().iter().map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }

    fn as_page(&self) -> &Page {
        self.as_ref()
    }

    fn as_cells(&self) -> &CellPage<Page> {
        self.as_ref()
    }

    fn as_header(&self) -> &BPTreeInteriorHeader {
        self.as_ref()
    }
}

impl<Page> AsRef<Page> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &Page {
        self.0.as_ref()
    }
}

impl<Page> AsRef<BPTreeInteriorHeader> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeInteriorHeader {
        BPTreeInteriorHeader::ref_from_bytes(&self.as_page().as_ref()[LEAF_HEADER_RANGE]).unwrap()
    }
}
impl<Page> AsMut<Page> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut Page {
        self.0.as_mut()
    }
}
impl<Page> AsMut<BPTreeInteriorHeader> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeInteriorHeader {
        BPTreeInteriorHeader::mut_from_bytes(&mut self.as_mut_page().as_mut()[LEAF_HEADER_RANGE]).unwrap()
    }
}
impl<Page> AsRef<CellPage<Page>> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &CellPage<Page> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl<Page> AsMut<CellPage<Page>> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut CellPage<Page> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug)]
#[repr(C, packed)]
/// L'entête d'un noeud d'un arbre B+
pub struct BPTreeInteriorHeader {
    /// Pointeur vers le noeud parent
    pub(super) parent: OptionalPageId,
    /// Pointeur vers le noeud enfant le plus à droite
    pub(super) tail: OptionalPageId,
}

impl BPTreeInteriorHeader {
    pub fn set_tail(&mut self, tail: Option<PageId>) {
        self.tail = tail.into()
    }

    pub fn tail(&self) -> Option<PageId> {
        self.tail.into()
    }
}

/// Une cellule d'un noeud intérieur contenant le tuple {noeud de gauche | clé}
pub struct BPTreeInteriorCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice + ?Sized;

impl<Slice> BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice + ?Sized {
    pub fn borrow_key(&self) -> &Value {
        Value::from_ref(self.as_key_slice())
    }

    pub fn left(&self) -> Option<PageId> {
        OptionalPageId::read_from_bytes(&self.as_left_slice()).unwrap().into()
    }

    fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }

    fn left_range(&self) -> Range<usize> {
        return 0..size_of::<OptionalPageId>()    
    }

    fn key_range(&self) -> Range<usize> {
        let base = self.left_range().end;
        let kind = ValueKind::from(self.as_cell().as_content_slice().as_bytes()[base]);
        let full_size = kind.full_size().expect(&format!("expecting key to be a sized-type (kind: {kind})"));
        return base..(base + full_size)
    }

    fn as_left_slice(&self) -> &[u8] {
        &self.as_cell().as_content_slice().as_bytes()[self.left_range()]
    }

    fn as_key_slice(&self) -> &[u8] {
        &self.as_cell().as_content_slice()[self.key_range()]
    }
}

impl<Slice> BPTreeInteriorCell<Slice> where Slice: AsMutPageSlice + ?Sized {
    pub fn initialise(&mut self, key: &Value, left: PageId) {
        key.kind().full_size().expect(&format!("expecting key to be a sized-type (kind: {0})", key.kind()));
        let loc = self.left_range().end;
        
        self
            .as_mut_cell()
            .as_mut_content_slice()
            .as_mut_bytes()[loc] = key.kind().clone().into();

        self.borrow_mut_key().set(key);
        self.set_left(Some(left));
    }

    pub fn borrow_mut_key(&mut self) -> &mut Value {
        Value::from_mut(self.as_mut_key_slice())
    }

    pub fn set_left(&mut self, left: Option<PageId>) {
        *self.borrow_mut_left() = left.into();
    }

    pub fn borrow_mut_left(&mut self) -> &mut OptionalPageId {
        OptionalPageId::mut_from_bytes(self.as_mut_left_slice()).unwrap()
    }

    fn as_mut_cell(&mut self) -> &mut Cell<Slice> {
        &mut self.0
    }

    fn as_mut_left_slice(&mut self) -> &mut [u8] {
        let range = self.left_range();
        &mut self.as_mut_cell().as_mut_content_slice()[range]
    }

    fn as_mut_key_slice(&mut self) -> &mut [u8] {
        let range = self.key_range();
        &mut self.as_mut_cell().as_mut_content_slice()[range]
    }
}

impl<Slice> PartialEq<Value> for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn eq(&self, other: &Value) -> bool {
        self.borrow_key() == other
    }
}
impl<Slice> PartialOrd<Value> for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        self.borrow_key().partial_cmp(other)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{bplus_tree::BPlusTree, pager::{fixtures::fixture_new_pager, page::PageId}, value::{ValueBuf, U64}};

    #[test]
    fn test_insert() -> Result<(), Box<dyn Error>>{
        let pager = fixture_new_pager();
        let mut tree = BPlusTree::new(pager.as_ref(), &U64, &U64)?;
        
        let mut interior = tree.insert_interior().and_then(|pid| tree.borrow_mut_interior(&pid))?;
        interior.insert(PageId::new(100), &ValueBuf::from(100_u64), PageId::new(200))?;
        interior.search_child(&ValueBuf::from(100_u64));

        Ok(())
    }
}