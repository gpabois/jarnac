use std::ops::{Div, Range};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    cell::{Cell, CellCapacity, CellPage, Cells, WithCells}, error::Error, knack::{Knack, buf::KnackBuf, kind::KnackKind}, page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, PageSlice, RefPage}, result::Result, tag::DataArea, utils::{Comparable, Shift, Sized}
};

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

impl<Page> BPlusTreeInterior<Page> where Page: AsRefPageSlice {
    pub fn search_child(&self, key: &Comparable<Knack>) -> PageId {
        let maybe_child: Option<PageId> = self.iter()
            .filter(|&interior| interior.borrow_key() >= key)
            .next()
            .map(|interior| interior.left().clone())
            .unwrap_or_else(|| self.as_meta().tail())
            .into();

        maybe_child.expect("should have a child to perform the search")
    }

    pub fn is_full(&self) -> bool {
        self.0.is_full()
    }
    
    pub fn parent(&self) -> Option<PageId> {
        self.as_meta().parent()
    }

    pub fn tail(&self) -> Option<PageId> {
        self.as_meta().tail()
    }

    pub fn iter(&self) -> impl Iterator<Item = &BPTreeInteriorCell<PageSlice>> {
        self.0.iter().map(|cell| <&BPTreeInteriorCell<PageSlice>>::from(cell))
    }

    fn as_meta(&self) -> &BPTreeInteriorMeta {
        BPTreeInteriorMeta::ref_from_bytes(&self.0.as_bytes()[BPTreeInteriorMeta::AREA]).unwrap()
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
    
    /// Insère un nouveau triplet {gauche | clé | droit} dans le noeud intérieur.
    pub fn insert(&mut self, left: PageId, key: &Knack, key_kind: &Sized<KnackKind>, right: PageId) -> Result<()> {
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
                cell.initialise(key, right);
            },

        };

        Ok(())
    }

    /// Divise le noeud à la moitié de sa capacité et retourne la clé pivot.
    pub fn split_into<P2>(&mut self, dest: &mut BPlusTreeInterior<P2>) -> Result<KnackBuf> where P2: AsMutPageSlice {
        let at = self.0.len().div(2) + 1;
        
        self.0.split_at_into(&mut dest.0, at)?;
        dest.set_tail(self.tail());


        let (to_remove, pivot, new_left_tail) = self
            .iter()
            .last()
            .map(|cell| (
                cell.0.id(), 
                cell.borrow_key().to_owned(), 
                cell.left().unwrap()
            ))
            .unwrap();
        
        self.0.free_cell(&to_remove);
        self.set_tail(Some(new_left_tail));

        
        Ok(pivot)
    }

    pub fn set_parent(&mut self, parent: Option<PageId>) {
        self.as_mut_meta().set_parent(parent);
    }

    fn set_tail(&mut self, tail: Option<PageId>) {
        self.as_mut_meta().set_tail(tail);
    }

    fn as_mut_meta(&mut self) -> &mut BPTreeInteriorMeta {
        BPTreeInteriorMeta::mut_from_bytes(&mut self.0.as_mut_bytes()[BPTreeInteriorMeta::AREA]).unwrap()
    }
}


impl BPlusTreeInterior<()> {
    pub fn compute_cell_content_size(key: Sized<KnackKind>) -> PageSize {
        u16::try_from(size_of::<PageId>()+ key.outer_size()).unwrap()
    }

    pub fn within_available_cell_space_size(page_size: PageSize, key: Sized<KnackKind>, k: CellCapacity) -> bool {
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

impl BPTreeInteriorMeta {
    pub fn tail(&self) -> Option<PageId> {
        self.tail.into()
    }

    pub fn set_tail(&mut self, tail: Option<PageId>) {
        self.tail = tail.into()
    }

    pub fn parent(&self) -> Option<PageId> {
        self.parent.into()
    }

    pub fn set_parent(&mut self, parent: Option<PageId>) {
        self.parent = parent.into()
    }
}

impl DataArea for BPTreeInteriorMeta {
    const AREA: std::ops::Range<usize> = WithCells::<Self>::AREA;
}

/// Une cellule d'un noeud intérieur contenant le tuple {noeud de gauche | clé}
pub struct BPTreeInteriorCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice + ?std::marker::Sized;

impl From<&Cell<PageSlice>> for &BPTreeInteriorCell<PageSlice> {
    fn from(value: &Cell<PageSlice>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl From<&mut Cell<PageSlice>> for &mut BPTreeInteriorCell<PageSlice> {
    fn from(value: &mut Cell<PageSlice>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl<Slice> BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice + ?std::marker::Sized {
    pub fn borrow_key(&self) -> &Knack {
        Knack::from_ref(self.as_key_slice())
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
        self.kind().as_area().shift(size_of::<PageId>())
    }

    fn as_left_slice(&self) -> &[u8] {
        &self.as_cell().as_content_slice().as_bytes()[self.left_range()]
    }

    fn as_key_slice(&self) -> &[u8] {
        &self.as_cell().as_content_slice()[self.key_range()]
    }

    fn kind(&self) -> Comparable<Sized<KnackKind>> {
        let kind = KnackKind::try_from(&self.0[..size_of::<KnackKind>()]).unwrap();
        kind.try_as_comparable().unwrap()
    }
}

impl<Slice> BPTreeInteriorCell<Slice> where Slice: AsMutPageSlice + ?std::marker::Sized {
    pub fn initialise(&mut self, key: &Comparable<Knack>, left: PageId) {
        key.kind().outer_size().expect(&format!("expecting key to be a sized-type (kind: {0})", key.kind()));
        let loc = self.left_range().end;
        
        self
            .as_mut_cell()
            .as_mut_content_slice()
            .as_mut_bytes()[loc] = key.kind().clone().into();

        self.borrow_mut_key().set(key);
        self.set_left(Some(left));
    }

    pub fn borrow_mut_key(&mut self) -> &mut Knack {
        Knack::from_mut(self.as_mut_key_slice())
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