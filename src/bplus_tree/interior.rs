use std::{cmp::Ordering, ops::{Deref, DerefMut, Div}};

use zerocopy::TryFromBytes;
use zerocopy_derive::{FromBytes, Immutable, KnownLayout, TryFromBytes};

use crate::{pager::{cell::{Cell, CellHeader, CellId, CellPage, CellPageHeader, CellSize}, page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageId, PageKind, PageSlice}, PagerResult}, value::numeric::Numeric};

use super::BPTreeNodeKind;

pub struct BPTreeInterior<Page>(Page) where Page: AsRefPageSlice;

impl<Page> BPTreeInterior<Page> where Page: AsMutPageSlice {
    /// Initialise un nouveau noeud intérieur.
    pub fn new(mut page: Page, k: u8, cell_size: CellSize) -> Self {
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTreeLeaf as u8;

        let mut interior: Self = page.into();
        let data: &mut BPTreeInteriorData = interior.as_mut();
        data.header.cell_spec = CellPageHeader::new(
            cell_size, 
            k,
            size_of::<BPTreeInteriorHeader>().try_into().unwrap()
        );
        interior
    }

    /// Insère un nouveau triplet {gauche | clé | droit} dans le noeud intérieur.
    pub fn insert(&mut self, left: PageId, key: Numeric, right: PageId) -> PagerResult<()> {
        let maybe_existing_cid = self.iter()
            .filter(|cell| cell.left == Some(left).into())
            .map(|cell| *cell.as_cell().id())
            .last();
        
        match maybe_existing_cid {
            None => {
                // Le lien de gauche est en butée de cellule
                if self.header.tail == Some(left).into() {
                    let cid = self.as_mut_cells().push()?;

                    let mut cell = self.borrow_mut_cell(&cid).unwrap();
                    cell.key = key;
                    cell.left = Some(left).into();
                    
                    self.header.tail = Some(right).into();
                // Le noeud est vide
                } else {
                    let cid = self.as_mut_cells().push()?;
                    let mut cell = self.borrow_mut_cell(&cid).unwrap();
                    cell.key = key;
                    cell.left = Some(left).into();
                    self.header.tail = Some(right).into();
                }
            },
            // Il existe une cellule contenant déjà le lien gauche.
            // On va intercaler une nouvelle cellule.
            Some(existing_cid) => {
                let cid = self.as_mut_cells().insert_after(&existing_cid)?;
                
                let mut cell = self.borrow_mut_cell(&cid).unwrap();
                cell.key = key;
                cell.left = Some(left).into();

                self.borrow_mut_cell(&existing_cid).unwrap().left = Some(right).into();
            },

        };

        Ok(())
    }

    /// Emprunte une cellule en mutation.
    pub fn borrow_mut_cell(&mut self, cid: &CellId) -> Option<BPTreeInteriorCell<&mut PageSlice>> {
        self.as_mut_cells().borrow_mut_cell(cid).map(BPTreeInteriorCell)
    }

    pub fn as_mut_cells(&mut self) -> &mut CellPage<Page> {
        self.as_mut()
    }
    
    pub fn split_into<P2>(&mut self, dest: &mut BPTreeInterior<P2>) -> PagerResult<Numeric> where P2: AsMutPageSlice {
        let at = self.len().div(2);
        self.as_mut_cells().split_at_into(dest.as_mut_cells(), at)?;

        let key = self.iter().last().map(|cell| cell.borrow_key().clone()).unwrap();

        Ok(key)
    }
}
impl<Page> BPTreeInterior<Page> where Page: AsRefPageSlice {
    /// Recherche le noeud enfant à partir de la clé passée en référence.
    pub fn search_child(&self, key: &Numeric) -> PageId 
    {
        let cells = CellPage::from(&self.0);
        
        let maybe_child: Option<PageId>  = cells.iter_ids()
        .flat_map(|cid| cells.borrow_cell(&cid))
        .map(BPTreeInteriorCell)
        .filter(|interior| {
            interior <= key
        })
        .last()
        .map(|interior| {
            interior.left
        })
        .unwrap_or_else(|| self.header.tail)
        .into();

        maybe_child.expect("should have a child to perform the search")
    }

    pub fn as_page(&self) -> &Page {
        self.as_ref()
    }

    pub fn as_cells(&self) -> &CellPage<Page> {
        self.as_ref()
    }

    pub fn iter(&self) -> impl Iterator<Item = BPTreeInteriorCell<& PageSlice>> {
        self.as_cells().iter().map(BPTreeInteriorCell)
    }
}

impl<Page> From<Page> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn from(value: Page) -> Self {
        Self(value)
    }
}

impl<Page> Deref for BPTreeInterior<Page> where Page: AsRefPageSlice {
    type Target = BPTreeInteriorData;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
impl<Page> DerefMut for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<Page> AsRef<Page> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &Page {
        &self.0
    }
}
impl<Page> AsRef<BPTreeInteriorData> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeInteriorData {
        BPTreeInteriorData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}
impl<Page> AsMut<Page> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut Page {
        &mut self.0
    }
}
impl<Page> AsMut<BPTreeInteriorData> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeInteriorData {
        BPTreeInteriorData::try_mut_from_bytes(self.0.as_mut()).unwrap()
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

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// L'entête d'un noeud d'un arbre B+
pub struct BPTreeInteriorHeader {
    kind: BPTreeNodeKind,
    cell_spec: CellPageHeader,
    pub(super) parent: OptionalPageId,
    /// Pointeur vers le noeud enfant le plus à droite
    pub(super) tail: OptionalPageId,
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Noeud intérieur
pub struct BPTreeInteriorData {
    /// L'entête du noeud
    pub(super) header: BPTreeInteriorHeader,
    /// Contient les cellules du noeud. (cf [crate::pager::cell])
    cells: [u8],
}

impl BPTreeInteriorData {
    pub fn is_full(&self) -> bool {
        self.header.cell_spec.is_full()
    }

    pub fn len(&self) -> u8 {
        self.header.cell_spec.len()
    }
}

pub struct BPTreeInteriorCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice;

impl<Slice> BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    pub fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }
}

impl<Slice> PartialEq<Numeric> for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    fn eq(&self, other: &Numeric) -> bool {
        self.borrow_key() == other
    }
}

impl<Slice> PartialOrd<Numeric> for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    fn partial_cmp(&self, other: &Numeric) -> Option<Ordering> {
        self.borrow_key().partial_cmp(other)
    }
}

impl<Slice> BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    pub fn borrow_key(&self) -> &Numeric {
        self.as_ref().borrow_key()
    }
}

impl<Slice> Deref for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    type Target = BPTreeInteriorCellData;
    
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<Slice> DerefMut for BPTreeInteriorCell<Slice> where Slice: AsMutPageSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}


impl<Slice> AsRef<BPTreeInteriorCellData> for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeInteriorCellData {
        BPTreeInteriorCellData::try_ref_from_bytes(self.0.as_slice()).unwrap()
    }
}

impl<Slice> AsMut<BPTreeInteriorCellData> for BPTreeInteriorCell<Slice> where Slice: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeInteriorCellData {
        BPTreeInteriorCellData::try_mut_from_bytes(self.0.as_mut_slice()).unwrap()
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Cellule d'un noeud intérieur.
pub struct BPTreeInteriorCellData {
    cell: CellHeader,
    pub(super) left: OptionalPageId,
    key: Numeric,
    rem: [u8]
}

impl BPTreeInteriorCellData {
    pub fn borrow_key(&self) -> &Numeric {
        &self.key
    }
}
