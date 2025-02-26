use std::{cmp::Ordering, ops::{Deref, DerefMut}};

use zerocopy::TryFromBytes;
use zerocopy_derive::{FromBytes, Immutable, KnownLayout, TryFromBytes};

use crate::{pager::{cell::{CellHeader, CellPage, CellPageHeader, CellSize}, page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageId, PageKind}}, value::numeric::Numeric};

use super::BPTreeNodeKind;

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// L'entête d'un noeud d'un arbre B+
pub struct BPTreeInteriorPageHeader {
    kind: BPTreeNodeKind,
    cell_spec: CellPageHeader
}

pub struct BPTreeInterior<Page>(Page) where Page: AsRefPageSlice;

impl<Page> BPTreeInterior<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, k: u8, cell_size: CellSize) -> Self {
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTreeLeaf as u8;

        let mut interior: Self = page.into();
        let data: &mut BPTreeInteriorData = interior.as_mut();
        data.header.cell_spec = CellPageHeader::new(
            cell_size, 
            k,
            size_of::<BPTreeInteriorPageHeader>().try_into().unwrap()
        );
        interior
    }
}

impl<Page> BPTreeInterior<Page> where Page: AsRefPageSlice {
    pub fn is_full(&self) -> bool {
        self.as_ref().is_full()
    }

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
            interior.as_ref().left
        })
        .unwrap_or_else(|| self.as_ref().tail)
        .into();

        maybe_child.expect("should have a child to perform the search")
    }
}

impl<Page> From<Page> for BPTreeInterior<Page> where Page: AsRefPageSlice
{
    fn from(value: Page) -> Self {
        Self(value)
    }
}

impl<Page> AsRef<BPTreeInteriorData> for BPTreeInterior<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeInteriorData {
        BPTreeInteriorData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}

impl<Page> AsMut<BPTreeInteriorData> for BPTreeInterior<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPTreeInteriorData {
        BPTreeInteriorData::try_mut_from_bytes(self.0.as_mut()).unwrap()
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Noeud intérieur
pub struct BPTreeInteriorData {
    /// L'entête du noeud
    header: BPTreeInteriorPageHeader,
    /// Pointeur vers le noeud enfant le plus à droite
    tail: OptionalPageId,
    /// Contient les cellules du noeud. (cf [crate::pager::cell])
    cells: [u8],
}

impl BPTreeInteriorData {
    pub fn is_full(&self) -> bool {
        self.header.cell_spec.is_full()
    }
}

pub struct BPTreeInteriorCell<Slice>(Slice) where Slice: AsRefPageSlice;

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


impl<Slice> AsRef<BPTreeInteriorCellData> for BPTreeInteriorCell<Slice> where Slice: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeInteriorCellData {
        BPTreeInteriorCellData::try_ref_from_bytes(&self.0.as_ref()).unwrap()
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Cellule d'un noeud intérieur.
pub struct BPTreeInteriorCellData {
    cell: CellHeader,
    left: OptionalPageId,
    parent: OptionalPageId,
    key: Numeric,
    rem: [u8]
}

impl BPTreeInteriorCellData {
    pub fn borrow_key(&self) -> &Numeric {
        &self.key
    }
}
