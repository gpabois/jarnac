//! Système de répartition par cellules de taille constante du contenu d'une page.
//!
//! Permet de :
//! - découper l'espace en liste chaînée réordonnable sans réaliser de déplacements de blocs de données
//! - allouer/libérer des cellules 
//! 
//! # Exigence
//! Pour que cela marche :
//! - l'entête de la page doit contenir, après le nombre magique ([crate::pager::page::PageKind]), [CellPageHeader]
//! - l'entête de la cellule doit contenir en premier lieu [CellHeader].
//! 
//! [CellPageHeader] est utilisé pour piloter les cellules, notamment via :
//! - [CellPageHeader::push], et ses variantes [CellPageHeader::push_after] ou [CellPageHeader::push_before]
//! - [CellPageHeader::iter_cells_bytes]


use std::{num::NonZeroU8, ops::{Add, Mul, Range}};

use zerocopy::TryFromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{error::PagerError, PagerResult};

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct Cell {
    header: CellHeader,
    data: [u8]
}

pub struct CellIdIter<'a> {
    header: &'a CellPageHeader,
    cells: &'a [u8],
    current: Option<CellId>
}

impl Iterator for CellIdIter<'_> {
    type Item = CellId;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(curr) => {
                self.current = self.header.get_cell(&curr, self.cells).unwrap().header.next.into();
                Some(curr)
            },
            None => None
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
/// En-tête d'une cellule.
pub struct CellHeader {
    prev: OptionalCellId,
    next: OptionalCellId,
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// En-tête de la page contenant les informations relatives aux cellules qui y sont stockées.
pub struct CellPageHeader {
    /// Taille d'une cellule
    cell_size: CellSize,
    /// Nombre de cellules alloués
    len: u8,
    /// Nombre maximal de cellules stockables.
    capacity: u8,
    /// Nombre de cellules libres
    free_len: u8,
    /// Tête de la liste des cellules libérées
    free_head_cell: OptionalCellId,
    /// Tête de a liste des cellules allouées
    head_cell: OptionalCellId,
}

impl CellPageHeader {
    pub fn new(cell_size: CellSize, capacity: u8) -> Self {
        return Self {
            cell_size,
            capacity,
            len: 0,
            free_len: 0,
            free_head_cell: None.into(),
            head_cell: None.into()
        }
    }

    pub fn is_full(&self) -> bool {
        self.len - self.free_len >= self.capacity
    }
    /// Pousse une nouvelle cellule à la fin de la liste chaînée
    pub fn push(&mut self, cells: &mut [u8]) -> PagerResult<CellId> {
        let cid = self.alloc(cells)?;

        let prev_cid = if let Some(last_cid) = self.iter_cells_ids(cells).last() {
            self.get_mut_cell(&last_cid, cells).unwrap().header.next = Some(cid).into();
            Some(last_cid)
        } else {
            self.head_cell = Some(cid).into();
            None
        };

        self.get_mut_cell(&cid, cells).unwrap().header.prev = prev_cid.into();

        Ok(cid)
    }

    /// Pousse une nouvelle cellule après la cellule.
    pub fn push_after(&mut self, after_cid: &CellId, cells: &mut [u8]) -> PagerResult<CellId> {
        let cid = self.alloc(cells)?;

        let next_next: Option<CellId> = if let Some(next) = self.get_mut_cell(&after_cid, cells) {
            let next_next = next.header.next.into();
            next.header.next = Some(cid).into();
            next_next

        } else {
            self.head_cell = Some(cid).into();
            None
        };

        if let Some(next_next) = next_next.and_then(|nn |self.get_mut_cell(&nn, cells)) {
            next_next.header.prev = Some(cid).into()
        }

        Ok(cid)
    }

    /// Pousse une nouvelle cellule avant la cellule.
    pub fn push_before(&mut self, before_cid: &CellId, cells: &mut [u8]) -> PagerResult<CellId> {
        let cid = self.alloc(cells)?;

        let prev_prev: Option<CellId> = if let Some(prev) = self.get_mut_cell(&before_cid, cells) {
            let prev_prev = prev.header.prev.into();
            prev.header.next = Some(cid).into();
            prev_prev

        } else {
            self.head_cell = Some(cid).into();
            None
        };

        if let Some(prev_prev) = prev_prev.and_then(|nn |self.get_mut_cell(&nn, cells)) {
            prev_prev.header.next = Some(cid).into()
        }

        Ok(cid)
    }


    fn alloc(&mut self, cells: &mut [u8]) -> PagerResult<CellId> {
        if self.len >= self.capacity {
            return Err(PagerError::new(super::error::PagerErrorKind::CellPageFull));
        }

        let cid = self.pop_free_cell(cells).unwrap_or_else(|| {
            self.len += 1;
            CellId(NonZeroU8::new(self.len).unwrap())
        });

        let cell = self.get_mut_cell(&cid, cells).unwrap();
        cell.header = CellHeader::default();
        cell.data.fill(0);

        Ok(cid)
    }

    /// Pop une cellule libre
    fn pop_free_cell(&mut self, cells: &mut [u8]) -> Option<CellId> {
        let mut fragment = self.iter_free_cells_ids(cells).take(2);

        if let Some(head) = fragment.next() {
            if let Some(new_head_id) = fragment.next() {
                let new_head = self.get_mut_cell(&new_head_id, cells).unwrap();
                new_head.header.prev = None.into();
            }

            self.free_len -= 1;
            return Some(head)
        }

        return None
    }

    pub fn iter_cells_bytes<'a>(&'a self, cells: &'a [u8]) -> impl Iterator<Item=&'a[u8]> {
        self.iter_cells_ids(cells).map(|cid| self.get_cell_slice(&cid, cells))
    }

    /// Itère sur les cellules allouées
    pub fn iter_cells_ids<'a>(&'a self, cells: &'a [u8]) -> CellIdIter<'a> {
        CellIdIter {
            header: self,
            cells,
            current: self.head_cell.into()
        }
    }

    /// Itère sur les cellules libres
    fn iter_free_cells_ids<'a>(&'a self, cells: &'a [u8]) -> CellIdIter<'a> {
        CellIdIter {
            header: self,
            cells,
            current: self.free_head_cell.into()
        }       
    }

    /// Récupère le range pour cibler la tranche relative à une cellule
    fn get_cell_range(&self, cid: &CellId) -> Range<usize> {
        let start = (*cid) * self.cell_size;
        let end = start + self.cell_size;
        usize::try_from(start.0).unwrap()..usize::try_from(end.0).unwrap()
    }

    /// Récupère une référence mutable sur une cellule.
    pub fn get_mut_cell<'a>(&self, cid: &CellId, cells: &'a mut [u8]) -> Option<&'a mut Cell> {
        let cell_bytes = self.get_mut_cell_slice(cid, cells);
        Some(Cell::try_mut_from_bytes(cell_bytes).unwrap())
    }

    /// Récupère une référence sur une cellule.
    pub fn get_cell<'a>(&self, cid: &CellId, cells: &'a [u8]) -> Option<&'a Cell> {
        let cell_bytes = self.get_cell_slice(cid, cells);
        Some(Cell::try_ref_from_bytes(cell_bytes).unwrap())
    }

    /// Récupère une référence vers la tranche brute d'une cellule.
    pub fn get_cell_slice<'a>(&self, cid: &CellId, cells: &'a [u8]) -> &'a [u8] {
        let range = self.get_cell_range(cid);
        &cells[range]
    }

    /// Récupère une référence mutable vers la tranche brute d'une cellule.
    pub fn get_mut_cell_slice<'a>(&self, cid: &CellId, cells: &'a mut [u8]) -> &'a mut [u8] {
        let range = self.get_cell_range(cid);
        &mut cells[range]    
    }
}

#[derive(Clone, Copy)]
pub struct CellId(NonZeroU8);

impl Mul<CellSize> for CellId {
    type Output = CellLocation;

    fn mul(self, rhs: CellSize) -> Self::Output {
        CellLocation(u32::from(self.0.get()) * u32::from(rhs.0))
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(transparent)]
pub struct CellSize(u16);

impl From<u16> for CellSize {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy)]
pub struct CellLocation(u32);

impl Add<CellSize> for CellLocation {
    type Output = CellLocation;

    fn add(mut self, rhs: CellSize) -> Self::Output {
       self.0 += u32::from(rhs.0);
       self
    }
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Default)]
pub struct OptionalCellId(Option<NonZeroU8>);

impl Into<Option<CellId>> for OptionalCellId {
    fn into(self) -> Option<CellId> {
        self.0.map(CellId)
    }
}

impl From<Option<CellId>> for OptionalCellId {
    fn from(value: Option<CellId>) -> Self {
        Self(value.map(|n| n.0))
    }
}

impl AsRef<Option<CellId>> for OptionalCellId {
    fn as_ref(&self) -> &Option<CellId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl Mul<u32> for CellId {
    type Output = u32;

    fn mul(self, rhs: u32) -> Self::Output {
        u32::from(self.0.get()) * rhs
    }
}

