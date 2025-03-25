use std::cell::Cell;

use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{pager::{cell::{CellCapacity, Cells, WithCells}, page::{AsRefPageSlice, OptionalPageId, PageId, PageSize}}, tag::DataArea, utils::Sized, value::ValueKind};

use super::MaybeSized;

pub struct BPlusTreeLeaf<Page>(Page);

impl BPlusTreeLeaf<()> {
    pub fn compute_leaf_cell_size(key: Sized<ValueKind>) -> PageSize {
        let content_size = u16::try_from(size_of::<PageId>() + key.outer_size()).unwrap(); 
        Cells::compute_cell_size(content_size)
    }

    pub fn compute_max_value_size(cell_size: PageSize, key: Sized<ValueKind>) -> u16 {
        cell_size - u16::try_from(key.outer_size()).unwrap()
    }

    pub fn within_available_cell_space_size(page_size: PageSize, key: Sized<ValueKind>, value: MaybeSized<ValueKind>, k: CellCapacity) -> bool {
        let content_size = Self::compute_leaf_cell_size(key);
        let reserved = u16::try_from(size_of::<BPTreeLeafMeta>()).unwrap();
        Cells::within_available_cell_space_size(page_size, reserved, content_size, k)
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

pub struct BPTreeLeafCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice + ?Sized;

