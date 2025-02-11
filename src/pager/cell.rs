use std::{num::NonZeroU8, ops::Mul};

use zerocopy::TryFromBytes;

/// Spécification pour manipuler une séquence binaire découpée en cellules de taille constante.
pub struct CellSpec {
    cell_size: u64,
}

/// En-tête d'une cellule.
pub struct CellHeader {
    next: OptionalCellId,
}

/// En-tête de la page contenant les informations relatives aux cellules qui y sont stockées.
pub struct CellPageHeader {
    cell_len: u8,
    free_head_cell: OptionalCellId,
    head_cell: OptionalCellId,
}

pub struct CellId(NonZeroU8);

pub struct OptionalCellId(Option<NonZeroU8>);

impl Mul<u64> for CellId {
    type Output = u64;

    fn mul(self, rhs: u64) -> Self::Output {
        self.0 * rhs
    }
}

impl PartialOrd<u64> for CellId {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(other)
    }
}

impl CellSpec {
    pub fn get_cell_slice<'a>(&self, cid: &CellId, body: &'a [u8]) -> &'a [u8] {
        let cell_space_size = u64::from(body.len());
        let capacity = u64::div_ceil(cell_space_size, self.cell_size);

        if cid > &capacity {
            panic!("cell overflow")
        }

        let base = cid * self.cell_size;

        let cell_bytes = &body[base..(base + self.cell_size)];
    }

    pub fn try_from_ref_bytes<'a, T: TryFromBytes>(&self, cid: &CellId, body: &'a [u8]) {
        T::try_ref_from_bytes(self.get_cell_slice(cid, body))
    }
}
