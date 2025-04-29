use std::ops::Range;

use crate::{cell::CellId, page::PageId};

pub type JarId = u64;

#[derive(Hash, Clone, Copy, PartialEq, Eq, Debug)]
pub struct JarTag {
    pub jar_id: JarId,
    pub page_id: PageId,
    pub cell_id: CellId
}

impl JarTag {
    pub(crate) fn new(jar_id: JarId, page_id: PageId, cell_id: CellId) -> Self {
        Self {jar_id,page_id,cell_id}
    }
    pub fn in_jar(jar_id: JarId) -> Self {
        Self {
            jar_id,
            page_id: 0,
            cell_id: 0
        }
    }
    pub fn in_page(self, page_id: PageId) -> Self {
        Self {
            jar_id: self.jar_id,
            page_id, 
            cell_id: 0
        }
    }
}

impl std::fmt::Display for JarTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.jar_id, self.page_id)
    }
}

pub trait DataArea {
    const AREA: Range<usize>;
    const INTEGRATED_AREA: Range<usize> = 0..(Self::AREA.end);
}
