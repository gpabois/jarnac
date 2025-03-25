use std::{marker::PhantomData, ops::Range};

use crate::pager::{cell::CellId, page::PageId};

#[derive(Clone, Copy, Hash, Debug, PartialEq, Eq)]
pub struct JarId(u64);

impl std::fmt::Display for JarId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Hash, Clone, Copy, PartialEq, Eq, Debug)]
pub struct JarTag {
    pub jar_id: JarId,
    pub page_id: PageId,
    pub cell_id: Option<CellId>
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
