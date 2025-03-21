use std::marker::PhantomData;

use crate::{pager::page::RefPageSlice, result::Result, value::Value};

use super::{leaf::BPTreeLeafCell, AsBPlusTreeLeafRef, BPlusTreeCellId, IRefBPlusTree};

pub struct BPTreeCursor<'a, Tree> where Tree: AsBPlusTreeLeafRef<'a> {
    pub(super) tree: Tree, 
    pub(super) current: Option<BPlusTreeCellId>,
    pub(super) _pht: PhantomData<&'a ()>
}

pub enum BPTreeCursorSeek<'a> {
    Head,
    Tail,
    Strict(&'a Value),
    NearestCeil(&'a Value),
    NearestFloor(&'a Value)
}

impl<'a, Tree> BPTreeCursor<'a, Tree> where Tree: AsBPlusTreeLeafRef<'a> {
    /// Pointe le curseur à l'endroit recherchée
    pub fn seek(&mut self, seek: BPTreeCursorSeek<'_>) -> Result<()> {
        match seek {
            BPTreeCursorSeek::Head => {
                self.current = self.tree.as_ref().head()?;
            },
            BPTreeCursorSeek::Tail => {
                self.current = self.tree.as_ref().tail()?;
            },
            BPTreeCursorSeek::Strict(value) => {
                todo!()
            },
            BPTreeCursorSeek::NearestCeil(key) => {
                self.current = self.tree.as_ref().search_nearest_ceil(key)?;
            },
            BPTreeCursorSeek::NearestFloor(key) => {
                self.current = self.tree.as_ref().search_nearest_floor(key)?;
            },
        }
        Ok(())
    }
    /// Va à la prochaine paire clé/valeur
    pub fn next(&mut self) -> Result<()> {
        if let Some(current) = self.current {
            self.current = self.tree.as_ref().next_sibling(&current)?;
        }

        Ok(())
    }

    /// Va à la précédente paire clé/valeur
    pub fn previous(&mut self) -> Result<()> {
        if let Some(current) = self.current {
            self.current = self.tree.as_ref().prev_sibling(&current)?;
        }

        Ok(())     
    }

    /// Récupère la paire clé/valeur en cours.
    pub fn current<'b>(&'b self) -> Result<Option<BPTreeLeafCell<RefPageSlice<'b>>>> where 'b: 'a {
        if let Some(current) = self.current {
            return self.tree.as_ref().get_cell_ref(&current)
        }

        Ok(None)
    }
}

