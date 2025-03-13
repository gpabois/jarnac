use std::borrow::Borrow;

use crate::{pager::PagerResult, value::Value};

use super::{AsBPlusTreeRef, BPlusTreeCellId, IRefBPlusTree};


/// Un curseur sur un arbre B+
pub struct RefBPlusTreeCursor<Tree> where Tree: AsBPlusTreeRef {
    tree: Tree,
    current: Option<BPlusTreeCellId>
}

impl<Tree> RefBPlusTreeCursor<Tree> where Tree: AsBPlusTreeRef {
    /// Place le curseur à un endroit 
    pub fn seek<K: Borrow<Value>>(&mut self, key: &K) -> PagerResult<bool> {
        self.current = self.tree.as_ref().search(key.borrow())?;
        Ok(self.current.is_some())
    }

    /// Place le curseur en tête
    pub fn seek_head(&mut self) -> PagerResult<bool> {
        self.current = self.tree.as_ref().head()?;
        Ok(self.current.is_some())       
    }
    
    /// Déplace le curseur à l'élément suivant
    pub fn next(&mut self) -> PagerResult<bool> {
        Ok(if let Some(current) = self.current {
            match self.tree.as_ref().next_sibling(&current)? {
                Some(next) => {
                    self.current = Some(next);
                    true
                },
                None => {
                    false
                },
            }
        } else {
            false
        })
    }

    /// Déplace le curseur à l'élément précédent.
    pub fn prev(&mut self) -> PagerResult<bool> {
        Ok(if let Some(current) = self.current {
            match self.tree.as_ref().prev_sibling(&current)? {
                Some(prev) => {
                    self.current = Some(prev);
                    true
                },
                None => {
                    false
                },
            }
        } else {
            false
        })
    }
}
