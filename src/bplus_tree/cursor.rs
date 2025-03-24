use std::marker::PhantomData;

use crate::{pager::page::RefPageSlice, result::Result, value::Value, utils::Flip};
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

impl<'a, Tree> Iterator for BPTreeCursor<'a, Tree> where Tree: AsBPlusTreeLeafRef<'a> {
    type Item = Result<BPTreeLeafCell<RefPageSlice<'a>>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(current) = self.current {
            let kv = self.tree.as_ref().get_cell_ref(&current);
            self.next().expect("cannot advance cursor");
            return kv.flip();
        }

        None
    }
}

impl<'a, Tree> BPTreeCursor<'a, Tree> where Tree: AsBPlusTreeLeafRef<'a> {
    pub fn open(tree: Tree) -> Self {
        Self {
            tree,
            current: None,
            _pht: PhantomData
        }
    }
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

#[cfg(test)]
mod tests {
    use std::error::Error;

    use itertools::Itertools;

    use crate::{bplus_tree::{cursor::BPTreeCursorSeek, BPlusTree}, pager::fixtures::fixture_new_pager, value::IntoValueBuf};

    use super::BPTreeCursor;

    #[test]
    fn test_iter() -> std::result::Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();
        let mut tree = BPlusTree::new::<u64, u64>(&pager)?;

        let mut expected: Vec<u64> = vec![];

        for i in 0..100u64 {
            tree.insert(
                &i.into_value_buf(),
                &i.into_value_buf()
            ).inspect_err(|err| {
                println!("{0:#?}", err.backtrace)
            })?;
            expected.push(i);
        }

        let mut cursor = BPTreeCursor::open(&tree);
        cursor.seek(BPTreeCursorSeek::Head)?;

        let got: Vec<u64> = cursor
            .map(|kv| {
                kv.and_then(|kv| 
                    kv.borrow_value()
                        .get(&pager)
                        .map(|var| var.cast::<u64>().to_owned())
                    )
            })
            .try_collect()?;

        assert_eq!(expected, got);

        Ok(())
    }
}