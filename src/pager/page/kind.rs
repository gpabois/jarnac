use std::fmt::Display;

use crate::{error::{Error, ErrorKind}, result::Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
/// Type de page.
/// 
/// Toutes les pages démarrent avec un octet qui permet d'identifier sa nature.
pub enum PageKind {
    /// Une page libre (cf [crate::pager::free])
    Free = 0,
    /// Une page de débordement (cf [crate::pager::spill])
    Spill = 1,
    /// La page d'entrée d'un arbre B+ (cf [crate::bp_tree::BPlusTreePage])
    BPlusTree = 2,
    /// La page représentant un noeud intérieur d'un arbre B+ (cf [crate::bp_tree::BPTreeInteriorPage])
    BPlusTreeInterior = 3,
    /// La page représentant une feuille d'un arbre B+ (cf [crate::bp_tree::BPTreeLeafPage])
    BPlusTreeLeaf = 4
}

impl Display for PageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageKind::Free => write!(f, "free"),
            PageKind::Spill => write!(f, "spill"),
            PageKind::BPlusTree => write!(f, "b+ tree"),
            PageKind::BPlusTreeInterior => write!(f, "b+ tree interior"),
            PageKind::BPlusTreeLeaf => write!(f, "b+ tree leaf"),
        }
    }
}

impl PageKind {
    pub fn assert(&self, to: PageKind) -> Result<()> {
        (*self == to).then_some(()).ok_or_else(|| {
            Error::new(ErrorKind::WrongPageKind {
                expected: to,
                got: *self,
            })
        })
    }
}

impl TryFrom<u8> for PageKind {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Free),
            1 => Ok(Self::Spill),
            2 => Ok(Self::BPlusTree),
            3 => Ok(Self::BPlusTreeInterior),
            4 => Ok(Self::BPlusTreeLeaf),
            invalid_code => Err(Error::new(ErrorKind::InvalidPageKind(invalid_code))),
        }
    }
}