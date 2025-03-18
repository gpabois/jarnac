use std::ops::DerefMut;

use crate::{pager::{page::{AsMutPageSlice, AsRefPageSlice, OptionalPageId, PageId, PageKind, PageSize}, PagerResult}, value::ValueKind};
use zerocopy::FromBytes;
use zerocopy_derive::*;

pub struct BPTreeDescriptor<Page>(Page) where Page: AsRefPageSlice;

impl<Page> BPTreeDescriptor<Page> where Page: AsRefPageSlice {    
    pub fn try_from(page: Page) -> PagerResult<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTree.assert(kind).map(|_| Self(page))
    }

    pub fn k(&self) -> u8 {
        self.as_header().k
    }

    pub fn root(&self) -> Option<PageId> {
        self.as_header().root.into()
    }

    pub fn value_kind(&self) -> ValueKind {
        self.as_header().value
    }

    pub fn key_kind(&self) -> ValueKind {
        self.as_header().key
    }

    pub fn interior_cell_size(&self) -> PageSize {
        self.as_header().interior_cell_size.into()
    }

    pub fn leaf_cell_size(&self) -> PageSize {
        self.as_header().leaf_cell_size.into()
    }

    fn as_header(&self) -> &BPlusTreeHeader {
        self.as_ref()
    }
}

impl<Page> BPTreeDescriptor<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, header: BPlusTreeHeader) -> PagerResult<Self> {
        // initialisation bas-niveau de la page.
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTree as u8;

        let mut desc = Self::try_from(page)?;
        *desc.as_mut_header() = header;

        Ok(desc)   
    }

    pub fn set_root(&mut self, root: Option<PageId>) {
        self.as_mut_header().root = root.into();
    }

    fn as_mut_header(&mut self) -> &mut BPlusTreeHeader {
        self.as_mut()
    }
}

impl<Page> AsRef<BPlusTreeHeader> for BPTreeDescriptor<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPlusTreeHeader {
        let idx = 1..(1+size_of::<BPlusTreeHeader>());
        BPlusTreeHeader::ref_from_bytes(&self.0.as_ref()[idx]).unwrap()
    }
}


impl<Page> AsMut<BPlusTreeHeader> for BPTreeDescriptor<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut BPlusTreeHeader {
        let idx = 1..(1+size_of::<BPlusTreeHeader>());
        BPlusTreeHeader::mut_from_bytes(&mut self.0.as_mut()[idx]).unwrap()
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct BPlusTreeHeader {
    /// Type de la clé
    pub(super) key: ValueKind,
    /// Type de la valeur
    pub(super) value: ValueKind,
    /// Taille d'une cellule d'une feuille
    pub(super) leaf_cell_size: u16,
    /// Taille d'une cellule d'un noeud intérieur
    pub(super) interior_cell_size: u16,
    /// La taille de la donnée stockable dans une cellule d'une feuille
    pub(super) data_size: u16,
    /// Nombre maximum de clés dans l'arbre B+
    pub(super) k: u8,
    /// Pointeur vers la racine
    pub(super) root: OptionalPageId,
}
