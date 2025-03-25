use std::{mem::MaybeUninit, ops::DerefMut};

use crate::{pager::page::{AsMutPageSlice, AsRefPageSlice, InPage, OptionalPageId, PageId, PageKind, PageSize}, result::Result, tag::DataArea, value::ValueKind};
use zerocopy::FromBytes;
use zerocopy_derive::*;

pub struct BPTreeDescriptor<Page>(Page) where Page: AsRefPageSlice;

impl<Page> BPTreeDescriptor<Page> where Page: AsRefPageSlice {    
    pub fn try_from(page: Page) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTree.assert(kind).map(|_| Self(page))
    }

    pub fn len(&self) -> u64 {
        self.as_data().len
    }

    pub fn k(&self) -> u8 {
        self.as_data().k
    }

    pub fn root(&self) -> Option<PageId> {
        self.as_data().root.into()
    }

    pub fn value_kind(&self) -> ValueKind {
        self.as_data().value
    }

    pub fn key_kind(&self) -> ValueKind {
        self.as_data().key
    }

    pub fn interior_cell_size(&self) -> PageSize {
        self.as_data().interior_cell_size.into()
    }

    pub fn leaf_cell_size(&self) -> PageSize {
        self.as_data().leaf_cell_size.into()
    }

    fn as_data(&self) -> &BPlusTreeDescriptorData {
        BPlusTreeDescriptorData::ref_from_bytes(&self.0.as_ref()[BPlusTreeDescriptorData::AREA]).unwrap()
    }
}

impl<Page> BPTreeDescriptor<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, header: BPlusTreeDescriptorData) -> Result<Self> {
        // initialisation bas-niveau de la page.
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTree as u8;

        let mut desc = Self::try_from(page)?;
        desc.as_uninit_data().write(BPlusTreeDescriptorData {
            
        })
        *desc.as_mut_data() = header;

        Ok(desc)   
    }

    pub fn inc_len(&mut self) {
        self.as_mut_data().len += 1;
    }

    pub fn dec_len(&mut self) {
        self.as_mut_data().len -= 1;
    }

    pub fn set_root(&mut self, root: Option<PageId>) {
        self.as_mut_data().root = root.into();
    }

    fn as_mut_data(&mut self) -> &mut BPlusTreeDescriptorData {
        BPlusTreeDescriptorData::mut_from_bytes(
            &mut self.0.as_mut()[BPlusTreeDescriptorData::AREA]
        ).unwrap()
    }

    fn as_uninit_data(&mut self) -> &mut MaybeUninit<BPlusTreeDescriptorData> {
        unsafe {
            std::mem::transmute(self.as_mut_data())
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct BPlusTreeDescriptorData {
    /// Type de la clé
    pub(super) key: ValueKind,
    /// Type de la valeur
    pub(super) value: ValueKind,
    /// Taille d'une cellule d'une feuille
    pub(super) leaf_cell_size: u16,
    /// Taille d'une cellule d'un noeud intérieur
    pub(super) interior_cell_size: u16,
    /// La taille de la donnée stockable dans une cellule d'une feuille
    pub(super) value_size: u16,
    /// Nombre maximum de clés dans l'arbre B+
    pub(super) k: u8,
    /// Pointeur vers la racine
    pub(super) root: OptionalPageId,
    /// Nombre d'éléments stockés
    pub(super) len: u64
}

impl BPlusTreeDescriptorData {
    pub fn new(key: ValueKind, value: ValueKind, k: u8, value_size: u16) -> Self {

    }
}

impl DataArea for BPlusTreeDescriptorData {
    const AREA: std::ops::Range<usize> = InPage::<Self>::AREA;
}