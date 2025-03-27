use std::{mem::MaybeUninit, ops::DerefMut};

use crate::{page::{AsMutPageSlice, AsRefPageSlice, InPage, OptionalPageId, PageId, PageKind}, result::Result, tag::DataArea, utils::{MaybeSized, Sized, Valid, VarSized}, knack::KnackKind};
use zerocopy::FromBytes;
use zerocopy_derive::*;

use super::{interior::BPlusTreeInterior, leaf::BPlusTreeLeaf, BPlusTreeDefinition};

pub struct BPTreeDescriptor<Page>(Page) where Page: AsRefPageSlice;

impl<Page> BPTreeDescriptor<Page> where Page: AsRefPageSlice {    
    pub fn try_from(page: Page) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTree.assert(kind).map(|_| Self(page))
    }

    /// Le nombre d'éléments stockés dans l'arbre
    pub fn len(&self) -> u64 {
        self.as_description().len
    }

    /// Le nombre de cellules que peut contenir un noeud intérieur ou une feuille.
    pub fn k(&self) -> u8 {
        self.as_description().k
    }

    pub fn root(&self) -> Option<PageId> {
        self.as_description().root.into()
    }

    pub fn value_kind(&self) -> MaybeSized<KnackKind> {
        self.as_description().value_kind()
    }

    pub fn key_kind(&self) -> Sized<KnackKind> {
        self.as_description().key_kind()
    }

    pub fn is_var_sized(&self) -> bool {
        return self.as_description().flags & BPlusTreeDefinition::VAL_IS_VAR_SIZED > 0
    }

    pub(super) fn as_description(&self) -> &BPlusTreeDescription {
        BPlusTreeDescription::ref_from_bytes(&self.0.as_ref()[BPlusTreeDescription::AREA]).unwrap()
    }
}

impl<Page> BPTreeDescriptor<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, definition: Valid<BPlusTreeDefinition>) -> Result<Self> {
        // initialisation bas-niveau de la page.
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTree as u8;

        let mut desc = Self::try_from(page)?;
        
        desc
            .as_uninit_description()
            .write(BPlusTreeDescription::new(definition));

        Ok(desc)   
    }

    pub fn inc_len(&mut self) {
        self.as_mut_description().len += 1;
    }

    pub fn dec_len(&mut self) {
        self.as_mut_description().len -= 1;
    }

    pub fn set_root(&mut self, root: Option<PageId>) {
        self.as_mut_description().set_root(root);
    }

    fn as_mut_description(&mut self) -> &mut BPlusTreeDescription {
        BPlusTreeDescription::mut_from_bytes(
            &mut self.0.as_mut()[BPlusTreeDescription::AREA]
        ).unwrap()
    }

    fn as_uninit_description(&mut self) -> &mut MaybeUninit<BPlusTreeDescription> {
        unsafe {
            std::mem::transmute(self.as_mut_description())
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct BPlusTreeDescription {
    /// Nombre maximum de clés dans l'arbre B+
    pub(super) k: u8,
    /// Quelques caractéristiques de l'Arbre B+ (VAL_WILL_SPILL, VAL_IS_VAR_SIZED)
    pub(super) flags: u8,
    /// Type de la clé
    pub(super) key_kind: KnackKind,
    /// La taille de la clé
    pub(super) key_size: u16,
    /// Type de la valeur
    pub(super) value_kind: KnackKind,
    /// La taille de la donnée stockable dans une cellule d'une feuille
    pub(super) value_size: u16,
    /// Pointeur vers la racine
    pub(super) root: OptionalPageId,
    /// Nombre d'éléments stockés
    pub(super) len: u64
}

impl BPlusTreeDescription {
    pub fn new(def: Valid<BPlusTreeDefinition>) -> Self {
        Self {
            k: def.0.k,
            flags: def.0.flags,
            key_kind: def.0.key,
            key_size: def.0.key_size,
            value_kind: def.0.value,
            value_size: def.0.value_size,
            root: None.into(),
            len: 0
        }
    }

    pub fn key_kind(&self) -> Sized<KnackKind> {
        Sized::new(self.key_kind, usize::from(self.key_size))
    }

    pub fn value_kind(&self) -> MaybeSized<KnackKind> {
        if self.flags | BPlusTreeDefinition::VAL_IS_VAR_SIZED > 0 {
            return MaybeSized::Var(VarSized::new(self.value_kind))
        } else {
            return MaybeSized::Sized(Sized::new(self.value_kind, self.value_size.into()))
        }
    }

    pub fn leaf_content_size(&self) -> u16 {
        BPlusTreeLeaf::<()>::compute_cell_content_size(self.key_kind(), self.value_size)
    }

    pub fn interior_content_size(&self) -> u16 {
        BPlusTreeInterior::<()>::compute_cell_content_size(self.key_kind())
    }

    pub fn set_root(&mut self, root: Option<PageId>) {
        self.root = root.into()
    }
}

impl DataArea for BPlusTreeDescription {
    const AREA: std::ops::Range<usize> = InPage::<Self>::AREA;
}