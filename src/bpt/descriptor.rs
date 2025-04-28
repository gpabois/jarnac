use std::{mem::MaybeUninit, ops::{Deref, DerefMut}};

use crate::{
    knack::{
        kind::{EmcompassingKnackKind, KnackKind},
        marker::{AsFixedSized, Comparable, ComparableAndFixedSized, FixedSized},
    },
    page::{AsMutPageSlice, AsRefPageSlice, InPage, OptionalPageId, PageId, PageKind, PageSize},
    result::Result,
    tag::DataArea,
    utils::Valid,
};
use zerocopy::FromBytes;
use zerocopy_derive::*;

use super::{interior::BPlusTreeInterior, leaf::BPlusTreeLeaf, BPlusTreeDefinition};

pub struct BPTreeDescriptor<Page>(Page)
where
    Page: AsRefPageSlice;

impl<Page> BPTreeDescriptor<Page>
where
    Page: AsRefPageSlice,
{
    pub fn try_from(page: Page) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTree.assert(kind).map(|_| Self(page))
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Le nombre d'éléments stockés dans l'arbre
    pub fn len(&self) -> u64 {
        self.as_description().len
    }

    /// Le nombre de cellules que peut contenir un noeud intérieur ou une feuille.
    pub fn k(&self) -> u8 {
        self.as_description().k()
    }

    pub fn root(&self) -> Option<PageId> {
        self.as_description().root.into()
    }

    pub fn value_kind(&self) -> &KnackKind {
        self.as_description().value_kind()
    }

    pub fn key_kind(&self) -> &Comparable<FixedSized<KnackKind>> {
        self.as_description().key_kind()
    }

    pub fn is_var_sized(&self) -> bool {
        self.as_description().flags() & BPlusTreeDefinition::VAL_IS_VAR_SIZED > 0
    }

    pub(super) fn as_description(&self) -> &BPlusTreeDescription {
        BPlusTreeDescription::ref_from_bytes(&self.0.as_ref()[BPlusTreeDescription::AREA]).unwrap()
    }
}

impl<Page> BPTreeDescriptor<Page>
where
    Page: AsMutPageSlice,
{
    pub fn new(mut page: Page, definition: Valid<BPlusTreeDefinition>) -> Result<Self> {
        // initialisation bas-niveau de la page.
        page.as_mut().fill(0);
        page.as_mut().deref_mut()[0] = PageKind::BPlusTree as u8;

        let mut desc = Self::try_from(page)?;

        desc.as_uninit_description()
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
        BPlusTreeDescription::mut_from_bytes(&mut self.0.as_mut()[BPlusTreeDescription::AREA])
            .unwrap()
    }

    fn as_uninit_description(&mut self) -> &mut MaybeUninit<BPlusTreeDescription> {
        unsafe { std::mem::transmute(self.as_mut_description()) }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct BPlusTreeDescription {
    /// Définition de l'arbre B+
    k: u8,
    flags: u8,
    key: EmcompassingKnackKind,
    value: EmcompassingKnackKind,
    in_cell_value_size: u16,
    page_size: PageSize,
    /// Pointeur vers la racine
    pub(super) root: OptionalPageId,
    /// Nombre d'éléments stockés
    pub(super) len: u64,
}

impl BPlusTreeDescription {
    pub fn new(def: Valid<BPlusTreeDefinition>) -> Self {
        Self {
            k: def.0.k,
            flags: def.0.flags,
            key: def.0.key,
            value: def.0.value,
            in_cell_value_size: def.0.in_cell_value_size,
            page_size: def.0.page_size,
            root: None.into(),
            len: 0,
        }
    }

    pub fn k(&self) -> u8 {
        self.k
    }

    pub fn flags(&self) -> &u8 {
        &self.flags
    }

    pub fn value_kind(&self) -> &KnackKind {
        self.value.deref()
    }

    pub fn value_will_spill(&self) -> bool {
        self.flags() & BPlusTreeDefinition::VAL_IS_VAR_SIZED > 0
    }

    pub fn key_kind(&self) -> &ComparableAndFixedSized<KnackKind> {
        let kind: &KnackKind = self.key.deref();

        <&ComparableAndFixedSized::<KnackKind>>::try_from(kind).unwrap()
    }

    pub fn leaf_content_size(&self) -> u16 {
        BPlusTreeLeaf::<()>::compute_cell_content_size(
            self.key_kind().as_fixed_sized(),
            self.in_cell_value_size,
        )
    }

    pub fn interior_content_size(&self) -> u16 {
        BPlusTreeInterior::<()>::compute_cell_content_size(self.key_kind().as_fixed_sized())
    }

    pub fn set_root(&mut self, root: Option<PageId>) {
        self.root = root.into()
    }
}

impl DataArea for BPlusTreeDescription {
    const AREA: std::ops::Range<usize> = InPage::<Self>::AREA;
}

