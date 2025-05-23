use std::ops::Deref;

use itertools::Itertools;

use crate::utils::Shift;

use super::{
    buf::{IntoKnackBuf, KnackBuf},
    kind::{GetKnackKind, KnackKind, KnackKindDescriptor, ANY_TYPE_ID, ARRAY_FLAG, KNACK_KIND_DESCRIPTORS},
    marker::{sized::{Sized, VarSized}, Comparable, Element, FixedSized},
    FromKnack, Knack, KnackBuilder, KnackTypeId,
};

impl super::marker::Element<KnackKind> {
    pub fn type_id(&self) -> KnackTypeId {
        self.0.type_id() - ARRAY_FLAG
    }

    pub fn as_sized(&self) -> Sized<'_, KnackKind> {
        if let Some(fixed) = self.try_as_fixed_sized() {
            return Sized::Fixed(fixed);
        }

        if let Some(var) = self.try_as_var_sized() {
            return Sized::Var(var);
        }

        unreachable!("should be either fixed or var sized")
    }

    pub fn try_as_var_sized(&self) -> Option<&VarSized<KnackKind>> {
        if !self.is_sized() {
            unsafe { Some(std::mem::transmute(self)) }
        } else {
            None
        }
    }

    pub fn try_as_fixed_sized(&self) -> Option<&FixedSized<KnackKind>> {
        if self.is_sized() {
            unsafe { Some(std::mem::transmute(self)) }
        } else {
            None
        }
    }

    pub fn try_as_comparable(&self) -> Option<&Comparable<KnackKind>> {
        if self.is_comparable() {
            return unsafe { Some(std::mem::transmute(self)) };
        } else {
            None
        }
    }

    fn is_sized(&self) -> bool {
        KNACK_KIND_DESCRIPTORS
            .get(&self.type_id())
            .map(|desc| desc.flags & KnackKindDescriptor::FLAG_SIZED > 0)
            .unwrap_or_default()
    }

    fn is_comparable(&self) -> bool {
        KNACK_KIND_DESCRIPTORS
            .get(&self.type_id())
            .map(|desc| desc.flags & KnackKindDescriptor::FLAG_COMPARABLE > 0)
            .unwrap_or_default()
    }
}

impl super::marker::Array<KnackKind> {
    pub fn element_kind(&self) -> &super::marker::Element<KnackKind> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

pub struct Array([u8]);

impl FromKnack for Array {
    type Output = Self;

    fn try_ref_from_knack(value: &Knack) -> super::result::KnackResult<&Self::Output> {
        todo!()
    }

    fn try_mut_from_knack(value: &mut Knack) -> super::result::KnackResult<&mut Self::Output> {
        todo!()
    }
}

impl GetKnackKind for Array {
    type Kind = KnackKind;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[ANY_TYPE_ID | ARRAY_FLAG];
            std::mem::transmute(raw)
        }
    }
}


impl Array {
    pub fn get<'array>(&'array self, index: &usize) -> Option<&'array Knack> {
        if let Sized::Fixed(desc) = self.element_kind().as_sized() {
            let offset = index * desc.outer_size();
            let base = 1usize;

            let range = (0..desc.outer_size()).shift(base + offset);
            Some(<&Knack>::from(&self.0[range]))
        } else {
            todo!("implement variable array")
        }
    }

    fn element_kind(&self) -> &super::marker::Element<KnackKind> {
        self.kind().element_kind()
    }

    fn kind(&self) -> &super::marker::Array<KnackKind> {
        <&KnackKind>::try_from(&self.0).unwrap().try_as_array().unwrap()
    }
}

/// Constructeur d'une liste de trucs
pub struct ArrayBuilder(Vec<KnackBuilder>);

impl ArrayBuilder {
    pub fn kinds(&self) -> Vec<&KnackKind> {
        self.0.iter().map(|kb| kb.kind()).dedup().collect()
    }
}

impl IntoKnackBuf for ArrayBuilder {
    type Buf = KnackBuf;

    fn into_knack_buf(self) -> KnackBuf {
        let kinds = self.kinds();

        // Il s'agit d'une liste d'un seul type
        // de taille fixe,
        // on peut avoir une structure simplifiée
        if kinds.len() == 1 && kinds[0].try_as_fixed_sized().is_some() {
            let element_kind = kinds[0].try_as_fixed_sized().unwrap();
            
        } else {

        }

        todo!("implement into_knack_buf for ArrayBuilder");
    }
}

impl FromKnack for ArrayBuilder {
    type Output = Array;

    fn try_ref_from_knack(value: &super::Knack) -> super::result::KnackResult<&Self::Output> {
        todo!()
    }

    fn try_mut_from_knack(
        value: &mut super::Knack,
    ) -> super::result::KnackResult<&mut Self::Output> {
        todo!()
    }
}


impl Deref for ArrayBuilder {
    type Target = [KnackBuilder];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
