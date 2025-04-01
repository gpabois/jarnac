use std::{convert::Infallible, fmt::Display, ops::{Deref, Range}};

use zerocopy::IntoBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};


use super::{
    document::{Document, KeyValue}, error::{KnackError as KnackError, KnackErrorKind as KnackErrorKind}, marker::{
        kernel::{AsKernelMut, AsKernelRef}, 
        sized::{Sized, VarSized}, 
        Comparable, 
        FixedSized
    }, result::KnackResult as KnackResult, KnackSize, KnackTypeId
};

pub(super) const U8_TYPE_ID: KnackTypeId = 1;
pub(super) const U16_TYPE_ID: KnackTypeId = 2;
pub(super) const U32_TYPE_ID: KnackTypeId = 3;
pub(super) const U64_TYPE_ID: KnackTypeId = 4;
pub(super) const U128_TYPE_ID: KnackTypeId = 5;
pub(super) const I8_TYPE_ID: KnackTypeId = 6;
pub(super) const I16_TYPE_ID: KnackTypeId = 7;
pub(super) const I32_TYPE_ID: KnackTypeId = 8;
pub(super) const I64_TYPE_ID: KnackTypeId = 9;
pub(super) const I128_TYPE_ID: KnackTypeId = 10;
pub(super) const F32_TYPE_ID: KnackTypeId = 11;
pub(super) const F64_TYPE_ID: KnackTypeId = 12;
pub(super) const STR_TYPE_ID: KnackTypeId = 13;
pub(super) const DOCUMENT_TYPE_ID: KnackTypeId = 14;
pub(super) const KV_PAIR_TYPE_ID: KnackTypeId = 15;

pub trait GetKnackKind {
    type Kind: AsKernelRef<Kernel=KnackKind>;

    fn kind() -> Self::Kind;
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct KnackKind([u8;4]);

impl Display for KnackKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Knack::{0}", self.type_id())
    }
}

impl TryFrom<&[u8]> for &KnackKind {
    type Error = Infallible;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let bytes: &[u8;4] = value.try_into().unwrap();
        unsafe {
            std::mem::transmute(bytes)
        }
    }
}

impl AsRef<KnackKind> for KnackKind {
    fn as_ref(&self) -> &KnackKind {
        &self
    }
}

impl Deref for KnackKind {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KnackKind {
    pub const FLAG_SIZED: u8 = 0b1;
    pub const FLAG_COMPARABLE: u8 = 0b10;
    pub const FLAG_SIGNED: u8 = 0b100;
    pub const FLAG_FLOAT: u8 = 0b1000;

    pub const fn new(id: KnackTypeId) -> Self {
        Self([0, id, 0, 0])
    }
    
    pub fn assert_eq<K: AsKernelRef<Kernel = KnackKind>>(&self, other: &K) -> KnackResult<()> {
        if other.as_kernel_ref() != self {
            return Err(
                KnackError::new(
                    KnackErrorKind::WrongKind { 
                        expected: *self, 
                        got: *other.as_kernel_ref() 
                    }
                )
            )
        }

        Ok(())
    }

    pub fn as_sized(&self) -> Sized<'_, KnackKind> {
        if let Some(fixed) = self.try_as_fixed_sized() {
            return Sized::Fixed(fixed)
        } 

        if let Some(var) = self.try_as_var_sized() {
            return Sized::Var(var)
        }

        unreachable!("should be either fixed or var sized")
    }

    pub fn try_as_var_sized(&self) -> Option<&VarSized<KnackKind>> {
        if !self.is_sized() {
            unsafe {
                Some(std::mem::transmute(self))
            }
        } else {
            None
        }
    }


    pub fn try_as_fixed_sized(&self) -> Option<&FixedSized<KnackKind>> {
        if self.is_sized() {
            unsafe {
                Some(std::mem::transmute(self))
            }
        } else {
            None
        }
    }

    pub fn outer_size(&self) -> Option<usize> {
        self.try_as_fixed_sized().map(|sized| sized.outer_size())
    }

    pub fn inner_size(&self) -> Option<usize> {
        self.try_as_fixed_sized().map(|sized| sized.inner_size())
    }

    pub fn try_as_comparable(&self) -> Option<&Comparable<KnackKind>> {
        if self.is_comparable() {
            return unsafe {
                Some(std::mem::transmute(self))
            }
        } else {
            None
        }
    }

    pub const fn type_id(&self) -> KnackTypeId {
        self.0[1]
    }

    fn is_sized(&self) -> bool {
        self.0[0] & KnackKind::FLAG_SIZED > 0
    }

    fn is_comparable(&self) -> bool {
        self.0[0] & KnackKind::FLAG_COMPARABLE > 0
    }

    fn get_inner_size(&self) -> usize {
        return u16::from_le_bytes(self.0[2..4].try_into().unwrap()).try_into().unwrap()
    }
}

impl<L> FixedSized<L> where L: AsKernelRef<Kernel = KnackKind> {
    pub fn new(mut base: L, size: KnackSize) -> Self where L: AsKernelMut<Kernel = KnackKind> {
        base.as_kernel_mut().as_mut_bytes()[0] |= KnackKind::FLAG_SIZED;
        base.as_kernel_mut().as_mut_bytes()[2] = size.to_le_bytes()[0];
        base.as_kernel_mut().as_mut_bytes()[3] = size.to_le_bytes()[1];
        Self(base)
    }
}

impl FixedSized<KnackKind> {
    pub const fn into_inner(self) -> KnackKind { self.0 }

    pub fn outer_size(&self) -> usize {
        return size_of::<KnackKind>() + self.inner_size()
    }

    pub fn inner_size(&self) -> usize {
        return self.0.get_inner_size()
    }

    pub fn as_area(&self) -> Range<usize> {
        0..self.outer_size()
    }

    pub const fn type_id(&self) -> KnackTypeId {
        self.0.0[1]
    }
}

impl AsRef<KnackKind> for FixedSized<KnackKind> {
    fn as_ref(&self) -> &KnackKind {
        &self.0
    }
}

impl Comparable<FixedSized<KnackKind>> {
    pub const fn new(kind: FixedSized<KnackKind>, signed: bool, float: bool) -> Self {
        Self(
            FixedSized(KnackKind([
                kind.0.0[0] 
                    | KnackKind::FLAG_COMPARABLE 
                    | if signed {KnackKind::FLAG_SIGNED} else {0} 
                    | if float {KnackKind::FLAG_FLOAT} else {0},
                kind.0.0[1],
                kind.0.0[2],
                kind.0.0[3]
            ]))
        )
    }

    pub const fn is_signed(&self) -> bool {
        self.0.0.0[0] & KnackKind::FLAG_SIGNED > 0
    }

    pub const fn is_float(&self) -> bool {
        self.0.0.0[0] & KnackKind::FLAG_FLOAT > 0
    }

}

impl AsRef<FixedSized<KnackKind>> for Comparable<FixedSized<KnackKind>> {
    fn as_ref(&self) -> &FixedSized<KnackKind> {
        &self.0
    }
}

impl AsRef<KnackKind> for Comparable<FixedSized<KnackKind>> {
    fn as_ref(&self) -> &KnackKind {
        &self.0.0
    }
}

impl GetKnackKind for u8 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> Self::Kind {
        Comparable::new(
            FixedSized::new(
                KnackKind::new(U8_TYPE_ID), 
                1
            ), false, false)
    }
}

impl GetKnackKind for u16 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> Self::Kind {
        Comparable::new(FixedSized::new(KnackKind::new(U16_TYPE_ID), 2), false, false)
    }
}

impl GetKnackKind for u32 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(U32_TYPE_ID), 4)
    }
}

impl GetKnackKind for u64 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(U64_TYPE_ID), 8)
    }
}

impl GetKnackKind for u128 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(U128_TYPE_ID), 16)
    }
}

impl GetKnackKind for i8 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(I8_TYPE_ID), 1)
    }
}

impl GetKnackKind for i16 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(I16_TYPE_ID), 2)
    }
}

impl GetKnackKind for i32 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(I32_TYPE_ID), 4)
    }
}

impl GetKnackKind for i64 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(I64_TYPE_ID), 8)
    }
}

impl GetKnackKind for i128 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(I128_TYPE_ID), 16)
    }
}

impl GetKnackKind for f32 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(F32_TYPE_ID), 4)
    }
}

impl GetKnackKind for f64 {
    type Kind = FixedSized<KnackKind>;

    fn kind() -> Self::Kind {
        FixedSized::new(KnackKind::new(F64_TYPE_ID), 8)
    }
}

impl GetKnackKind for str {
    type Kind = KnackKind;

    fn kind() -> Self::Kind {
        KnackKind::new(STR_TYPE_ID)
    }
}

impl GetKnackKind for KeyValue {
    type Kind = VarSized<KnackKind>;

    fn kind() -> Self::Kind {
        VarSized::new(KnackKind::new(KV_PAIR_TYPE_ID))
    }
}

impl GetKnackKind for Document {
    type Kind = VarSized<KnackKind>;

    fn kind() -> Self::Kind {
        VarSized::new(KnackKind::new(DOCUMENT_TYPE_ID))
    }
}