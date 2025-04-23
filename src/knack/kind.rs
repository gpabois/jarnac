use core::slice;
use phf::phf_map;
use std::{any::Any, borrow::Borrow, convert::Infallible, fmt::{Debug, Display}, ops::Range};


use super::{
    document::{Document, KeyValue},
    error::{KnackError, KnackErrorKind},
    marker::{
        kernel::{AsKernelMut, AsKernelRef},
        sized::{Sized, VarSized},
        Comparable, FixedSized,
    },
    result::KnackResult,
    KnackTypeId,
};

pub(super) const ANY_TYPE_ID: KnackTypeId = 0;
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
pub(super) const FIXED_STR_TYPE_ID: KnackTypeId = 14;
pub(super) const DOCUMENT_TYPE_ID: KnackTypeId = 15;
pub(super) const KV_PAIR_TYPE_ID: KnackTypeId = 16;
pub(super) const ARRAY_FLAG: KnackTypeId = 128;

pub trait GetKnackKind {
    type Kind: AsKernelRef<Kernel=KnackKind> + ?std::marker::Sized + 'static;
    fn kind() -> &'static Self::Kind;
}

pub struct KnackKindDescriptor {
    type_name: &'static str,
    pub(crate) flags: u8,
    size: Option<u8>,
}

impl KnackKindDescriptor {
    pub const FLAG_SIZED: u8 = 0b1;
    pub const FLAG_COMPARABLE: u8 = 0b10;
    pub const FLAG_SIGNED: u8 = 0b100;
    pub const FLAG_FLOAT: u8 = 0b1000;
    pub const FLAG_DYN_SIZED: u8 = 0b10000;

    pub const fn new(type_name: &'static str) -> Self {
        Self {
            type_name,
            flags: 0,
            size: None,
        }
    }

    pub const fn comparable(mut self) -> Self {
        self.flags |= Self::FLAG_COMPARABLE;
        self
    }

    /// Le type est de taille fixe.
    pub const fn fixed_sized(mut self, size: u8) -> Self {
        self.flags |= Self::FLAG_SIZED;
        self.size = Some(size);
        self
    }

    /// Le type dynamique possède une taille fixe.
    pub const fn dyn_fixed_sized(mut self) -> Self {
        self.flags |= Self::FLAG_DYN_SIZED;
        self
    }

    pub fn is_dyn_fixed_sized(&self) -> bool {
        self.flags & Self::FLAG_DYN_SIZED > 0
    }

}

pub(super) static KNACK_KIND_DESCRIPTORS: phf::Map<KnackTypeId, KnackKindDescriptor> = phf_map! {
    0u8 => KnackKindDescriptor::new("any"),
    1u8 => KnackKindDescriptor::new("u8").comparable().fixed_sized(1),
    6u8 => KnackKindDescriptor::new("i8").comparable().fixed_sized(1),
    2u8 => KnackKindDescriptor::new("u16").comparable().fixed_sized(2),
    7u8 => KnackKindDescriptor::new("i16").comparable().fixed_sized(2),
    3u8 => KnackKindDescriptor::new("u32").comparable().fixed_sized(4),
    8u8 => KnackKindDescriptor::new("i32").comparable().fixed_sized(4),
    4u8 => KnackKindDescriptor::new("u64").comparable().fixed_sized(8),
    9u8 => KnackKindDescriptor::new("i64").comparable().fixed_sized(8),
    5u8 => KnackKindDescriptor::new("u128").comparable().fixed_sized(16),
    10u8 => KnackKindDescriptor::new("i128").comparable().fixed_sized(16),
    11u8 => KnackKindDescriptor::new("f32").comparable().fixed_sized(4),
    12u8 => KnackKindDescriptor::new("f64").comparable().fixed_sized(8),
    13u8 => KnackKindDescriptor::new("str"),
    14u8 => KnackKindDescriptor::new("fixed-str").comparable().dyn_fixed_sized()
};

pub struct KnackKindBuf(Vec<u8>);

impl Debug for KnackKindBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("KnackKindBuf").field(&self.0).finish()
    }
}

impl Display for KnackKindBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(Borrow::<KnackKind>::borrow(self), f)
    }
}

impl Borrow<KnackKind> for KnackKindBuf {
    fn borrow(&self) -> &KnackKind {
        unsafe {
            std::mem::transmute(self.0.as_slice())
        }
    }
}

impl ToOwned for KnackKind {
    type Owned = KnackKindBuf;

    fn to_owned(&self) -> Self::Owned {
        unsafe {
            let bytes: &[u8] = std::mem::transmute(self);
            KnackKindBuf(bytes.to_vec())
        }

    }
}


#[derive(Debug, PartialEq, Eq)]
pub struct KnackKind([u8]);

impl Display for KnackKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let type_name = KNACK_KIND_DESCRIPTORS
            .get(&self.0[0])
            .map(|desc| desc.type_name)
            .unwrap_or("unknown");
        write!(f, "{0}", type_name)
    }
}

impl TryFrom<&[u8]> for &KnackKind {
    type Error = Infallible;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let type_id = value[0];
        let desc = KNACK_KIND_DESCRIPTORS.get(&type_id).unwrap();

        if desc.is_dyn_fixed_sized() {
            unsafe { Ok(std::mem::transmute(&value[0..2])) }
        }
        else {
            unsafe { Ok(std::mem::transmute(&value[0..1])) }
        }
    }
}

impl AsRef<KnackKind> for KnackKind {
    fn as_ref(&self) -> &KnackKind {
        &self
    }
}

impl KnackKind {
    pub fn assert_same<K: AsKernelRef<Kernel = KnackKind> + ?std::marker::Sized>(&self, other: &K) -> KnackResult<()> {
        if other.as_kernel_ref() != self {
            return Err(KnackError::new(KnackErrorKind::WrongKind {
                expected: self.to_owned(),
                got: other.as_kernel_ref().to_owned(),
            }));
        }

        Ok(())
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

    pub fn try_as_array(&self) -> Option<&super::marker::Array<KnackKind>> {
        self.is_array()
        .then(|| unsafe {
            std::mem::transmute(self)
        })
    }

    pub const fn type_id(&self) -> &KnackTypeId {
        &self.0[0]
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::mem::transmute(self)
        }
    }

    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    fn is_array(&self) -> bool {
        self.0[0] & ARRAY_FLAG > 0
    }

    fn is_sized(&self) -> bool {
        KNACK_KIND_DESCRIPTORS
            .get(self.type_id())
            .map(|desc| desc.flags & KnackKindDescriptor::FLAG_SIZED > 0)
            .unwrap_or_default()
    }

    fn is_comparable(&self) -> bool {
        KNACK_KIND_DESCRIPTORS
            .get(self.type_id())
            .map(|desc| desc.flags & KnackKindDescriptor::FLAG_COMPARABLE > 0)
            .unwrap_or_default()
    }
}

impl FixedSized<KnackKind> {

    pub fn outer_size(&self) -> usize {
        return self.as_kernel_ref().len() + self.inner_size();
    }

    pub fn inner_size(&self) -> usize {
        return usize::from(
            KNACK_KIND_DESCRIPTORS
                .get(self.0.type_id())
                .unwrap()
                .size
                .unwrap(),
        );
    }

    pub fn as_area(&self) -> Range<usize> {
        0..self.outer_size()
    }
}

impl AsRef<KnackKind> for FixedSized<KnackKind> {
    fn as_ref(&self) -> &KnackKind {
        &self.0
    }
}

impl GetKnackKind for u8 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[U8_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for u16 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[U16_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for u32 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[U32_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for u64 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[U64_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for u128 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[U128_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for i8 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[I8_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for i16 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[I16_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for i32 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[I32_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for i64 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[I64_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for i128 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[I128_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for f32 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[F32_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for f64 {
    type Kind = Comparable<FixedSized<KnackKind>>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[F64_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for str {
    type Kind = VarSized<KnackKind>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[STR_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for KeyValue {
    type Kind = VarSized<KnackKind>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[KV_PAIR_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for Document {
    type Kind = VarSized<KnackKind>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[DOCUMENT_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

impl GetKnackKind for dyn Any {
    type Kind = VarSized<KnackKind>;

    fn kind() -> &'static Self::Kind {
        unsafe {
            let raw: &'static [u8] = &[ANY_TYPE_ID];
            std::mem::transmute(raw)
        }
    }
}

