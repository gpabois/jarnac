use std::ops::Deref;
use std::borrow::Borrow;

use super::{
    buf::KnackBuf, kind::{
        KnackKind, F32_TYPE_ID, F64_TYPE_ID, I128_TYPE_ID, I16_TYPE_ID, I32_TYPE_ID, I64_TYPE_ID,
        I8_TYPE_ID, U128_TYPE_ID, U16_TYPE_ID, U32_TYPE_ID, U64_TYPE_ID, U8_TYPE_ID,
    }, marker::{
        kernel::{AsKernelMut, AsKernelRef},
        Comparable, ComparableAndFixedSized,
    }, Knack
};

impl<L> Comparable<L>
where
    L: AsKernelRef<Kernel = KnackKind>,
{
    /// offset-size : xx [111:offset] [111:size]
    /// Size is a power of 2
    /// Offset is a power of 2
    pub(crate) fn new(base: L) -> Self
    where
        L: AsKernelMut<Kernel = KnackKind>,
    {
        Self(base)
    }
}

impl Deref for Comparable<Knack> {
    type Target = Knack;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Comparable<Knack> {
    pub fn kind(&self) -> &Comparable<KnackKind> {
        self.0.kind().try_as_comparable().unwrap()
    }
}

impl PartialEq<Comparable<KnackBuf>> for Comparable<Knack> {
    fn eq(&self, other: &Comparable<KnackBuf>) -> bool {
        let knack: &Knack = other.as_kernel_ref().borrow();
        self.eq(knack)
    }
}

impl PartialEq<Comparable<Knack>> for Comparable<Knack> {
    fn eq(&self, other: &Comparable<Knack>) -> bool {
        let knack: &Knack = other.as_kernel_ref();
        self.eq(knack)
    }
}


impl PartialEq<Knack> for Comparable<Knack> {
    fn eq(&self, other: &Knack) -> bool {
        self.kind().as_kernel_ref() == other.kind()
            && self.as_value_bytes() == other.as_value_bytes()
    }
}

impl PartialOrd<Knack> for Comparable<Knack> {
    fn partial_cmp(&self, other: &Knack) -> Option<std::cmp::Ordering> {
        if self.kind().as_kernel_ref() != other.kind() {
            return None;
        }

        let other = other.try_as_comparable().unwrap();
        let type_id = self.kind().type_id();

        match *type_id {
            U8_TYPE_ID => self.cast::<u8>().partial_cmp(other.cast::<u8>()),
            I8_TYPE_ID => self.cast::<i8>().partial_cmp(other.cast::<i8>()),
            U16_TYPE_ID => self.cast::<u16>().partial_cmp(other.cast::<u16>()),
            I16_TYPE_ID => self.cast::<i16>().partial_cmp(other.cast::<i16>()),
            U32_TYPE_ID => self.cast::<u32>().partial_cmp(other.cast::<u32>()),
            I32_TYPE_ID => self.cast::<i32>().partial_cmp(other.cast::<i32>()),
            U64_TYPE_ID => self.cast::<u64>().partial_cmp(other.cast::<u64>()),
            I64_TYPE_ID => self.cast::<i64>().partial_cmp(other.cast::<i64>()),
            U128_TYPE_ID => self.cast::<u128>().partial_cmp(other.cast::<u128>()),
            I128_TYPE_ID => self.cast::<i128>().partial_cmp(other.cast::<i128>()),
            F32_TYPE_ID => self.cast::<f32>().partial_cmp(other.cast::<f32>()),
            F64_TYPE_ID => self.cast::<f64>().partial_cmp(other.cast::<f64>()),
            _ => None,
        }
    }
}

