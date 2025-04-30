use std::{borrow::{Borrow, BorrowMut}, io::Write, ops::{Deref, DerefMut, Range}};

use byteorder::WriteBytesExt;
use zerocopy::IntoBytes;

use super::marker::{kernel::{AsKernelMut, AsKernelRef, IntoKernel}, ComparableAndFixedSized, FixedSized};

use super::{builder::KnackBuilder, document::KeyValue, kind::GetKnackKind, Knack};

pub trait IntoKnackBuf {
    type Buf: Borrow<Knack>;

    fn into_knack_buf(self) -> Self::Buf;
}

impl IntoKernel for KnackBuf {
    type Kernel = Self;

    fn into_kernel(self) -> Self::Kernel {
        self
    }
}

impl AsKernelRef for KnackBuf {
    type Kernel = Self;

    fn as_kernel_ref(&self) -> &Self::Kernel {
        self
    }
}

impl BorrowMut<Knack> for ComparableAndFixedSized<KnackBuf> {
    fn borrow_mut(&mut self) -> &mut Knack {
        self.as_kernel_mut().borrow_mut()
    }
}

impl Borrow<Knack> for ComparableAndFixedSized<KnackBuf> {
    fn borrow(&self) -> &Knack {
        self.as_kernel_ref().borrow()
    }
} 

impl Borrow<ComparableAndFixedSized<Knack>> for ComparableAndFixedSized<KnackBuf> {
    fn borrow(&self) -> &ComparableAndFixedSized<Knack> {
        let knack_ref: &Knack = self.as_kernel_ref().borrow();
        unsafe  {
            std::mem::transmute(knack_ref)
        }
    }
} 

impl Deref for ComparableAndFixedSized<KnackBuf> {
    type Target = Knack;

    fn deref(&self) -> &Self::Target {
        let knack: &Knack = self.borrow();
        knack
    }
}

impl DerefMut for ComparableAndFixedSized<KnackBuf> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let knack: &mut Knack = &mut self.0.0;
        knack
    }
}

impl FixedSized<KnackBuf> {
    pub fn range(&self) -> Range<usize> {
        let knack: &Knack = self.0.borrow();
        unsafe {
            FixedSized::from_ref_unchecked(knack).range()
        }

    }
}

#[derive(Hash)]
/// Valeur entièrement détenue par l'objet (Owned value)
pub struct KnackBuf(Vec<u8>);
impl std::fmt::Display for KnackBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}
impl KnackBuf {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Deref for KnackBuf {
    type Target = Knack;

    fn deref(&self) -> &Self::Target {
        self.borrow()
    }
}
impl DerefMut for KnackBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.borrow_mut()
    }
}
impl ToOwned for Knack {
    type Owned = KnackBuf;

    fn to_owned(&self) -> Self::Owned {
        KnackBuf(self.0.to_owned())
    }
}
impl BorrowMut<Knack> for KnackBuf {
    fn borrow_mut(&mut self) -> &mut Knack {
        unsafe {
            std::mem::transmute(self.0.as_mut_slice())
        }
    }
}
impl Borrow<Knack> for KnackBuf {
    fn borrow(&self) -> &Knack {
        unsafe {
            std::mem::transmute(self.0.as_slice())
        }
    }
}
impl From<(String, KnackBuilder)> for KnackBuf {
    fn from(kv: (String, KnackBuilder)) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(KeyValue::kind().as_kernel_ref().as_bytes()).unwrap();
        let v = kv.1.into_knack_buf();
        let k = kv.0;
        let size: u32 = u32::try_from(k.len() + v.as_bytes().len()).unwrap();
        buf.write_all(&size.to_le_bytes()).unwrap();
        buf.write_all(k.as_bytes()).unwrap();
        buf.write_all(v.as_bytes()).unwrap();
        Self(buf)
    }
}

impl IntoKnackBuf for u8 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<u8> for KnackBuf {
    fn from(value: u8) -> Self {    
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u8::kind().as_bytes()).unwrap();
        buf.write_u8(value).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for u16 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<u16> for KnackBuf {
    fn from(value: u16) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u16::kind().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for u32 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<u32> for KnackBuf {
    fn from(value: u32) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u32::kind().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for u64 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<u64> for KnackBuf {
    fn from(value: u64) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u64::kind().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for u128 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<u128> for KnackBuf {
    fn from(value: u128) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u128::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for i8 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<i8> for KnackBuf {
    fn from(value: i8) -> Self {       
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i8::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_i8(value).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for i16 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<i16> for KnackBuf {
    fn from(value: i16) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i16::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for i32 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<i32> for KnackBuf {
    fn from(value: i32) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i32::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for i64 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<i64> for KnackBuf {
    fn from(value: i64) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i64::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for i128 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<i128> for KnackBuf {
    fn from(value: i128) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i128::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for f32 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<f32> for KnackBuf {
    fn from(value: f32) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(f32::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for f64 {
    type Buf = ComparableAndFixedSized<KnackBuf>;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        unsafe {
            std::mem::transmute(buf)
        }
    }
}
impl From<f64> for KnackBuf {
    fn from(value: f64) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(f64::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl IntoKnackBuf for String {
    type Buf = KnackBuf;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        buf
    }
}
impl From<String> for KnackBuf {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}
impl IntoKnackBuf for &str {
    type Buf = KnackBuf;

    fn into_knack_buf(self) -> Self::Buf {
        let buf = KnackBuf::from(self);
        buf
    }
}
impl From<&str> for KnackBuf {
    fn from(value: &str) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(str::kind().as_kernel_ref().as_bytes()).unwrap();
        buf.write_all(&value.as_bytes()).unwrap();
        Self(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::IntoKnackBuf;

    #[test]
    fn test_str() {
        let knack = &"test".into_knack_buf();

        assert!(knack.is::<str>());
        assert_eq!(knack.cast::<str>(), "test");
    }
}