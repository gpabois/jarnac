use std::{borrow::{Borrow, BorrowMut}, io::Write, ops::{Deref, DerefMut}};

use byteorder::WriteBytesExt;
use zerocopy::IntoBytes;

use super::{builder::KnackBuilder, document::KeyValue, kind::GetKnackKind, Knack};


pub trait IntoKnackBuf {
    fn into_value_buf(self) -> KnackBuf;
}

impl<U> IntoKnackBuf for U where KnackBuf: From<U> {
    fn into_value_buf(self) -> KnackBuf {
        KnackBuf::from(self)
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
        buf.write_all(KeyValue::KIND.as_bytes()).unwrap();
        let v = kv.1.into_value_buf();
        let k = kv.0;
        let size: u32 = u32::try_from(k.len() + v.len()).unwrap();
        buf.write_all(&size.to_le_bytes()).unwrap();
        buf.write_all(k.as_bytes()).unwrap();
        buf.write_all(v.as_bytes()).unwrap();
        Self(buf)
    }
}
impl From<u8> for KnackBuf {
    fn from(value: u8) -> Self {    
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u8::KIND.as_bytes()).unwrap();
        buf.write_u8(value).unwrap();
        Self(buf)
    }
}
impl From<u16> for KnackBuf {
    fn from(value: u16) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u16::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<u32> for KnackBuf {
    fn from(value: u32) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u32::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<u64> for KnackBuf {
    fn from(value: u64) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u64::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<u128> for KnackBuf {
    fn from(value: u128) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(u128::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i8> for KnackBuf {
    fn from(value: i8) -> Self {       
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i8::KIND.as_bytes()).unwrap();
        buf.write_i8(value).unwrap();
        Self(buf)
    }
}
impl From<i16> for KnackBuf {
    fn from(value: i16) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i16::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i32> for KnackBuf {
    fn from(value: i32) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i32::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i64> for KnackBuf {
    fn from(value: i64) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i64::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i128> for KnackBuf {
    fn from(value: i128) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(i128::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<f32> for KnackBuf {
    fn from(value: f32) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(f32::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<f64> for KnackBuf {
    fn from(value: f64) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(f64::KIND.as_bytes()).unwrap();
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<String> for KnackBuf {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}
impl From<&str> for KnackBuf {
    fn from(value: &str) -> Self {
        let mut buf: Vec<u8> = vec![];
        buf.write_all(str::KIND.as_bytes()).unwrap();
        buf.write_all(&value.as_bytes()).unwrap();
        Self(buf)
    }
}