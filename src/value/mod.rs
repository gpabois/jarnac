//!
//! Numerics are between 1-12
//! Integers are between 1-10;
//! Unsigned integers are between 1-5;
//! Signed integers are between 6-10;
//! Floats are between 11-12;

use std::{borrow::Borrow, fmt::Display, io::Write, ops::Deref};

use zerocopy::{FromBytes, LittleEndian};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::pager::{error::PagerError, page::PageSlice, PagerResult};
pub mod numeric;

pub const U8: ValueKind     = ValueKind(1);
pub const U16: ValueKind    = ValueKind(2);
pub const U32: ValueKind    = ValueKind(3);
pub const U64: ValueKind    = ValueKind(4);
pub const U128: ValueKind   = ValueKind(5);
pub const I8: ValueKind     = ValueKind(6);
pub const I16: ValueKind    = ValueKind(7);
pub const I32: ValueKind    = ValueKind(8);
pub const I64: ValueKind    = ValueKind(9);
pub const I128: ValueKind   = ValueKind(10);
pub const F32: ValueKind    = ValueKind(11);
pub const F64: ValueKind    = ValueKind(12);

const ARRAY_KIND_FLAG: u8 = 128;

pub trait IntoValueBuf {
    fn into_value_buf(self) -> ValueBuf;
}

impl<U> IntoValueBuf for U where ValueBuf: From<U> {
    fn into_value_buf(self) -> ValueBuf {
        ValueBuf::from(self)
    }
}


#[derive(Debug, PartialEq, Eq, Clone, Copy, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct ValueKind(u8);

impl Display for ValueKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            U8 => f.write_str("u8"),
            U16 => f.write_str("u16"),
            U32 => f.write_str("u32"),
            U64 => f.write_str("u64"),
            U128 => f.write_str("u128"),
            I8 => f.write_str("i8"),
            I16 => f.write_str("i16"),
            I32 => f.write_str("i32"),
            I64 => f.write_str("i64"),
            I128 => f.write_str("i128"),
            F32 => f.write_str("f32"),
            F64 => f.write_str("f64"),
            _ => f.write_str("unknown")
        }
    }
}

impl From<u8> for ValueKind {
    fn from(value: u8) -> Self {
        Self(value)
    }
}
impl Into<u8> for ValueKind {
    fn into(self) -> u8 {
        self.0
    }
}
impl ValueKind {
    /// Détermine la portion dédiée au stockage de la valeur
    pub fn get_slice<'a>(&self, src: &'a [u8]) -> &'a [u8] {
        if let Some(size) = self.full_size() {
            return &src[..size];
        }

        todo!("implement var-sized data");
    }

    pub fn get_mut_slice<'a>(&self, src: &'a mut [u8]) -> &'a mut [u8] {
        if let Some(size) = self.full_size() {
            return &mut src[..size];
        }

        todo!("implement var-sized data");
    }
    
    pub fn assert_eq(&self, other: &ValueKind) -> PagerResult<()> {
        if *other != *self {
            return Err(PagerError::new(crate::pager::error::PagerErrorKind::WrongValueKind { expected: U8, got: *other }))
        }

        Ok(())
    }
    pub fn as_array(&self) -> ValueKind {
        Self(self.0 | ARRAY_KIND_FLAG)
    }

    pub fn is_array(&self) -> bool {
        self.0 & ARRAY_KIND_FLAG == ARRAY_KIND_FLAG
    }

    pub fn element_kind(&self) -> ValueKind {
        assert!(self.is_array(), "not an array");
        Self(self.0 & !ARRAY_KIND_FLAG)
    }

    pub fn is_numeric(&self) -> bool {
        self.0 >=1 && self.0 <= 12
    }

    pub fn is_integer(&self) -> bool {
        self.0 <= 10 && self.0 >=1
    }

    /// Taille en mémoire de la valeur (incluant le byte de type)
    /// 
    /// Un retour à None signifie que la valeur est de taille variable.
    pub fn full_size(&self) -> Option<usize> {
        self.size().map(|i| i + 1)
    }
    /// Taille de la valeur en retirait le byte de type.
    /// 
    /// Un retour à None signifie que la valeur est de taille variable.
    pub fn size(&self) -> Option<usize> {
        match *self {
            U8 | I8 => Some(1),
            U16 | I16 => Some(2),
            U32 | I32 | F32 => Some(4),
            U64 | I64 | F64 => Some(8),
            U128 | I128 => Some(16),
            _ => None
        }
    }
}

pub struct Value([u8]);

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self.kind() {
            U8 => self.try_as_u8().unwrap().fmt(f),
            U16 => self.try_as_u16().unwrap().fmt(f),
            U32 => self.try_as_u32().unwrap().fmt(f),
            U64 => self.try_as_u64().unwrap().fmt(f),
            U128 => self.try_as_u128().unwrap().fmt(f),
            I8 => self.try_as_i8().unwrap().fmt(f),
            I16 => self.try_as_i16().unwrap().fmt(f),
            I32 => self.try_as_i32().unwrap().fmt(f),
            I64 => self.try_as_i64().unwrap().fmt(f),
            I128 => self.try_as_i128().unwrap().fmt(f),
            _ => write!(f, ":unknown:")
        }
    }
}

impl From<&PageSlice> for &Value {
    fn from(value: &PageSlice) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}
impl PartialEq<Self> for Value {
    fn eq(&self, other: &Self) -> bool {
        if self.kind() != other.kind() { return false }

        match *self.kind() {
            U8 => self.try_as_u8().unwrap().eq(other.try_as_u8().unwrap()),
            U16 => self.try_as_u16().unwrap().eq(other.try_as_u16().unwrap()),
            U32 => self.try_as_u32().unwrap().eq(other.try_as_u32().unwrap()),
            U64 => self.try_as_u64().unwrap().eq(other.try_as_u64().unwrap()),
            U128 => self.try_as_u128().unwrap().eq(other.try_as_u128().unwrap()),
            I8 => self.try_as_i8().unwrap().eq(other.try_as_i8().unwrap()),
            I16 => self.try_as_i16().unwrap().eq(other.try_as_i16().unwrap()),
            I32 => self.try_as_i32().unwrap().eq(other.try_as_i32().unwrap()),
            I64 => self.try_as_i64().unwrap().eq(other.try_as_i64().unwrap()),
            I128 => self.try_as_i128().unwrap().eq(other.try_as_i128().unwrap()),
            _ => false
        }
    }
}
impl PartialOrd<Self> for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.kind() != other.kind() { return None }

        match *self.kind() {
            U8 => self.try_as_u8().unwrap().partial_cmp(other.try_as_u8().unwrap()),
            U16 => self.try_as_u16().unwrap().partial_cmp(other.try_as_u16().unwrap()),
            U32 => self.try_as_u32().unwrap().partial_cmp(other.try_as_u32().unwrap()),
            U64 => self.try_as_u64().unwrap().partial_cmp(other.try_as_u64().unwrap()),
            U128 => self.try_as_u128().unwrap().partial_cmp(other.try_as_u128().unwrap()),
            I8 => self.try_as_i8().unwrap().partial_cmp(other.try_as_i8().unwrap()),
            I16 => self.try_as_i16().unwrap().partial_cmp(other.try_as_i16().unwrap()),
            I32 => self.try_as_i32().unwrap().partial_cmp(other.try_as_i32().unwrap()),
            I64 => self.try_as_i64().unwrap().partial_cmp(other.try_as_i64().unwrap()),
            I128 => self.try_as_i128().unwrap().partial_cmp(other.try_as_i128().unwrap()),
            _ => None
        }
    }
}
impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Value {
    pub(crate) fn from_ref(bytes: &[u8]) -> &Self {
        unsafe {
            std::mem::transmute(bytes)
        }
    }

    pub(crate) fn from_mut(bytes: &mut [u8]) -> &mut Self {
        unsafe {
            std::mem::transmute(bytes)
        }
    }


    pub fn kind(&self) -> &ValueKind {
        unsafe {
            std::mem::transmute(&self.0[0])
        }
    }

    pub fn set(&mut self, value: &Self) {
        self.0.clone_from_slice(value.deref());
    }

    pub fn try_as_u8(&self) -> PagerResult<&ValU8> {
        self.try_into()
    }

    pub fn try_as_u16(&self) -> PagerResult<&ValU16> {
        self.try_into()
    }

    pub fn try_as_u32(&self) -> PagerResult<&ValU32> {
        self.try_into()
    }

    pub fn try_as_u64(&self) -> PagerResult<&ValU64> {
        self.try_into()
    }

    pub fn try_as_u128(&self) -> PagerResult<&ValU128> {
        self.try_into()
    }

    pub fn try_as_i8(&self) -> PagerResult<&ValI8> {
        self.try_into()
    }

    pub fn try_as_i16(&self) -> PagerResult<&ValI16> {
        self.try_into()
    }

    pub fn try_as_i32(&self) -> PagerResult<&ValI32> {
        self.try_into()
    }

    pub fn try_as_i64(&self) -> PagerResult<&ValI64> {
        self.try_into()
    }

    pub fn try_as_i128(&self) -> PagerResult<&ValI128> {
        self.try_into()
    }
}

/// Valeur entièrement détenue par l'objet (Owned value)
pub struct ValueBuf(Vec<u8>);

impl ValueBuf {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl PartialEq<Self> for ValueBuf {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.borrow())
    }
}

impl PartialOrd<Self> for ValueBuf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.borrow())
    }
}

impl Deref for ValueBuf {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        self.borrow()
    }
}
impl ToOwned for Value {
    type Owned = ValueBuf;

    fn to_owned(&self) -> Self::Owned {
        ValueBuf(self.0.to_owned())
    }
}
impl Borrow<Value> for ValueBuf {
    fn borrow(&self) -> &Value {
        unsafe {
            std::mem::transmute(self.0.as_slice())
        }
    }
}
impl From<u8> for ValueBuf {
    fn from(value: u8) -> Self {       
        Self(vec![U8.into(), value])
    }
}

impl From<&[u8]> for ValueBuf {
    fn from(value: &[u8]) -> Self {
        let kind = U8.as_array();
        let len = u32::try_from(value.len()).expect("Array length is limited to 2^32-1");
        let mut buf = Vec::<u8>::with_capacity(1 + 4 + usize::try_from(len).unwrap());
        buf.push(kind.into());
        buf.write_all(&len.to_le_bytes()).unwrap();
        buf.write_all(value).unwrap();
        Self(buf)
    }
}

impl From<u16> for ValueBuf {
    fn from(value: u16) -> Self {
        let mut buf = vec![U16.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<&[u16]> for ValueBuf {
    fn from(value: &[u16]) -> Self {
        let kind = U16.as_array();
        let len = u32::try_from(value.len()).expect("Array length is limited to 2^32-1");
        let mut buf = Vec::<u8>::with_capacity(1 + 4 + usize::try_from(len).unwrap() * size_of::<u16>());
        buf.push(kind.into());
        buf.write_all(&len.to_le_bytes()).unwrap();
        for el in value.iter() {
            buf.write_all(&el.to_le_bytes()).unwrap();
        }
        Self(buf)
    }
}

impl From<u32> for ValueBuf {
    fn from(value: u32) -> Self {
        let mut buf = vec![U32.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<&[u32]> for ValueBuf {
    fn from(value: &[u32]) -> Self {
        let kind = U16.as_array();
        let len = u32::try_from(value.len()).expect("Array length is limited to 2^32-1");
        let mut buf = Vec::<u8>::with_capacity(1 + 4 + usize::try_from(len).unwrap() * size_of::<u32>());
        buf.push(kind.into());
        buf.write_all(&len.to_le_bytes()).unwrap();
        for el in value.iter() {
            buf.write_all(&el.to_le_bytes()).unwrap();
        }
        Self(buf)
    }
}

impl From<u64> for ValueBuf {
    fn from(value: u64) -> Self {
        let mut buf = vec![U64.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<&[u64]> for ValueBuf {
    fn from(value: &[u64]) -> Self {
        let kind = U16.as_array();
        let len = u32::try_from(value.len()).expect("Array length is limited to 2^32-1");
        let mut buf = Vec::<u8>::with_capacity(1 + 4 + usize::try_from(len).unwrap() * size_of::<u64>());
        buf.push(kind.into());
        buf.write_all(&len.to_le_bytes()).unwrap();
        for el in value.iter() {
            buf.write_all(&el.to_le_bytes()).unwrap();
        }
        Self(buf)
    }
}

impl From<u128> for ValueBuf {
    fn from(value: u128) -> Self {
        let mut buf = vec![U128.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<&[u128]> for ValueBuf {
    fn from(value: &[u128]) -> Self {
        let kind = U16.as_array();
        let len = u32::try_from(value.len()).expect("Array length is limited to 2^32-1");
        let mut buf = Vec::<u8>::with_capacity(1 + 4 + usize::try_from(len).unwrap() * size_of::<u128>());
        buf.push(kind.into());
        buf.write_all(&len.to_le_bytes()).unwrap();
        for el in value.iter() {
            buf.write_all(&el.to_le_bytes()).unwrap();
        }
        Self(buf)
    }
}

impl From<i8> for ValueBuf {
    fn from(value: i8) -> Self {       
        Self(vec![I8.into(), unsafe {std::mem::transmute(value)}])
    }
}


impl From<i16> for ValueBuf {
    fn from(value: i16) -> Self {
        let mut buf = vec![I16.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<i32> for ValueBuf {
    fn from(value: i32) -> Self {
        let mut buf = vec![I32.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<i64> for ValueBuf {
    fn from(value: i64) -> Self {
        let mut buf = vec![I64.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

impl From<i128> for ValueBuf {
    fn from(value: i128) -> Self {
        let mut buf = vec![I128.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}

pub struct ValU8([u8]);

impl PartialEq<Self> for ValU8 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValU8 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValU8 {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0[1]
    }
}

impl TryFrom<&Value> for &ValU8 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        U8.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValU16([u8]);

impl PartialEq<Self> for ValU16 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValU16 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValU16 {
    type Target = zerocopy::U16<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U16::<LittleEndian>::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValU16 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        U16.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValU32([u8]);

impl PartialEq<Self> for ValU32 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValU32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValU32 {
    type Target = zerocopy::U32<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U32::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValU32 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        U32.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValU64([u8]);

impl ValU64 {
    pub fn to_owned(&self) -> u64 {
        self.into()
    }
}

impl Into<u64> for &ValU64 {
    fn into(self) -> u64 {
        self.deref().get()
    }
}

impl PartialEq<Self> for ValU64 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValU64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValU64 {
    type Target = zerocopy::U64<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U64::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValU64 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        U64.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValU128([u8]);

impl PartialEq<Self> for ValU128 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValU128 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValU128 {
    type Target = zerocopy::U128<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U128::ref_from_bytes(&self.0[1..]).unwrap()
    }
}


impl TryFrom<&Value> for &ValU128 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        U128.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValI8([u8]);

impl PartialEq<Self> for ValI8 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValI8 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValI8 {
    type Target = i8;

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::mem::transmute(&self.0[1])
        }
    }
}

impl TryFrom<&Value> for &ValI8 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        I8.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValI16([u8]);

impl PartialEq<Self> for ValI16 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValI16 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValI16 {
    type Target = zerocopy::I16<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I16::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValI16 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        I16.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValI32([u8]);

impl PartialEq<Self> for ValI32 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValI32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValI32 {
    type Target = zerocopy::I32<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I32::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValI32 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        I32.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValI64([u8]);

impl PartialEq<Self> for ValI64 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValI64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValI64 {
    type Target = zerocopy::I64<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I64::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValI64 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        I64.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValI128([u8]);

impl PartialEq<Self> for ValI128 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl PartialOrd<Self> for ValI128 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}

impl Deref for ValI128 {
    type Target = zerocopy::I128<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I128::ref_from_bytes(&self.0[1..]).unwrap()
    }
}

impl TryFrom<&Value> for &ValI128 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        I128.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IntoValueBuf;

    #[test]
    fn test_sizes() {
        assert_eq!(10u8.into_value_buf().kind().size(), Some(1), "unsigned int8 must have a size of 1");
        assert_eq!(10i8.into_value_buf().kind().size(), Some(1), "signed int8 must have a size of 1");
        assert_eq!(10u16.into_value_buf().kind().size(), Some(2), "unsigned int16 must have a size of 2");
        assert_eq!(10i16.into_value_buf().kind().size(), Some(2), "signed int16 must have a size of 2");
        assert_eq!(10u32.into_value_buf().kind().size(), Some(4), "unsigned int32 must have a size of 4");
        assert_eq!(10i32.into_value_buf().kind().size(), Some(4), "signed int32 must have a size of 4");
        assert_eq!(10u64.into_value_buf().kind().size(), Some(8), "unsigned int64 must have a size of 8");
        assert_eq!(10i64.into_value_buf().kind().size(), Some(8), "signed int64 must have a size of 8");
        assert_eq!(10u128.into_value_buf().kind().size(), Some(16), "unsigned int128 must have a size of 16");
        assert_eq!(10i128.into_value_buf().kind().size(), Some(16), "signed int128 must have a size of 16");
    }

    #[test]
    fn test_partial_ord() {
        let v1 = 10u8.into_value_buf();
        let v2 = 11u8.into_value_buf();
        let v4 = 12u8.into_value_buf();
        let v3 = 11u16.into_value_buf();

        assert!(v1 <= v2, "v2 must be >= v1");
        assert!(v4 > v2, "v4 must be > v2");
        assert!(!(v1 <= v3), "v3 must not be compared to v1 as they are not of the same kind");
    }
}