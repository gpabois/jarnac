//!
//! Numerics are between 1-12
//! Integers are between 1-10;
//! Unsigned integers are between 1-5;
//! Signed integers are between 6-10;
//! Floats are between 11-12;

pub mod document;
pub mod array;
pub mod builder;

use std::{borrow::Borrow, collections::VecDeque, fmt::Display, io::Write, ops::Deref};

use builder::ValueBuilder;
use document::Document;
use zerocopy::{FromBytes, IntoBytes, LittleEndian};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::pager::{error::PagerError, page::PageSlice, PagerResult};

const U8: ValueKind     = ValueKind(1);
const U16: ValueKind    = ValueKind(2);
const U32: ValueKind    = ValueKind(3);
const U64: ValueKind    = ValueKind(4);
const U128: ValueKind   = ValueKind(5);
const I8: ValueKind     = ValueKind(6);
const I16: ValueKind    = ValueKind(7);
const I32: ValueKind    = ValueKind(8);
const I64: ValueKind    = ValueKind(9);
const I128: ValueKind   = ValueKind(10);
const F32: ValueKind    = ValueKind(11);
const F64: ValueKind    = ValueKind(12);

const STR: ValueKind    = ValueKind(13);
const DOCUMENT: ValueKind = ValueKind(14);
const KV_PAIR: ValueKind = ValueKind(15);

const ARRAY_KIND_FLAG: u8 = 128;

pub trait IntoValueBuilder {
    fn into_value_builder(self) -> ValueBuilder;
}

pub trait FromValueBuilder {
    type Output;

    fn borrow_value(value: &ValueBuilder) -> &Self;
    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self;
}

pub trait IntoValuePath{
    fn into_value_path(self) -> ValuePath;
}

impl<V> IntoValuePath for V where ValuePath: From<V> {
    fn into_value_path(self) -> ValuePath {
        self.into()
    }
}

pub struct ValuePath(VecDeque<String>);

impl ValuePath {
    pub fn pop(&mut self) -> Option<String> {
        self.0.pop_front()
    }
}

impl From<&str> for ValuePath {
    fn from(value: &str) -> Self {
        Self(value.split(".").into_iter().map(|seg| seg.to_owned()).collect())
    }
}



pub trait GetValueKind {
    fn get_value_kind() -> ValueKind;
}

pub trait FromValue: GetValueKind {
    type Output: ?Sized;

    fn from_value(value: &Value) -> &Self::Output {
        Self::try_from_value(value).expect("wrong value type")
    }

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output>;
}

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
            STR => f.write_str("str"),
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
            U8 => self.cast::<u8>().fmt(f),
            U16 => self.cast::<u16>().fmt(f),
            U32 => self.cast::<u32>().fmt(f),
            U64 => self.cast::<u64>().fmt(f),
            U128 => self.cast::<u128>().fmt(f),
            I8 => self.cast::<i8>().fmt(f),
            I16 => self.cast::<i16>().fmt(f),
            I32 => self.cast::<i32>().fmt(f),
            I64 => self.cast::<i64>().fmt(f),
            I128 => self.cast::<i128>().fmt(f),
            F32 => self.cast::<f32>().fmt(f),
            F64 => self.cast::<f64>().fmt(f),
            STR => self.cast::<str>().fmt(f),
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
            U8 => self.cast::<u8>().eq(other.cast::<u8>()),
            U16 => self.cast::<u16>().eq(other.cast::<u16>()),
            U32 => self.cast::<u32>().eq(other.cast::<u32>()),
            U64 => self.cast::<u64>().eq(other.cast::<u64>()),
            U128 => self.cast::<u128>().eq(other.cast::<u128>()),
            I8 => self.cast::<i8>().eq(other.cast::<i8>()),
            I16 => self.cast::<i16>().eq(other.cast::<i16>()),
            I32 => self.cast::<i32>().eq(other.cast::<i32>()),
            I64 => self.cast::<i64>().eq(other.cast::<i64>()),
            I128 => self.cast::<i128>().eq(other.cast::<i128>()),
            F32 => self.cast::<f32>().eq(other.cast::<f32>()),
            F64 => self.cast::<f64>().eq(other.cast::<f64>()),
            STR => self.cast::<str>().eq(other.cast::<str>()),
            _ => false
        }
    }
}
impl PartialOrd<Self> for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.kind() != other.kind() { return None }

        match *self.kind() {
            U8 => self.cast::<u8>().partial_cmp(other.cast::<u8>()),
            U16 => self.cast::<u16>().partial_cmp(other.cast::<u16>()),
            U32 => self.cast::<u32>().partial_cmp(other.cast::<u32>()),
            U64 => self.cast::<u64>().partial_cmp(other.cast::<u64>()),
            U128 => self.cast::<u128>().partial_cmp(other.cast::<u128>()),
            I8 => self.cast::<i8>().partial_cmp(other.cast::<i8>()),
            I16 => self.cast::<i16>().partial_cmp(other.cast::<i16>()),
            I32 => self.cast::<i32>().partial_cmp(other.cast::<i32>()),
            I64 => self.cast::<i64>().partial_cmp(other.cast::<i64>()),
            I128 => self.cast::<i128>().partial_cmp(other.cast::<i128>()),
            F32 => self.cast::<f32>().partial_cmp(other.cast::<f32>()),
            F64 => self.cast::<f64>().partial_cmp(other.cast::<f64>()),
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

    pub fn is<T: GetValueKind>(&self) -> bool {
        T::get_value_kind().assert_eq(self.kind()).is_ok()
    }

    pub fn cast<T: FromValue + ?Sized>(&self) -> &T::Output {
        T::from_value(self)
    }

    pub fn kind(&self) -> &ValueKind {
        unsafe {
            std::mem::transmute(&self.0[0])
        }
    }

    pub fn set(&mut self, value: &Self) {
        self.0.clone_from_slice(value.deref());
    }

}

#[derive(Hash, Eq)]
/// Valeur entièrement détenue par l'objet (Owned value)
pub struct ValueBuf(Vec<u8>);
impl ValueBuf {
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
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
impl From<(String, ValueBuilder)> for ValueBuf {
    fn from(kv: (String, ValueBuilder)) -> Self {
        let mut buf: Vec<u8> = vec![KV_PAIR.into()];
        let v = kv.1.into_value_buf();
        let k = kv.0;
        let size: u32 = u32::try_from(k.len() + v.len()).unwrap();
        buf.write_all(&size.to_le_bytes()).unwrap();
        buf.write_all(k.as_bytes()).unwrap();
        buf.write_all(v.as_bytes()).unwrap();
        Self(buf)
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
impl From<f32> for ValueBuf {
    fn from(value: f32) -> Self {
        let mut buf = vec![F32.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<f64> for ValueBuf {
    fn from(value: f64) -> Self {
        let mut buf = vec![F64.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<String> for ValueBuf {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}
impl From<&str> for ValueBuf {
    fn from(value: &str) -> Self {
        let mut buf = vec![STR.into()];
        buf.write_all(value.as_bytes()).unwrap();
        Self(buf)
    }
}

#[derive(Debug)]
pub struct ValU8([u8]);
impl PartialEq<Self> for ValU8 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialEq<u8> for ValU8 {
    fn eq(&self, other: &u8) -> bool {
        self.deref() == other
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

pub struct ValF32([u8]);
impl PartialEq<Self> for ValF32 {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl PartialOrd<Self> for ValF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl PartialEq<f32> for ValF32 {
    fn eq(&self, other: &f32) -> bool {
        self.deref().eq(other)
    }
}
impl PartialOrd<f32> for ValF32 {
    fn partial_cmp(&self, other: &f32) -> Option<std::cmp::Ordering> {
        self.deref().get().partial_cmp(other)
    }
}
impl Deref for ValF32 {
    type Target = zerocopy::F32<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::F32::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &ValF32 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        F32.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct ValF64([u8]);
impl PartialEq<Self> for ValF64 {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl PartialOrd<Self> for ValF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl PartialEq<f64> for ValF64 {
    fn eq(&self, other: &f64) -> bool {
        self.deref().eq(other)
    }
}
impl PartialOrd<f64> for ValF64 {
    fn partial_cmp(&self, other: &f64) -> Option<std::cmp::Ordering> {
        self.deref().get().partial_cmp(other)
    }
}
impl Deref for ValF64 {
    type Target = zerocopy::F64<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::F64::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &ValF64 {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        F64.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

#[derive(Debug)]
pub struct ValStr([u8]);
impl PartialEq<str> for ValStr {
    fn eq(&self, other: &str) -> bool {
        self.deref().eq(other)
    }
}
impl PartialEq<Self> for ValStr {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl std::fmt::Display for ValStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}
impl ValStr {
    pub fn to_owned(&self) -> String {
        self.deref().to_owned()
    }
}
impl Deref for ValStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::str::from_utf8_unchecked(&self.0[1..])
        }
    }
}
impl TryFrom<&Value> for &ValStr {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        STR.assert_eq(value.kind())?;
        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

impl FromValueBuilder for u8 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self {
        if let ValueBuilder::U8(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self {
        if let ValueBuilder::U8(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for u8 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for u8 {
    fn get_value_kind() -> ValueKind {
        U8
    }
}

impl FromValue for u8 {
    type Output = ValU8;
    
    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl FromValueBuilder for u16 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self {
        if let ValueBuilder::U16(val) = value {
            val
        }
        panic!("not an unsigned word")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self {
        if let ValueBuilder::U16(val) = value {
            val
        }
        panic!("not an unsigned word")
    }
}

impl IntoValueBuilder for u16 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for u16 {
    fn get_value_kind() -> ValueKind {
        U16
    }
}

impl FromValue for u16 {
    type Output = ValU16;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl FromValueBuilder for u32 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self {
        if let ValueBuilder::U32(val) = value {
            val
        }
        panic!("not an u32")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self {
        if let ValueBuilder::U32(val) = value {
            val
        }
        panic!("not an u32")
    }
}

impl IntoValueBuilder for u32 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for u32 {
    fn get_value_kind() -> ValueKind {
        U32
    }
}
impl FromValue for u32 {
    type Output = ValU32;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}
impl FromValueBuilder for u64 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self {
        if let ValueBuilder::U64(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self {
        if let ValueBuilder::U64(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for u64 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}
impl GetValueKind for u64 {
    fn get_value_kind() -> ValueKind {
        U64
    }
}
impl FromValue for u64 {
    type Output = ValU64;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl FromValueBuilder for u128 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self {
        if let ValueBuilder::U128(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self {
        if let ValueBuilder::U128(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for u128 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}
impl GetValueKind for u128 {
    fn get_value_kind() -> ValueKind {
        U128
    }
}
impl FromValue for u128 {
    type Output = ValU128;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl FromValueBuilder for i8 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self {
        if let ValueBuilder::I8(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self {
        if let ValueBuilder::I8(val) = value {
            val
        }
        panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for i8 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}
impl GetValueKind for i8 {
    fn get_value_kind() -> ValueKind {
        I8
    }
}
impl FromValue for i8 {
    type Output = ValI8;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for i16 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}
impl GetValueKind for i16 {
    fn get_value_kind() -> ValueKind {
        I16
    }
}
impl FromValue for i16 {
    type Output = ValI16;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for i32 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}
impl GetValueKind for i32 {
    fn get_value_kind() -> ValueKind {
        I32
    }
}
impl FromValue for i32 {
    type Output = ValI32;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for i64 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for i64 {
    fn get_value_kind() -> ValueKind {
        I64
    }
}
impl FromValue for i64 {
    type Output = ValI64;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for i128 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for i128 {
    fn get_value_kind() -> ValueKind {
        I128
    }
}

impl FromValue for i128 {
    type Output = ValI128;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for f32 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for f32 {
    fn get_value_kind() -> ValueKind {
        F32
    }
}

impl FromValue for f32 {
    type Output = ValF32;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for f64 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for f64 {
    fn get_value_kind() -> ValueKind {
        F64
    }
}

impl FromValue for f64 {
    type Output = ValF64;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

impl IntoValueBuilder for &str {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Value(self.into_value_buf())
    }
}

impl GetValueKind for str {
    fn get_value_kind() -> ValueKind {
        STR
    }
}

impl FromValue for str {
    type Output = ValStr;

    fn try_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
}

#[cfg(test)]
mod tests {
    use super::IntoValueBuf;

    #[test]
    fn test_is() {
        assert!(10_u8.into_value_buf().is::<u8>());
        assert!(10_i8.into_value_buf().is::<i8>());
        assert!(10_u16.into_value_buf().is::<u16>());
        assert!(10_i16.into_value_buf().is::<i16>());
        assert!(10_u32.into_value_buf().is::<u32>());
        assert!(10_i32.into_value_buf().is::<i32>());
        assert!(10_u64.into_value_buf().is::<u64>());
        assert!(10_i64.into_value_buf().is::<i64>());
        assert!(10_u128.into_value_buf().is::<u128>());
        assert!(10_i128.into_value_buf().is::<i128>());
    }

    #[test]
    fn test_cast() {
        assert!(10_u8.into_value_buf().cast::<u8>() == &10_u8)
    }

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