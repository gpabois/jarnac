//!
//! Numerics are between 1-12
//! Integers are between 1-10;
//! Unsigned integers are between 1-5;
//! Signed integers are between 6-10;
//! Floats are between 11-12;

pub mod document;
pub mod array;
pub mod builder;
pub mod path;

use std::{borrow::{Borrow, BorrowMut}, fmt::Display, io::Write, ops::{Deref, DerefMut}};

use builder::ValueBuilder;
use zerocopy::{FromBytes, IntoBytes, LittleEndian};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{error::{Error, ErrorKind}, pager::page::PageSlice, result::Result};

const U8_KIND: ValueKind = ValueKind(1);
const U16_KIND: ValueKind = ValueKind(2);
const U32_KIND: ValueKind = ValueKind(3);
const U64_KIND: ValueKind = ValueKind(4);
const U128_KIND: ValueKind = ValueKind(5);
const I8_KIND: ValueKind = ValueKind(6);
const I16_KIND: ValueKind = ValueKind(7);
const I32_KIND: ValueKind = ValueKind(8);
const I64_KIND: ValueKind = ValueKind(9);
const I128_KIND: ValueKind = ValueKind(10);
const F32_KIND: ValueKind = ValueKind(11);
const F64_KIND: ValueKind = ValueKind(12);

const STR_KIND: ValueKind    = ValueKind(13);
const DOCUMENT_KIND: ValueKind = ValueKind(14);
const KV_PAIR_KIND: ValueKind = ValueKind(15);

const ARRAY_KIND_FLAG: u8 = 128;

pub trait IntoValueBuilder {
    fn into_value_builder(self) -> ValueBuilder;
}

pub trait FromValueBuilder {
    type Output: ?Sized;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output;
    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output;
}

pub trait GetValueKind {
    const KIND: ValueKind;
}

pub trait FromValue: GetValueKind {
    type Output: ?Sized;

    fn ref_from_value(value: &Value) -> &Self::Output {
        Self::try_ref_from_value(value).expect("wrong value type")
    }

    fn mut_from_value(value: &mut Value) -> &mut Self::Output {
        Self::try_mut_from_value(value).expect("wrong value type")
    }

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output>;
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output>;
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
            U8_KIND => f.write_str("u8"),
            U16_KIND => f.write_str("u16"),
            U32_KIND => f.write_str("u32"),
            U64_KIND => f.write_str("u64"),
            U128_KIND => f.write_str("u128"),
            I8_KIND => f.write_str("i8"),
            I16_KIND => f.write_str("i16"),
            I32_KIND => f.write_str("i32"),
            I64_KIND => f.write_str("i64"),
            I128_KIND => f.write_str("i128"),
            F32_KIND => f.write_str("f32"),
            F64_KIND => f.write_str("f64"),
            STR_KIND => f.write_str("str"),
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
    
    pub fn assert_eq(&self, other: &ValueKind) -> Result<()> {
        if *other != *self {
            return Err(Error::new(ErrorKind::WrongValueKind { expected: U8_KIND, got: *other }))
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
            U8_KIND | I8_KIND => Some(1),
            U16_KIND | I16_KIND => Some(2),
            U32_KIND | I32_KIND | F32_KIND => Some(4),
            U64_KIND | I64_KIND | F64_KIND => Some(8),
            U128_KIND | I128_KIND => Some(16),
            _ => None
        }
    }
}

pub struct Value([u8]);
impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self.kind() {
            U8_KIND => self.cast::<u8>().fmt(f),
            U16_KIND => self.cast::<u16>().fmt(f),
            U32_KIND => self.cast::<u32>().fmt(f),
            U64_KIND => self.cast::<u64>().fmt(f),
            U128_KIND => self.cast::<u128>().fmt(f),
            I8_KIND => self.cast::<i8>().fmt(f),
            I16_KIND => self.cast::<i16>().fmt(f),
            I32_KIND => self.cast::<i32>().fmt(f),
            I64_KIND => self.cast::<i64>().fmt(f),
            I128_KIND => self.cast::<i128>().fmt(f),
            F32_KIND => self.cast::<f32>().fmt(f),
            F64_KIND => self.cast::<f64>().fmt(f),
            STR_KIND => self.cast::<str>().fmt(f),
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
            U8_KIND => self.cast::<u8>().eq(other.cast::<u8>()),
            U16_KIND => self.cast::<u16>().eq(other.cast::<u16>()),
            U32_KIND => self.cast::<u32>().eq(other.cast::<u32>()),
            U64_KIND => self.cast::<u64>().eq(other.cast::<u64>()),
            U128_KIND => self.cast::<u128>().eq(other.cast::<u128>()),
            I8_KIND => self.cast::<i8>().eq(other.cast::<i8>()),
            I16_KIND => self.cast::<i16>().eq(other.cast::<i16>()),
            I32_KIND => self.cast::<i32>().eq(other.cast::<i32>()),
            I64_KIND => self.cast::<i64>().eq(other.cast::<i64>()),
            I128_KIND => self.cast::<i128>().eq(other.cast::<i128>()),
            F32_KIND => self.cast::<f32>().eq(other.cast::<f32>()),
            F64_KIND => self.cast::<f64>().eq(other.cast::<f64>()),
            STR_KIND => self.cast::<str>().eq(other.cast::<str>()),
            _ => false
        }
    }
}
impl PartialOrd<Self> for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.kind() != other.kind() { return None }

        match *self.kind() {
            U8_KIND => self.cast::<u8>().partial_cmp(other.cast::<u8>()),
            U16_KIND => self.cast::<u16>().partial_cmp(other.cast::<u16>()),
            U32_KIND => self.cast::<u32>().partial_cmp(other.cast::<u32>()),
            U64_KIND => self.cast::<u64>().partial_cmp(other.cast::<u64>()),
            U128_KIND => self.cast::<u128>().partial_cmp(other.cast::<u128>()),
            I8_KIND => self.cast::<i8>().partial_cmp(other.cast::<i8>()),
            I16_KIND => self.cast::<i16>().partial_cmp(other.cast::<i16>()),
            I32_KIND => self.cast::<i32>().partial_cmp(other.cast::<i32>()),
            I64_KIND => self.cast::<i64>().partial_cmp(other.cast::<i64>()),
            I128_KIND => self.cast::<i128>().partial_cmp(other.cast::<i128>()),
            F32_KIND => self.cast::<f32>().partial_cmp(other.cast::<f32>()),
            F64_KIND => self.cast::<f64>().partial_cmp(other.cast::<f64>()),
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

    pub fn is<T: GetValueKind + ?Sized>(&self) -> bool {
        T::KIND.assert_eq(self.kind()).is_ok()
    }

    pub fn cast<T: FromValue + ?Sized>(&self) -> &T::Output {
        T::ref_from_value(self)
    }

    pub fn cast_mut<T: FromValue + ?Sized>(&mut self) -> &mut T::Output {
        T::mut_from_value(self)
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
impl std::fmt::Display for ValueBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}
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
impl DerefMut for ValueBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.borrow_mut()
    }
}
impl ToOwned for Value {
    type Owned = ValueBuf;

    fn to_owned(&self) -> Self::Owned {
        ValueBuf(self.0.to_owned())
    }
}
impl BorrowMut<Value> for ValueBuf {
    fn borrow_mut(&mut self) -> &mut Value {
        unsafe {
            std::mem::transmute(self.0.as_mut_slice())
        }
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
        let mut buf: Vec<u8> = vec![KV_PAIR_KIND.into()];
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
        Self(vec![U8_KIND.into(), value])
    }
}
impl From<&[u8]> for ValueBuf {
    fn from(value: &[u8]) -> Self {
        let kind = U8_KIND.as_array();
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
        let mut buf = vec![U16_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<&[u16]> for ValueBuf {
    fn from(value: &[u16]) -> Self {
        let kind = U16_KIND.as_array();
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
        let mut buf = vec![U32_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<&[u32]> for ValueBuf {
    fn from(value: &[u32]) -> Self {
        let kind = U16_KIND.as_array();
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
        let mut buf = vec![U64_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<&[u64]> for ValueBuf {
    fn from(value: &[u64]) -> Self {
        let kind = U16_KIND.as_array();
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
        let mut buf = vec![U128_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<&[u128]> for ValueBuf {
    fn from(value: &[u128]) -> Self {
        let kind = U16_KIND.as_array();
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
        Self(vec![I8_KIND.into(), unsafe {std::mem::transmute(value)}])
    }
}
impl From<i16> for ValueBuf {
    fn from(value: i16) -> Self {
        let mut buf = vec![I16_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i32> for ValueBuf {
    fn from(value: i32) -> Self {
        let mut buf = vec![I32_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i64> for ValueBuf {
    fn from(value: i64) -> Self {
        let mut buf = vec![I64_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<i128> for ValueBuf {
    fn from(value: i128) -> Self {
        let mut buf = vec![I128_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<f32> for ValueBuf {
    fn from(value: f32) -> Self {
        let mut buf = vec![F32_KIND.into()];
        buf.write_all(&value.to_le_bytes()).unwrap();
        Self(buf)
    }
}
impl From<f64> for ValueBuf {
    fn from(value: f64) -> Self {
        let mut buf = vec![F64_KIND.into()];
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
        let mut buf = vec![STR_KIND.into()];
        buf.write_all(value.as_bytes()).unwrap();
        Self(buf)
    }
}

#[derive(Debug)]
pub struct U8([u8]);
impl U8 {
    pub fn set(&mut self, value: u8) {
        *self.deref_mut() = value
    }
}
impl PartialEq<Self> for U8 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for U8 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl PartialEq<u8> for U8 {
    fn eq(&self, other: &u8) -> bool {
        self.deref() == other
    }
}
impl PartialOrd<u8> for U8 {
    fn partial_cmp(&self, other: &u8) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other)
    }
}
impl Deref for U8 {
    type Target = u8;

    fn deref(&self) -> &Self::Target {
        &self.0[1]
    }
}
impl DerefMut for U8 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0[1]
    }
}
impl TryFrom<&Value> for &U8 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        U8_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut U8 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        U8_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}


pub struct U16([u8]);
impl U16 {
    pub fn set(&mut self, value: u16) {
        self.deref_mut().set(value)
    }
}
impl PartialEq<Self> for U16 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for U16 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for U16 {
    type Target = zerocopy::U16<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U16::<LittleEndian>::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for U16 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::U16::<LittleEndian>::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &U16 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        U16_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut U16 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        U16_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct U32([u8]);
impl U32 {
    pub fn to_owned(&self) -> u32 {
        self.into()
    }

    pub fn set(&mut self, value: u32) {
        self.deref_mut().set(value)
    }
}
impl Into<u32> for &U32 {
    fn into(self) -> u32 {
        self.deref().get()
    }
}
impl PartialEq<Self> for U32 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for U32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for U32 {
    type Target = zerocopy::U32<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U32::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for U32 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::U32::<LittleEndian>::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &U32 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        U32_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut U32 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        U32_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct U64([u8]);
impl std::fmt::Debug for U64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{0}", self.deref())
    }
}
impl U64 {
    pub fn to_owned(&self) -> u64 {
        self.into()
    }

    pub fn set(&mut self, value: u64) {
        self.deref_mut().set(value)
    }
}
impl Into<u64> for &U64 {
    fn into(self) -> u64 {
        self.deref().get()
    }
}
impl PartialEq<Self> for U64 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for U64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl PartialEq<u64> for U64 {
    fn eq(&self, other: &u64) -> bool {
        self.deref().eq(other)
    }
}
impl Deref for U64 {
    type Target = zerocopy::U64<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U64::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for U64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::U64::<LittleEndian>::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &U64 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        U64_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut U64 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        U64_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}


pub struct U128([u8]);
impl U128 {
    pub fn to_owned(&self) -> u128 {
        self.into()
    }

    pub fn set(&mut self, value: u128) {
        self.deref_mut().set(value)
    }
}
impl Into<u128> for &U128 {
    fn into(self) -> u128 {
        self.deref().get()
    }
}
impl PartialEq<Self> for U128 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for U128 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for U128 {
    type Target = zerocopy::U128<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::U128::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for U128 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::U128::<LittleEndian>::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &U128 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        U128_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut U128 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        U128_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct I8([u8]);
impl I8 {
    pub fn to_owned(&self) -> i8 {
        self.into()
    }

    pub fn set(&mut self, value: i8) {
        *self.deref_mut() = value
    }
}
impl Into<i8> for &I8 {
    fn into(self) -> i8 {
        *self.deref()
    }
}
impl PartialEq<Self> for I8 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for I8 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for I8 {
    type Target = i8;

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::mem::transmute(&self.0[1])
        }
    }
}
impl DerefMut for I8 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            std::mem::transmute(&mut self.0[1])
        }
    }
}
impl TryFrom<&Value> for &I8 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        I8_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut I8 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        I8_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct I16([u8]);
impl I16 {
    pub fn to_owned(&self) -> i16 {
        self.into()
    }

    pub fn set(&mut self, value: i16) {
        self.deref_mut().set(value)
    }
}
impl Into<i16> for &I16 {
    fn into(self) -> i16 {
        self.deref().get()
    }
}
impl PartialEq<Self> for I16 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for I16 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for I16 {
    type Target = zerocopy::I16<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I16::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for I16 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::I16::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &I16 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        I16_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut I16 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        I16_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct I32([u8]);
impl I32 {
    pub fn to_owned(&self) -> i32 {
        self.into()
    }

    pub fn set(&mut self, value: i32) {
        self.deref_mut().set(value)
    }
}
impl Into<i32> for &I32 {
    fn into(self) -> i32 {
        self.deref().get()
    }
}
impl PartialEq<Self> for I32 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for I32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for I32 {
    type Target = zerocopy::I32<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I32::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for I32 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::I32::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &I32 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        I32_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut I32 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        I32_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct I64([u8]);
impl I64 {
    pub fn to_owned(&self) -> i64 {
        self.into()
    }

    pub fn set(&mut self, value: i64) {
        self.deref_mut().set(value)
    }
}
impl Into<i64> for &I64 {
    fn into(self) -> i64 {
        self.deref().get()
    }
}
impl PartialEq<Self> for I64 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for I64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for I64 {
    type Target = zerocopy::I64<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I64::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for I64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::I64::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &I64 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        I64_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut I64 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        I64_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct I128([u8]);
impl I128 {
    pub fn to_owned(&self) -> i128 {
        self.into()
    }

    pub fn set(&mut self, value: i128) {
        self.deref_mut().set(value)
    }
}
impl Into<i128> for &I128 {
    fn into(self) -> i128 {
        self.deref().get()
    }
}
impl PartialEq<Self> for I128 {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl PartialOrd<Self> for I128 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl Deref for I128 {
    type Target = zerocopy::I128<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::I128::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for I128 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::I128::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &I128 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        I128_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut I128 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        I128_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct F32([u8]);
impl F32 {
    pub fn to_owned(&self) -> f32 {
        self.into()
    }

    pub fn set(&mut self, value: f32) {
        self.deref_mut().set(value)
    }
}
impl Into<f32> for &F32 {
    fn into(self) -> f32 {
        self.deref().get()
    }
}
impl PartialEq<Self> for F32 {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl PartialOrd<Self> for F32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl PartialEq<f32> for F32 {
    fn eq(&self, other: &f32) -> bool {
        self.deref().eq(other)
    }
}
impl PartialOrd<f32> for F32 {
    fn partial_cmp(&self, other: &f32) -> Option<std::cmp::Ordering> {
        self.deref().get().partial_cmp(other)
    }
}
impl Deref for F32 {
    type Target = zerocopy::F32<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::F32::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for F32 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::F32::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &F32 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        F32_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut F32 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        F32_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct F64([u8]);
impl F64 {
    pub fn to_owned(&self) -> f64 {
        self.into()
    }

    pub fn set(&mut self, value: f64) {
        self.deref_mut().set(value)
    }
}
impl Into<f64> for &F64 {
    fn into(self) -> f64 {
        self.deref().get()
    }
}
impl PartialEq<Self> for F64 {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl PartialOrd<Self> for F64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.deref().partial_cmp(other.deref())
    }
}
impl PartialEq<f64> for F64 {
    fn eq(&self, other: &f64) -> bool {
        self.deref().eq(other)
    }
}
impl PartialOrd<f64> for F64 {
    fn partial_cmp(&self, other: &f64) -> Option<std::cmp::Ordering> {
        self.deref().get().partial_cmp(other)
    }
}
impl Deref for F64 {
    type Target = zerocopy::F64<LittleEndian>;

    fn deref(&self) -> &Self::Target {
        zerocopy::F64::ref_from_bytes(&self.0[1..]).unwrap()
    }
}
impl DerefMut for F64 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        zerocopy::F64::mut_from_bytes(&mut self.0[1..]).unwrap()
    }
}
impl TryFrom<&Value> for &F64 {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        F64_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut F64 {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        F64_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

#[derive(Debug)]
pub struct Str([u8]);
impl Str {
    pub fn to_owned(&self) -> String {
        self.deref().to_owned()
    }
}
impl PartialEq<str> for Str {
    fn eq(&self, other: &str) -> bool {
        self.deref().eq(other)
    }
}
impl PartialEq<Self> for Str {
    fn eq(&self, other: &Self) -> bool {
        self.deref().eq(other.deref())
    }
}
impl std::fmt::Display for Str {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}
impl Deref for Str {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::str::from_utf8_unchecked(&self.0[1..])
        }
    }
}
impl TryFrom<&Value> for &Str {
    type Error = Error;

    fn try_from(value: &Value) -> std::result::Result<Self, Self::Error> {
        STR_KIND.assert_eq(value.kind())?;
        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut Str {
    type Error = Error;

    fn try_from(value: &mut Value) -> std::result::Result<Self, Self::Error> {
        STR_KIND.assert_eq(value.kind())?;
        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}


impl FromValueBuilder for u8 {
    type Output = Self;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for u8 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for u8 {
    const KIND: ValueKind = U8_KIND;
}

impl FromValue for u8 {
    type Output = U8;
    
    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for u16 {
    type Output = U16;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for u16 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for u16 {
    const KIND: ValueKind = U16_KIND;
}

impl FromValue for u16 {
    type Output = U16;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for u32 {
    type Output = U32;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for u32 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for u32 {
    const KIND: ValueKind = U32_KIND;
}
impl FromValue for u32 {
    type Output = U32;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}
impl FromValueBuilder for u64 {
    type Output = U64;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for u64 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}
impl GetValueKind for u64 {
    const KIND: ValueKind = U64_KIND;
}
impl FromValue for u64 {
    type Output = U64;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for u128 {
    type Output = U128;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for u128 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}
impl GetValueKind for u128 {
    const KIND: ValueKind = U128_KIND;
}
impl FromValue for u128 {
    type Output = U128;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for i8 {
    type Output = I8;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for i8 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}
impl GetValueKind for i8 {
    const KIND: ValueKind = I8_KIND;
}
impl FromValue for i8 {
    type Output = I8;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}
impl FromValueBuilder for i16 {
    type Output = I16;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for i16 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}
impl GetValueKind for i16 {
    const KIND: ValueKind = I16_KIND;
}
impl FromValue for i16 {
    type Output = I16;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}
impl FromValueBuilder for i32 {
    type Output = I32;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for i32 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}
impl GetValueKind for i32 {
    const KIND: ValueKind = I32_KIND;
}
impl FromValue for i32 {
    type Output = I32;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}
impl FromValueBuilder for i64 {
    type Output = I64;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for i64 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for i64 {
    const KIND: ValueKind = I64_KIND;
}
impl FromValue for i64 {
    type Output = I64;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl IntoValueBuilder for i128 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}
impl FromValueBuilder for i128 {
    type Output = I128;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl GetValueKind for i128 {
    const KIND: ValueKind = I128_KIND;
}

impl FromValue for i128 {
    type Output = I128;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for f32 {
    type Output = F32;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}
impl IntoValueBuilder for f32 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for f32 {
    const KIND: ValueKind = F32_KIND;
}

impl FromValue for f32 {
    type Output = F32;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for f64 {
    type Output = F64;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for f64 {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for f64 {
    const KIND: ValueKind = F64_KIND;
}


impl FromValue for f64 {
    type Output = F64;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromValueBuilder for str {
    type Output = Str;

    fn borrow_value(value: &ValueBuilder) -> &Self::Output {
        if let ValueBuilder::Other(val) = value {
           return val.cast::<Self>()
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut ValueBuilder) -> &mut Self::Output {
        if let ValueBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>()
         }
         panic!("not an unsigned byte")
    }
}

impl IntoValueBuilder for &str {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Other(self.into_value_buf())
    }
}

impl GetValueKind for str {
    const KIND: ValueKind = STR_KIND;
}

impl FromValue for str {
    type Output = Str;

    fn try_ref_from_value(value: &Value) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> Result<&mut Self::Output> {
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