//!
//! Numerics are between 1-12
//! Integers are between 1-10;
//! Unsigned integers are between 1-5;
//! Signed integers are between 6-10;
//! Floats are between 11-12;

pub mod array;
pub mod buf;
pub mod builder;
pub mod document;
pub mod error;
pub mod kind;
pub mod marker;
pub mod ord;
pub mod path;
pub mod prelude;
pub mod result;

use std::{convert::Infallible, ops::{Deref, DerefMut, Range}};

use array::Array;
use buf::KnackBuf;
use builder::KnackBuilder;
use document::Document;
use error::KnackError;
use kind::{GetKnackKind, KnackKind};
use marker::{kernel::AsKernelRef, Comparable, ComparableAndFixedSized, FixedSized};
use path::IntoKnackPath;
use result::KnackResult;
use zerocopy::{FromBytes, LittleEndian};

use crate::page::{AsRefPageSlice, PageSlice};

pub type KnackTypeId = u8;
pub type KnackSize = u16;

pub trait FromKnack: GetKnackKind {
    type Output: ?std::marker::Sized;

    fn ref_from_knack(value: &Knack) -> &Self::Output {
        Self::try_ref_from_knack(value).expect("wrong value type")
    }
    fn mut_from_knack(value: &mut Knack) -> &mut Self::Output {
        Self::try_mut_from_knack(value).expect("wrong value type")
    }

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output>;
    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output>;
}

/// Valeur détenue dans une tranche de page.
pub struct KnackCell<Slice>(Slice)
where
    Slice: AsRefPageSlice;

impl<Slice> From<Slice> for KnackCell<Slice>
where
    Slice: AsRefPageSlice,
{
    fn from(value: Slice) -> Self {
        Self(value)
    }
}

impl<Slice> Deref for KnackCell<Slice>
where
    Slice: AsRefPageSlice,
{
    type Target = Knack;

    fn deref(&self) -> &Self::Target {
        <&Knack>::from(self.0.as_ref())
    }
}

pub enum MaybeOwnedKnack<Slice>
where
    Slice: AsRefPageSlice,
{
    Borrow(KnackCell<Slice>),
    Owned(KnackBuf),
}

impl<Slice> Deref for MaybeOwnedKnack<Slice>
where
    Slice: AsRefPageSlice,
{
    type Target = Knack;

    fn deref(&self) -> &Self::Target {
        match self {
            MaybeOwnedKnack::Borrow(knack_cell) => knack_cell.deref(),
            MaybeOwnedKnack::Owned(knack_buf) => knack_buf.deref(),
        }
    }
}

pub struct Knack([u8]);

impl std::fmt::Display for Knack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{0}({1:?})", self.kind(), self.as_value_bytes())
    }
}

impl From<&PageSlice> for &Knack {
    fn from(value: &PageSlice) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl From<&[u8]> for &Knack {
    fn from(value: &[u8]) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl AsRef<[u8]> for Knack {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl ComparableAndFixedSized<Knack> {
    pub unsafe fn from_ref_unchecked(value: &Knack) -> &ComparableAndFixedSized<Knack> {
        std::mem::transmute(value)
    }
}

impl TryFrom<&Knack> for &ComparableAndFixedSized<Knack> {
    type Error = Infallible;

    fn try_from(value: &Knack) -> Result<Self, Self::Error> {
        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

impl Deref for ComparableAndFixedSized<Knack> {
    type Target = Knack;

    fn deref(&self) -> &Self::Target {
        self.as_kernel_ref()
    }
}

impl Knack {
    pub(crate) fn from_ref(bytes: &[u8]) -> &Self {
        unsafe { std::mem::transmute(bytes) }
    }

    #[allow(dead_code)]
    pub(crate) fn from_mut(bytes: &mut [u8]) -> &mut Self {
        unsafe { std::mem::transmute(bytes) }
    }

    pub fn is<T>(&self) -> bool
    where
        T: GetKnackKind + ?std::marker::Sized,
    {
        T::kind().as_kernel_ref().assert_same(self.kind()).is_ok()
    }

    pub fn cast<T: FromKnack + ?std::marker::Sized>(&self) -> &T::Output {
        T::ref_from_knack(self)
    }

    pub fn cast_mut<T: FromKnack + ?std::marker::Sized>(&mut self) -> &mut T::Output {
        T::mut_from_knack(self)
    }

    pub fn kind(&self) -> &KnackKind {
        <&KnackKind>::try_from(&self.0).unwrap()
    }

    pub fn get<Path: IntoKnackPath>(&self, path: Path) -> Option<&Knack> {
        let mut pth = path.into_value_path();

        match pth.pop() {
            None => Some(self),
            Some(attr_name) => {
                if self.is::<Document>() {
                    self.cast::<Document>()
                        .get_field(&attr_name)
                        .and_then(|v| v.get(pth))
                } else if self.is::<Array>() {
                    let index = attr_name.parse::<usize>().ok()?;
                    self.cast::<Array>().get(&index).and_then(|v| v.get(pth))
                } else {
                    None
                }
            }
        }
    }

    pub fn set(&mut self, value: &Self) {
        self.0.clone_from_slice(value.as_ref());
    }

    pub fn try_as_comparable(&self) -> Option<&Comparable<Self>> {
        self.kind()
            .try_as_comparable()
            .map(|_| unsafe { std::mem::transmute(self) })
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }

    pub fn as_value_bytes(&self) -> &[u8] {
        let offset = self.kind().len();
        &self.0[offset..]
    }
}

impl FixedSized<Knack> {
    unsafe fn from_ref_unchecked(value: &Knack) -> &FixedSized<Knack> {
        std::mem::transmute(value)
    }

    pub fn range(&self) -> Range<usize> {
        self.0.kind()
            .try_as_fixed_sized()
            .unwrap()
            .range()
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
impl TryFrom<&Knack> for &U8 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        u8::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut U8 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        u8::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &U16 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        u16::kind().as_kernel_ref().assert_same(value.kind())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut U16 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        u16::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &U32 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        u32::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut U32 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        u32::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &U64 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        u64::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut U64 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        u64::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &U128 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        u128::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut U128 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        u128::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
        unsafe { std::mem::transmute(&self.0[1]) }
    }
}
impl DerefMut for I8 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::mem::transmute(&mut self.0[1]) }
    }
}
impl TryFrom<&Knack> for &I8 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        i8::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut I8 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        i8::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &I16 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        i16::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut I16 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        i16::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &I32 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        i32::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut I32 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        i32::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &I64 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        i64::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut I64 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        i64::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &I128 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        i128::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut I128 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        i128::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &F32 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        f32::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut F32 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        f32::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
impl TryFrom<&Knack> for &F64 {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        f64::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut F64 {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        f64::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
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
        unsafe { std::str::from_utf8_unchecked(&self.0[1..]) }
    }
}
impl TryFrom<&Knack> for &Str {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        str::kind().as_kernel_ref().assert_same(value.kind().as_kernel_ref())?;
        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut Str {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        str::kind().as_kernel_ref().assert_same(value.kind().as_kernel_ref())?;
        unsafe { Ok(std::mem::transmute(value)) }
    }
}

impl FromKnack for u8 {
    type Output = U8;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for u16 {
    type Output = U16;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for u32 {
    type Output = U32;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for u64 {
    type Output = U64;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for u128 {
    type Output = U128;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for i8 {
    type Output = I8;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for i16 {
    type Output = I16;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for i32 {
    type Output = I32;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for i64 {
    type Output = I64;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for i128 {
    type Output = I128;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for f32 {
    type Output = F32;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for f64 {
    type Output = F64;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}
impl FromKnack for str {
    type Output = Str;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::IntoKnackBuf;

    #[test]
    fn test_is() {
        assert!(10_u8.into_knack_buf().is::<u8>());
        assert!(10_i8.into_knack_buf().is::<i8>());
        assert!(10_u16.into_knack_buf().is::<u16>());
        assert!(10_i16.into_knack_buf().is::<i16>());
        assert!(10_u32.into_knack_buf().is::<u32>());
        assert!(10_i32.into_knack_buf().is::<i32>());
        assert!(10_u64.into_knack_buf().is::<u64>());
        assert!(10_i64.into_knack_buf().is::<i64>());
        assert!(10_u128.into_knack_buf().is::<u128>());
        assert!(10_i128.into_knack_buf().is::<i128>());
    }

    #[test]
    fn test_cast() {
        assert!(10_u8.into_knack_buf().cast::<u8>() == &10_u8)
    }

    #[test]
    fn test_sizes() {
        assert_eq!(
            10u8.into_knack_buf().kind().as_sized().inner_size(),
            Some(1),
            "unsigned int8 must have a size of 1"
        );
        assert_eq!(
            10i8.into_knack_buf().kind().as_sized().inner_size(),
            Some(1),
            "signed int8 must have a size of 1"
        );
        assert_eq!(
            10u16.into_knack_buf().kind().as_sized().inner_size(),
            Some(2),
            "unsigned int16 must have a size of 2"
        );
        assert_eq!(
            10i16.into_knack_buf().kind().as_sized().inner_size(),
            Some(2),
            "signed int16 must have a size of 2"
        );
        assert_eq!(
            10u32.into_knack_buf().kind().as_sized().inner_size(),
            Some(4),
            "unsigned int32 must have a size of 4"
        );
        assert_eq!(
            10i32.into_knack_buf().kind().as_sized().inner_size(),
            Some(4),
            "signed int32 must have a size of 4"
        );
        assert_eq!(
            10u64.into_knack_buf().kind().as_sized().inner_size(),
            Some(8),
            "unsigned int64 must have a size of 8"
        );
        assert_eq!(
            10i64.into_knack_buf().kind().as_sized().inner_size(),
            Some(8),
            "signed int64 must have a size of 8"
        );
        assert_eq!(
            10u128.into_knack_buf().kind().as_sized().inner_size(),
            Some(16),
            "unsigned int128 must have a size of 16"
        );
        assert_eq!(
            10i128.into_knack_buf().kind().as_sized().inner_size(),
            Some(16),
            "signed int128 must have a size of 16"
        );
    }
}

