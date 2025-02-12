use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
enum NumericInner {
    Uint8(Uint8),
    Uint16(Uint16),
    Uint32(Uint32),
    Uint64(Uint64),
    Int8(Int8),
    Int16(Int16),
    Int32(Int32),
    Int64(Int64),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Numeric(NumericInner);

impl Numeric {
    pub fn into_numeric_spec(&self) -> NumericSpec {
        match self.0 {
            NumericInner::Uint8(_) => Uint8::into_numeric_spec(),
            NumericInner::Uint16(_) => Uint16::into_numeric_spec(),
            NumericInner::Uint32(_) => Uint32::into_numeric_spec(),
            NumericInner::Uint64(_) => Uint64::into_numeric_spec(),
            NumericInner::Int8(_) => Int8::into_numeric_spec(),
            NumericInner::Int16(_) => Int16::into_numeric_spec(),
            NumericInner::Int32(_) => Int32::into_numeric_spec(),
            NumericInner::Int64(_) => Int64::into_numeric_spec(),
        }
    }
}

pub trait IntoNumericSpec {
    fn into_numeric_spec() -> NumericSpec;
}

#[derive(FromBytes, Clone, Copy, KnownLayout, Immutable, Eq, PartialEq, Debug)]
pub struct NumericSpec(u8);

impl NumericSpec {
    pub fn from<NS: IntoNumericSpec>() -> Self {
        NS::into_numeric_spec()
    }

    fn new(size: u8, signed: bool) -> Self {
        let signed: u8 = signed.into();
        Self(size & 0b111 | (signed * 128))
    }

    pub fn is_signed(&self) -> bool {
        self.0 & 128 == 128
    }

    pub fn size(&self) -> u8 {
        self.0 & 0b111
    }

    pub fn get_byte_slice<'data>(&self, data: &'data [u8]) -> &'data [u8] {
        &data[0..usize::from(self.size())]
    }

    /// Récupère une référence vers une valeur numérique sans réaliser de copie.
    pub fn from_byte_slice<'data>(&self, data: &[u8]) -> Numeric {
        Numeric(match (self.size(), self.is_signed()) {
            (1, false) => NumericInner::Uint8(*Uint8::ref_from_bytes(data).unwrap()),
            (2, false) => NumericInner::Uint16(*Uint16::ref_from_bytes(data).unwrap()),
            (3, false) => NumericInner::Uint32(*Uint32::ref_from_bytes(data).unwrap()),
            (4, false) => NumericInner::Uint64(*Uint64::ref_from_bytes(data).unwrap()),
            (1, true) => NumericInner::Int8(*Int8::ref_from_bytes(data).unwrap()),
            (2, true) => NumericInner::Int16(*Int16::ref_from_bytes(data).unwrap()),
            (3, true) => NumericInner::Int32(*Int32::ref_from_bytes(data).unwrap()),
            (4, true) => NumericInner::Int64(*Int64::ref_from_bytes(data).unwrap()),
            _ => unreachable!(),
        })
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Uint8(u8);

impl IntoNumericSpec for Uint8 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(1, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Uint16(u16);

impl IntoNumericSpec for Uint16 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(2, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Uint32(u32);

impl IntoNumericSpec for Uint32 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(3, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Uint64(u64);

impl IntoNumericSpec for Uint64 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(4, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Int8(i8);

impl IntoNumericSpec for Int8 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(1, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Int16(i16);

impl IntoNumericSpec for Int16 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(2, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Int32(i32);

impl IntoNumericSpec for Int32 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(3, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Int64(i64);

impl IntoNumericSpec for Int64 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(4, false)
    }
}

