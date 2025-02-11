use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
enum NumericRefInner<'data> {
    Uint8(&'data Uint8),
    Uint16(&'data Uint16),
    Uint32(&'data Uint32),
    Uint64(&'data Uint64),
    Int8(&'data Int8),
    Int16(&'data Int16),
    Int32(&'data Int32),
    Int64(&'data Int64),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct NumericRef<'data>(NumericRefInner<'data>);

pub trait IntoNumericSpec {
    fn into_numeric_spec() -> NumericSpec;
}

#[derive(FromBytes, Clone, Copy, KnownLayout, Immutable)]
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

    /// Récupère une référence vers une valeur numérique sans réaliser de copie.
    pub fn into_ref<'data>(&self, data: &'data [u8]) -> NumericRef<'data> {
        NumericRef(match (self.size(), self.is_signed()) {
            (1, false) => NumericRefInner::Uint8(Uint8::ref_from_bytes(data).unwrap()),
            (2, false) => NumericRefInner::Uint16(Uint16::ref_from_bytes(data).unwrap()),
            (3, false) => NumericRefInner::Uint32(Uint32::ref_from_bytes(data).unwrap()),
            (4, false) => NumericRefInner::Uint64(Uint64::ref_from_bytes(data).unwrap()),
            (1, true) => NumericRefInner::Int8(Int8::ref_from_bytes(data).unwrap()),
            (2, true) => NumericRefInner::Int16(Int16::ref_from_bytes(data).unwrap()),
            (3, true) => NumericRefInner::Int32(Int32::ref_from_bytes(data).unwrap()),
            (4, true) => NumericRefInner::Int64(Int64::ref_from_bytes(data).unwrap()),
            _ => unreachable!(),
        })
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Uint8(u8);

impl IntoNumericSpec for Uint8 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(1, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Uint16(u16);

impl IntoNumericSpec for Uint16 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(2, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Uint32(u32);

impl IntoNumericSpec for Uint32 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(3, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Uint64(u64);

impl IntoNumericSpec for Uint64 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(4, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int8(i8);

impl IntoNumericSpec for Int8 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(1, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int16(i16);

impl IntoNumericSpec for Int16 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(2, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int32(i32);

impl IntoNumericSpec for Int32 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(3, false)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, PartialEq, Eq, PartialOrd, Ord)]
pub struct Int64(i64);

impl IntoNumericSpec for Int64 {
    fn into_numeric_spec() -> NumericSpec {
        NumericSpec::new(4, false)
    }
}

