use byteorder::{BigEndian, WriteBytesExt};
use zerocopy::{byteorder::{BigEndian as ZBigEndian, U16, U32}, FromBytes, I16, I32, I64, U64};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(PartialEq, Eq, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
#[repr(C, packed)]
pub struct Numeric([u8;17]);

impl PartialOrd<Numeric> for Numeric {
    fn partial_cmp(&self, other: &Numeric) -> Option<std::cmp::Ordering> {
        if self.0[0] != other.0[0] {
            return None
        }

        match *self.kind() {
            UINT8 => self.as_u8().partial_cmp(other.as_u8()),
            UINT16 => self.as_u16().partial_cmp(other.as_u16()),
            UINT32 => self.as_u32().partial_cmp(other.as_u32()),
            UINT64 => self.as_u64().partial_cmp(other.as_u64()),
            INT8 => self.as_i8().partial_cmp(other.as_i8()),
            INT16 => self.as_i16().partial_cmp(other.as_i16()),
            INT32 => self.as_i32().partial_cmp(other.as_i32()),
            INT64 => self.as_i64().partial_cmp(other.as_i64()),
            _ => unreachable!("unknown numeric kind")
        }
    }
}

impl Numeric {
    pub fn as_u8(&self) -> &u8 {
        &self.borrow_numeric()[0]
    }

    pub fn as_u16(&self) -> &U16<ZBigEndian> {
        U16::<ZBigEndian>::ref_from_bytes(self.borrow_numeric()).unwrap()
    }

    pub fn as_u32(&self) -> &U32<ZBigEndian> {
        U32::<ZBigEndian>::ref_from_bytes(self.borrow_numeric()).unwrap()
    }

    pub fn as_u64(&self) -> &U64<ZBigEndian> {
        U64::<ZBigEndian>::ref_from_bytes(self.borrow_numeric()).unwrap()
    }

    pub fn as_i8(&self) -> &i8 {
        unsafe {
            std::mem::transmute(&self.borrow_numeric()[0])
        }
    }

    pub fn as_i16(&self) -> &I16<ZBigEndian> {
        I16::<ZBigEndian>::ref_from_bytes(self.borrow_numeric()).unwrap()
    }

    pub fn as_i32(&self) -> &I32<ZBigEndian> {
        I32::<ZBigEndian>::ref_from_bytes(self.borrow_numeric()).unwrap()
    }

    pub fn as_i64(&self) -> &I64<ZBigEndian> {
        I64::<ZBigEndian>::ref_from_bytes(self.borrow_numeric()).unwrap()
    }

    pub fn borrow_numeric(&self) -> &[u8] {
        self.kind().get_byte_slice(&self.0[1..])
    }

    pub fn borrow_mut_numeric(&mut self) -> &mut [u8] {
        self.kind().clone().get_mut_byte_slice(&mut self.0[1..])
    }
}

impl From<u8> for Numeric {
    fn from(value: u8) -> Self {
        let mut num = Numeric::default();
        num.0[0] = UINT8.into();
        (&mut num.0[1..=1]).write_u8(value).unwrap();
        num
    }
}

impl From<u16> for Numeric {
    fn from(value: u16) -> Self {
        let mut num = Numeric::default();
        num.0[0] = UINT16.into();
        (&mut num.0[1..=3]).write_u16::<BigEndian>(value).unwrap();
        num        
    }
}

impl From<u32> for Numeric {
    fn from(value: u32) -> Self {
        let mut num = Numeric::default();
        num.0[0] = UINT32.into();
        (&mut num.0[1..=5]).write_u32::<BigEndian>(value).unwrap();
        num        
    }
}

impl From<u64> for Numeric {
    fn from(value: u64) -> Self {
        let mut num = Numeric::default();
        num.0[0] = UINT64.into();
        (&mut num.0[1..=9]).write_u64::<BigEndian>(value).unwrap();
        num        
    }
}

impl From<i8> for Numeric {
    fn from(value: i8) -> Self {
        let mut num = Numeric::default();
        num.0[0] = INT8.into();
        (&mut num.0[1..=1]).write_i8(value).unwrap();
        num
    }
}

impl From<i16> for Numeric {
    fn from(value: i16) -> Self {
        let mut num = Numeric::default();
        num.0[0] = INT16.into();
        (&mut num.0[1..=3]).write_i16::<BigEndian>(value).unwrap();
        num        
    }
}

impl From<i32> for Numeric {
    fn from(value: i32) -> Self {
        let mut num = Numeric::default();
        num.0[0] = INT32.into();
        (&mut num.0[1..=5]).write_i32::<BigEndian>(value).unwrap();
        num        
    }
}

impl From<i64> for Numeric {
    fn from(value: i64) -> Self {
        let mut num = Numeric::default();
        num.0[0] = INT64.into();
        (&mut num.0[1..=9]).write_i64::<BigEndian>(value).unwrap();
        num        
    }
}

impl Numeric {
    pub fn kind(&self) -> &NumericKind {
        unsafe {
            std::mem::transmute(&self.0[0])
        }
    }
}

#[derive(FromBytes, Clone, Copy, KnownLayout, Immutable, Eq, PartialEq, Debug)]
pub struct NumericKind(u8);

impl Into<u8> for NumericKind {
    fn into(self) -> u8 {
        self.0
    }
}

impl NumericKind {
    const fn new_int(size: u8, signed: bool) -> Self {
        unsafe {
            let signed: u8 = std::mem::transmute(signed);
            Self(size & 0b111 | (signed * 128))
        }
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

    pub fn get_mut_byte_slice<'data>(&self, data: &'data mut [u8]) -> &'data mut [u8] {
        &mut data[0..usize::from(self.size())]
    }

}

pub const UINT8: NumericKind = NumericKind::new_int(1, false);
pub const UINT16: NumericKind = NumericKind::new_int(2, false);
pub const UINT32: NumericKind = NumericKind::new_int(3, false);
pub const UINT64: NumericKind = NumericKind::new_int(4, false);

pub const INT8: NumericKind = NumericKind::new_int(1, true);
pub const INT16: NumericKind = NumericKind::new_int(2, true);
pub const INT32: NumericKind = NumericKind::new_int(3, true);
pub const INT64: NumericKind = NumericKind::new_int(4, true);
