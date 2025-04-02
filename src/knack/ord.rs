use std::mem::transmute;

use zerocopy::{FromBytes, IntoBytes, F32, F64, I128, I16, I32, I64, U128, U16, U32, U64, LE};

use crate::utils::Shift;

use super::{kind::KnackKind, marker::{kernel::{AsKernelMut, AsKernelRef}, Comparable}, GetKnackKind as _, Knack};

impl<L> Comparable<L> where L: AsKernelRef<Kernel = KnackKind> {
    /// offset-size : xx [111:offset] [111:size]
    /// Size is a power of 2
    /// Offset is a power of 2
    pub fn new(mut base: L, float: bool, signed: bool, size: u8, offset: u8) -> Self where L: AsKernelMut<Kernel = KnackKind> {
        base.as_kernel_mut().as_mut_bytes()[0] |= signed.then(|| KnackKind::FLAG_SIGNED).unwrap_or_default() 
            | float.then(|| KnackKind::FLAG_FLOAT).unwrap_or_default();
        base.as_kernel_mut().as_mut_bytes()[4] = size | offset;
        Self(base)
    }
}

impl Comparable<KnackKind> {
    fn is_float(&self) -> bool {
        let flags = self.0.as_bytes()[0];
        flags & KnackKind::FLAG_FLOAT > 0
    }

    fn is_signed(&self) -> bool {
        let flags = self.0.as_bytes()[0];
        flags & KnackKind::FLAG_SIGNED > 0
    }

    fn size(&self) -> u8 {
        self.as_kernel_ref().as_bytes()[4] & 0b111
    }

    fn offset(&self) -> u8 {
        self.as_kernel_ref().as_bytes()[4] & 0b111000
    }
}

impl Comparable<Knack> {
    pub fn kind(&self) -> &Comparable<KnackKind> {
        self.0.kind().try_as_comparable().unwrap()
    }

    fn raw(&self) -> &[u8] {
        let kind = self.kind();
        let slice = (0..usize::from(self.kind().size())).shift(usize::from(kind.offset()));
        &self.as_bytes()[slice]
    }
}

impl PartialEq<Knack> for Comparable<Knack> {
    fn eq(&self, other: &Knack) -> bool {
        self.as_value_bytes() == other.as_value_bytes()
    }
}

impl PartialOrd<Knack> for Comparable<Knack> {
    fn partial_cmp(&self, other: &Knack) -> Option<std::cmp::Ordering> {
        if self.kind().as_kernel_ref() != other.kind() {
            return None
        }
        let other = other.try_as_comparable().unwrap();

        let kind = self.kind();

        match (kind.is_float(), kind.is_signed(), kind.size()) {
            (true, _, 3) => {
                let lhs = F32::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = F32::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)
            }, 
            (true, _, 4) => {
                let lhs = F64::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = F64::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)          
            }, 
            (_, true, 1) => {
                unsafe {
                    let lhs: i8 = transmute(self.raw()[0]);
                    let rhs: i8 = transmute(other.raw()[0]);
                    lhs.partial_cmp(&rhs)   
                }   
            }, 
            (_, false, 1) => {
                unsafe {
                    let lhs: u8 = transmute(self.raw()[0]);
                    let rhs: u8 = transmute(other.raw()[0]);
                    lhs.partial_cmp(&rhs)   
                }  
            },
            (_, true, 2) => {
                let lhs = I16::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = I16::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)               
            },
            (_, false, 2) => {
                let lhs = U16::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = U16::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)        
            },
            (_, true, 3) => {
                let lhs = I32::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = I32::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)     
            }, 
            (_, false, 3) => {
                let lhs = U32::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = U32::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)     
            },  
            (_, true, 4) => {
                let lhs = I64::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = I64::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)     
            }, 
            (_, false, 4) => {
                let lhs = U64::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = U64::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)     
            }, 
            (_, true, 5) => {
                let lhs = I128::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = I128::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)     
            }, 
            (_, false, 5) => {
                let lhs = U128::<LE>::ref_from_bytes(self.raw()).unwrap();
                let rhs = U128::<LE>::ref_from_bytes(other.raw()).unwrap();
                lhs.partial_cmp(rhs)     
            },
            _ => None 
        }
    }
}