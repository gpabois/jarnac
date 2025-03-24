use std::{fmt::{Debug, Display}, ops::{Add, Mul, Sub}};

use zerocopy_derive::*;

use crate::pager::cell::CellCapacity;

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
/// Taille d'une page
/// 
/// Les valeurs vont jusqu'à [u16::MAX]. C'est à dire jusqu'à 64 Kio.
/// 
/// L'idée est d'avoir une taille de page qui soit le reflet de la taille d'un bloc du système de fichier.
/// 
/// # Example
/// Pour un volume entre 2 et 16 tebibytes, le FAT32 impose des blocs d'une taille de 64 Kio.
pub struct PageSize(u16);

impl Debug for PageSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for PageSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl PageSize {
    pub fn new(value: u16) -> Self {
        Self(value)
    }
}

impl Mul<CellCapacity> for PageSize {
    type Output = PageSize;

    fn mul(self, rhs: CellCapacity) -> Self::Output {
        let rhs_u16: u16 = rhs.into();
        Self(rhs_u16 * self.0)
    }
}

impl Mul<usize> for PageSize {
    type Output = usize;

    fn mul(self, rhs: usize) -> Self::Output {
        rhs * usize::from(self.0)
    }
}

impl Mul<u16> for PageSize {
    type Output = PageSize;

    fn mul(self, rhs: u16) -> Self::Output {
        Self(rhs * self.0)
    }
}


impl Into<usize> for PageSize {
    fn into(self) -> usize {
        usize::from(self.0)
    }
}

impl Into<u64> for PageSize {
    fn into(self) -> u64 {
        u64::from(self.0)
    }
}

impl Add<usize> for PageSize {
    type Output = usize;

    fn add(self, rhs: usize) -> Self::Output {
        let ps_usize: usize = self.0.try_into().unwrap();
        ps_usize + rhs
    }
}

impl Add<PageSize> for PageSize {
    type Output = PageSize;

    fn add(self, rhs: PageSize) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}


impl Sub<PageSize> for PageSize {
    type Output = PageSize;

    fn sub(self, rhs: PageSize) -> Self::Output {
        Self(self.0 - rhs.0)
    }
}

impl Sub<u16> for PageSize {
    type Output = u16;

    fn sub(self, rhs: u16) -> Self::Output {
        self.0 - rhs
    }
}

impl From<u16> for PageSize {
    fn from(value: u16) -> Self {
        Self(value)
    }
}


impl From<usize> for PageSize {
    fn from(value: usize) -> Self {
        Self(value.try_into().unwrap())
    }
}
