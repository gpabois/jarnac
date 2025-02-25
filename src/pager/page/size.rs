use std::ops::{Add, Sub};

use zerocopy_derive::*;

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
/// Taille d'une page
/// 
/// Les valeurs vont jusqu'à [u16::MAX]. C'est à dire jusqu'à 64 Kio.
/// 
/// L'idée est d'avoir une taille de page qui soit le reflet de la taille d'un bloc du système de fichier.
/// 
/// # Example
/// Pour un volume entre 2 et 16 tebibytes, le FAT32 impose des blocs d'une taille de 64 Kio.
pub struct PageSize(pub(super) u16);

impl PageSize {
    pub fn new(value: u16) -> Self {
        Self(value)
    }
}

impl Add<usize> for PageSize {
    type Output = usize;

    fn add(self, rhs: usize) -> Self::Output {
        let ps_usize: usize = self.0.try_into().unwrap();
        ps_usize + rhs
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

impl Into<usize> for PageSize {
    fn into(self) -> usize {
        self.0.try_into().unwrap()
    }
}

