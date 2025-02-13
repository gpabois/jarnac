//! Module de page
//! 
//! Les pages sont toujours renvoyées par le [pager](crate::pager::IPager) :
//! - soit en [référence](self::RefPage) ;
//! - soit en [référence mutable](self::MutPage).
//! 
//! Les pages sont indexées par [PageId]. 
use std::{
    fmt::Display, io::Cursor, mem::forget, num::NonZero, ops::{Add, Deref, DerefMut, Mul, Range, Sub}
};

use zerocopy::{ByteSlice, Immutable, KnownLayout, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use super::{
    cache::CachedPage,
    error::{PagerError, PagerErrorKind},
    PagerResult,
};

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, PartialEq, Eq)]
pub struct OptionalPageId(Option<NonZero<u64>>);

impl AsRef<Option<PageId>> for OptionalPageId {
    fn as_ref(&self) -> &Option<PageId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl AsMut<Option<PageId>> for OptionalPageId {
    fn as_mut(&mut self) -> &mut Option<PageId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl From<Option<PageId>> for OptionalPageId {
    fn from(value: Option<PageId>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl Into<Option<PageId>> for OptionalPageId {
    fn into(self) -> Option<PageId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}


#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, TryFromBytes, Immutable, KnownLayout)]
#[repr(transparent)]
/// Identifiant d'une page
/// 
/// Les valeurs vont de 1 à [u64::MAX]
pub struct PageId(pub(super)NonZero<u64>);

impl PageId {
    pub(super) fn new(value: u64) -> Self {
        Self(NonZero::new(value).expect("page id must be > 0"))
    }
}

impl Mul<PageSize> for PageId {
    type Output = PageLocation;

    fn mul(self, rhs: PageSize) -> Self::Output {
        PageLocation((self.0.get() - 1) * u64::from(rhs.0))
    }
}

impl PartialEq<u64> for PageId {
    fn eq(&self, other: &u64) -> bool {
        self.0.get().eq(other)
    }
}

impl PartialOrd<u64> for PageId {
    fn partial_cmp(&self, other: &u64) -> Option<std::cmp::Ordering> {
        self.0.get().partial_cmp(other)
    }
}

impl From<NonZero<u64>> for PageId {
    fn from(value: NonZero<u64>) -> Self {
        Self(value)
    }
}

impl Into<NonZero<u64>> for PageId {
    fn into(self) -> NonZero<u64> {
        self.0
    }
}

impl From<usize> for PageId {
    fn from(value: usize) -> Self {
        Self(NonZero::try_from(u64::try_from(value).unwrap()).expect("must be a non-zeroed value"))
    }
}

impl From<u64> for PageId {
    fn from(value: u64) -> Self {
        Self(NonZero::new(value).expect(&format!("page id should be > 0, got {value}")))
    }
}

impl Into<u64> for PageId {
    fn into(self) -> u64 {
        self.0.get()
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PageId {
    pub fn get_location(&self, base: u64, page_size: &PageSize) -> PageLocation {
        (*self) * (*page_size) + base
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageLocation(u64);

impl Add<u64> for PageLocation {
    type Output = PageLocation;

    fn add(mut self, rhs: u64) -> Self::Output {
        self.0 += rhs;
        self
    }
}

impl Into<u64> for PageLocation {
    fn into(self) -> u64 {
        self.0
    }
}

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


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
/// Type de page.
/// 
/// Toutes les pages démarrent avec un octet qui permet d'identifier sa nature.
pub enum PageKind {
    /// Une page libre (cf [crate::pager::free])
    Free = 0,
    /// Une page de débordement (cf [crate::pager::spill])
    Overflow = 1,
    /// La page d'entrée d'un arbre B+ (cf [crate::bp_tree::BPlusTreePage])
    BPlusTree = 2,
    /// La page représentant un noeud intérieur d'un arbre B+ (cf [crate::bp_tree::BPTreeInteriorPage])
    BPlusTreeInterior = 3,
    /// La page représentant une feuille d'un arbre B+ (cf [crate::bp_tree::BPTreeLeafPage])
    BPlusTreeLeaf = 4
}

impl Display for PageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageKind::Free => write!(f, "free"),
            PageKind::Overflow => write!(f, "spill"),
            PageKind::BPlusTree => write!(f, "b+ tree"),
            PageKind::BPlusTreeInterior => write!(f, "b+ tree interior"),
            PageKind::BPlusTreeLeaf => write!(f, "b+ tree leaf"),
        }
    }
}

impl PageKind {
    pub fn assert(&self, to: PageKind) -> PagerResult<()> {
        (*self == to).then_some(()).ok_or_else(|| {
            PagerError::new(PagerErrorKind::WrongPageKind {
                expected: to,
                got: *self,
            })
        })
    }
}

impl TryFrom<u8> for PageKind {
    type Error = PagerError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Free),
            1 => Ok(Self::Overflow),
            _ => Err(PagerError::new(PagerErrorKind::InvalidPageKind)),
        }
    }
}

pub trait PageData<'page>: Deref<Target = [u8]> + IntoPageSlice<Range<usize>> + 'page {}
pub trait PageMutData<'page>: DerefMut<Target = [u8]> + PageData<'page> {}

pub trait PageSlice<'page>: Deref<Target = [u8]> + 'page {}
pub trait PageMutSlice<'page>: DerefMut<Target = [u8]> + 'page {}

pub trait TryIntoRefFromBytes<Output: TryFromBytes + KnownLayout + Immutable + ?Sized> {
    fn try_into_ref_from_bytes(&self) -> &Output;
}

pub trait TryIntoMutFromBytes<Output: TryFromBytes + KnownLayout + Immutable + ?Sized> {
    fn try_into_mut_from_bytes(&mut self) -> &mut Output;
}

pub trait IntoPageSlice<Idx> {
    type Output: Deref<Target = [u8]>;

    fn into_page_slice(self, idx: Idx) -> Self::Output;
}

pub trait IntoMutPageSlice<Idx> {
    type Output;

    fn into_mut_page_slice(self, idx: Idx) -> Self::Output;
}

/// Référence vers une page.
pub struct RefPage<'pager>(CachedPage<'pager>);

impl<'pager> PageData<'pager> for RefPage<'pager> {}

impl<Output> TryIntoRefFromBytes<Output> for RefPage<'_> 
where Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_ref_from_bytes(&self) -> &Output {
        Output::try_ref_from_bytes(self.deref()).unwrap()
    }
}

impl Clone for RefPage<'_> {
    fn clone(&self) -> Self {
        Self::new(self.0.clone())
    }
}

impl<'pager, Idx> IntoPageSlice<Idx> for RefPage<'pager> 
where Idx: std::slice::SliceIndex<[u8], Output = [u8]>
{
    type Output = RefPageSlice<'pager>;
    
    fn into_page_slice(self, idx: Idx) -> Self::Output {
        unsafe {
            let slice = RefPageSlice {
                inner: self.0.clone(), 
                slice: &self.0.content.as_ref()[idx]
            };
            forget(self);
            slice
        }
    }
}

unsafe impl ByteSlice for RefPage<'_> {}

impl Deref for RefPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.content.as_ref() }
    }
}

impl Drop for RefPage<'_> {
    fn drop(&mut self) {
        self.0.rw_counter -= 1;
    }
}

impl<'pager> RefPage<'pager> {
    pub(super) fn new(mut cached: CachedPage<'pager>) -> Self {
        if cached.rw_counter < 0 {
            panic!("already mutable borrowed")
        }

        cached.rw_counter += 1;
        Self(cached)
    }

    pub(super) fn try_new(mut cached: CachedPage<'pager>) -> PagerResult<Self> {
        if cached.rw_counter < 0 {
            Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
        } else {
            cached.rw_counter += 1;
            Ok(Self(cached))
        }
    }

    /// Transforme la référence en référence faible.
    pub fn downgrade(self) -> WeakPage<'pager> {
        WeakPage(self.0.clone())
    }

    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.deref())
    }

    pub fn id(&self) -> PageId {
        self.0.id()
    }
}

/// Référence vers une tranche de données d'une page.
pub struct RefPageSlice<'pager>{
    inner: CachedPage<'pager>, 
    slice: &'pager [u8]
}

impl<Output> TryIntoRefFromBytes<Output> for RefPageSlice<'_> 
where Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_ref_from_bytes(&self) -> &Output {
        Output::try_ref_from_bytes(self.slice).unwrap()
    }
}

impl Drop for RefPageSlice<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter -= 1;
    }
}

impl Deref for RefPageSlice<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.slice
    }
}

/// Référence mutable vers une page.
pub struct MutPage<'pager> {
    /// If true, dirty flag is not raised upon modification
    dry: bool,
    inner: CachedPage<'pager>,
}

unsafe impl ByteSlice for MutPage<'_> {}

impl Deref for MutPage<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { self.inner.content.as_ref() }
    }
}

impl DerefMut for MutPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            if !self.dry {
                self.inner.set_dirty();
            }
            self.inner.content.as_mut()
        }
    }
}

impl Drop for MutPage<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter += 1;
    }
}

impl<'pager, Idx> IntoPageSlice<Idx> for MutPage<'pager> 
where Idx: std::slice::SliceIndex<[u8], Output = [u8]>
{
    type Output = RefPageSlice<'pager>;
    
    fn into_page_slice(self, idx: Idx) -> Self::Output {
        self
            .into_ref()
            .into_page_slice(idx)
    }
}

impl<'pager, Idx> IntoMutPageSlice<Idx> for MutPage<'pager> 
where Idx: std::slice::SliceIndex<[u8], Output = [u8]>
{
    type Output = MutPageSlice<'pager>;
    
    fn into_mut_page_slice(mut self, idx: Idx) -> Self::Output {
        unsafe {
            let slice = MutPageSlice {
                inner: self.inner.clone(), 
                slice: &mut self.inner.content.as_mut()[idx]
            };
            forget(self);
            slice
        }
    }
}

impl<Output> TryIntoMutFromBytes<Output> for MutPage<'_> 
where Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_mut_from_bytes(&mut self) -> &mut Output {
        Output::try_mut_from_bytes(self.deref_mut()).unwrap()
    }
}

impl<'pager> MutPage<'pager> {
    pub(super) fn try_new(mut inner: CachedPage<'pager>) -> PagerResult<Self> {
        if inner.rw_counter != 0 {
            Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
        } else {
            inner.rw_counter -= 1;
            Ok(Self { dry: false, inner })
        }
    }

    pub(super) fn try_new_with_options(
        mut inner: CachedPage<'pager>,
        dry: bool,
    ) -> PagerResult<Self> {
        if inner.rw_counter != 0 {
            Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
        } else {
            inner.rw_counter -= 1;
            Ok(Self { dry, inner })
        }
    }

    /// Transforme la référence en référence faible.
    pub fn downgrade(self) -> WeakPage<'pager> {
        WeakPage(self.inner.clone())
    }

    /// Transforme la référence mutable en référence simple.
    pub fn into_ref(mut self) -> RefPage<'pager> {
        self.inner.rw_counter = 1;
        let rf = RefPage(self.inner.clone());
        forget(self);
        rf
    }

    pub fn id(&self) -> PageId {
        self.inner.id()
    }

    /// Ouvre une curseur permettant de modifier le contenu de la page.
    pub fn open_mut_cursor(&mut self) -> Cursor<&mut [u8]> {
        Cursor::new(self.deref_mut())
    }

    /// Ouvre un curseur permettant de lire le contenu de la page.
    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.deref())
    }
}


/// Une tranche mutable d'une page.
pub struct MutPageSlice<'pager>{
    inner: CachedPage<'pager>, 
    slice: &'pager mut [u8]
}

impl Drop for MutPageSlice<'_> {
    fn drop(&mut self) {
        self.inner.rw_counter += 1;
    }
}

#[derive(Clone)]
/// Référence faible vers une page.
/// 
/// Le pointeur faible garde la page en mémoire, il faut donc veiller à ne 
/// pas les garder trop longtemps pour éviter un phénomène d'engorgement
/// du cache.
pub struct WeakPage<'pager>(CachedPage<'pager>);

impl<'pager> WeakPage<'pager> {
    /// Transforme la référence faible en référence.
    /// 
    /// #Panics
    /// Panique si une référence mutable est déjà prise.
    pub fn upgrade_ref(self) -> RefPage<'pager> {
        RefPage::new(self.0)
    }

    /// Transforme la référence faible en référence mutable.
    /// 
    /// #Panics
    /// Panique si une référence est déjà prise.
    pub fn upgrade_mut(self) -> MutPage<'pager> {
        MutPage::try_new(self.0).unwrap()
    }
}
