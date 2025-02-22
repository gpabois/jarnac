//! Module de page
//! 
//! Les pages sont toujours renvoyées par le [pager](crate::pager::IPager) :
//! - soit en [référence](self::RefPage) ;
//! - soit en [référence mutable](self::MutPage).
//! 
//! Les pages sont indexées par [PageId]. 
pub mod page_data;
pub mod into_page_slice_index;
pub mod page_slice_data;
pub mod page_kind;
pub mod page_size;
pub mod page_location;
pub mod page_id;

pub use page_data::*;
pub use into_page_slice_index::*;
pub use page_id::*;
pub use page_kind::*;
pub use page_size::*;
pub use page_slice_data::*;

use std::{
    io::Cursor, mem::forget, ops::{Deref, DerefMut}
};

use zerocopy::{Immutable, KnownLayout, TryFromBytes};

use super::{
    cache::CachedPage,
    error::{PagerError, PagerErrorKind},
    PagerResult,
};

pub trait TryIntoRefFromBytes<Output> 
where Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_ref_from_bytes(&self) -> &Output;
}

impl<'a, Output, U> TryIntoRefFromBytes<Output> for &'a U 
where U: TryIntoRefFromBytes<Output>, Output: TryFromBytes + KnownLayout + Immutable + ?Sized {
    fn try_into_ref_from_bytes(&self) -> &Output {
        self.deref().try_into_ref_from_bytes()
    }
}

pub trait TryIntoMutFromBytes<Output: TryFromBytes + KnownLayout + Immutable + ?Sized> {
    fn try_into_mut_from_bytes(&mut self) -> &mut Output;
}


/// Référence vers une page.
pub struct RefPage<'pager>(CachedPage<'pager>);

impl<'pager> PageData for RefPage<'pager> {}

impl AsRef<[u8]> for RefPage<'_> {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            self.0.content.as_ref()
        }
    }
}

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

/// Référence mutable vers une page.
pub struct MutPage<'pager> {
    /// If true, dirty flag is not raised upon modification
    dry: bool,
    inner: CachedPage<'pager>,
}

impl MutPageData for MutPage<'_> {}
impl PageData for MutPage<'_> {}

impl AsRef<[u8]> for MutPage<'_> {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl AsMut<[u8]> for MutPage<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

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


impl Drop for MutPage<'_> {
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
