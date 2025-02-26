//! Module de page
//! 
//! Les pages sont toujours renvoyées par le [pager](crate::pager::IPager) :
//! - soit en [référence](self::RefPage) ;
//! - soit en [référence mutable](self::MutPage).
//! 
//! Les pages sont indexées par [PageId]. 
pub mod data;
pub mod slice;
pub mod kind;
pub mod size;
pub mod location;
pub mod id;

pub use data::*;
pub use id::*;
pub use kind::*;
pub use size::*;
pub use slice::*;
pub use location::*;

use std::{
    io::Cursor, mem::forget, ops::{Deref, DerefMut}
};

use super::{
    cache::CachedPage,
    error::{PagerError, PagerErrorKind},
    PagerResult,
};

/// Référence vers une page.
pub struct RefPage<'pager>(CachedPage<'pager>);

impl AsRef<PageSlice> for RefPage<'_> {
    fn as_ref(&self) -> &PageSlice {
        unsafe {
            self.0.content.as_ref()
        }
    }
}

impl Clone for RefPage<'_> {
    fn clone(&self) -> Self {
        Self::new(self.0.clone())
    }
}

impl Deref for RefPage<'_> {
    type Target = PageSlice;

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

impl AsRef<PageSlice> for MutPage<'_> {
    fn as_ref(&self) -> &PageSlice {
        self.deref()
    }
}

impl AsMut<PageSlice> for MutPage<'_> {
    fn as_mut(&mut self) -> &mut PageSlice {
        self.deref_mut()
    }
}

impl Deref for MutPage<'_> {
    type Target = PageSlice;

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
