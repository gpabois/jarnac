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
pub mod descriptor;
pub mod cow;

pub use data::*;
use descriptor::PageDescriptor;
pub use id::*;
pub use kind::*;
pub use size::*;
pub use slice::*;
pub use location::*;

use std::{
    io::Cursor, mem::forget, ops::{Deref, DerefMut}
};

use super::{
    error::{PagerError, PagerErrorKind},
    PagerResult,
};

/// Référence vers une page.
pub struct RefPage<'pager>(PageDescriptor<'pager>);

impl AsRef<PageSlice> for RefPage<'_> {
    fn as_ref(&self) -> &PageSlice {
        unsafe {
            self.0.get_content_ptr().as_ref()
        }
    }
}

impl Clone for RefPage<'_> {
    fn clone(&self) -> Self {
        Self::try_new(self.0.clone()).unwrap()
    }
}

impl Deref for RefPage<'_> {
    type Target = PageSlice;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.get_content_ptr().as_ref() }
    }
}

impl Drop for RefPage<'_> {
    fn drop(&mut self) {
        unsafe {
            self.0.dec_rw_counter();
        }
    }
}

impl<'pager> RefPage<'pager> {
    pub(super) fn try_new(descriptor: PageDescriptor<'pager>) -> PagerResult<Self> {
        unsafe {
            if descriptor.get_rw_counter() < 0 {
                Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
            } else {
                descriptor.inc_rw_counter();
                Ok(Self(descriptor))
            }
        }
    }

    /// Transforme la référence en référence faible.
    pub fn downgrade(self) -> WeakPage<'pager> {
        WeakPage(self.0.clone())
    }

    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.deref())
    }

    pub fn id(&self) -> &PageId {
        &self.0.id()
    }
}

/// Référence mutable vers une page.
pub struct MutPage<'pager> {
    /// If true, dirty flag is not raised upon modification
    dry: bool,
    inner: PageDescriptor<'pager>,
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
        unsafe { 
            self.inner.get_content_ptr().as_ref()
        }
    }
}

impl DerefMut for MutPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            if !self.dry {
                self.inner.set_dirty();
            }
            self.inner.get_content_ptr().as_mut()
        }
    }
}

impl<'pager> MutPage<'pager> {
    pub(super) fn try_new(inner: PageDescriptor<'pager>) -> PagerResult<Self> {
        unsafe {
            if inner.get_rw_counter() != 0 {
                Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
            } else {
                inner.dec_rw_counter();
                Ok(Self { dry: false, inner })
            }
        }
    }

    pub(super) fn try_new_with_options(
        inner: PageDescriptor<'pager>,
        dry: bool,
    ) -> PagerResult<Self> {
        unsafe {
            if inner.get_rw_counter() != 0 {
                Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed))
            } else {
                inner.dec_rw_counter();
                Ok(Self { dry, inner })
            }
        }
    }

    /// Transforme la référence en référence faible.
    pub fn downgrade(self) -> WeakPage<'pager> {
        WeakPage(self.inner.clone())
    }

    /// Transforme la référence mutable en référence simple.
    pub fn into_ref(self) -> RefPage<'pager> {
        unsafe {
            self.inner.set_rw_counter(1);
            let rf = RefPage(self.inner.clone());
            forget(self);
            rf
        }
    }

    pub fn id(&self) -> &PageId {
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
        unsafe {
            self.inner.inc_rw_counter();
        }
    }
}


#[derive(Clone)]
/// Référence faible vers une page.
/// 
/// Le pointeur faible garde la page en mémoire, il faut donc veiller à ne 
/// pas les garder trop longtemps pour éviter un phénomène d'engorgement
/// du cache.
pub struct WeakPage<'pager>(PageDescriptor<'pager>);

impl<'pager> WeakPage<'pager> {
    /// Transforme la référence faible en référence.
    /// 
    /// #Panics
    /// Panique si une référence mutable est déjà prise.
    pub fn upgrade_ref(self) -> RefPage<'pager> {
        RefPage::try_new(self.0).unwrap()
    }

    /// Transforme la référence faible en référence mutable.
    /// 
    /// #Panics
    /// Panique si une référence est déjà prise.
    pub fn upgrade_mut(self) -> MutPage<'pager> {
        MutPage::try_new(self.0).unwrap()
    }
}
