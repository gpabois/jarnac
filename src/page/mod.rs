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

pub use data::*;
use descriptor::PageDescriptor;
pub use id::*;
pub use kind::*;
pub use size::*;
pub use slice::*;
pub use location::*;

use std::{
    io::Cursor, marker::PhantomData, mem::forget, ops::{Deref, DerefMut, Range}
};

use crate::{error::{Error, ErrorKind}, result::Result, tag::{DataArea, JarTag}};

pub struct InPage<T>(PhantomData<T>);

impl<T> DataArea for InPage<T> {
    const AREA: Range<usize> = 1..(size_of::<T>() + 1);
    const INTEGRATED_AREA: Range<usize> = 0..Self::AREA.end + 1;
}

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
        self.0.release_read_lock();
    }
}

impl<'pager> RefPage<'pager> {
    pub(crate) fn try_new(descriptor: PageDescriptor<'pager>) -> Result<Self> {
        if descriptor.acquire_read_lock() {
            Ok(Self(descriptor))
        } else {
            Err(Error::new(ErrorKind::PageCurrentlyBorrowed))
        }
    }

    /// Transforme la référence en référence faible.
    pub fn downgrade(self) -> WeakPage<'pager> {
        WeakPage(self.0.clone())
    }

    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.deref())
    }

    pub fn tag(&self) -> &JarTag {
        &self.0.tag()
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
    pub(crate) fn try_new(inner: PageDescriptor<'pager>) -> Result<Self> {
        if inner.acquire_write_lock() {
            Ok(Self { dry: false, inner })
        } else {
            Err(Error::new(ErrorKind::PageCurrentlyBorrowed))
        }
    }

    pub(crate) fn try_new_with_options(
        inner: PageDescriptor<'pager>,
        dry: bool,
    ) -> Result<Self> {
        if inner.acquire_write_lock() {
            Ok(Self { dry: dry, inner })
        } else {
            Err(Error::new(ErrorKind::PageCurrentlyBorrowed))
        }
    }

    /// Transforme la référence en référence faible.
    pub fn downgrade(self) -> WeakPage<'pager> {
        WeakPage(self.inner.clone())
    }

    /// Transforme la référence mutable en référence simple.
    pub fn into_ref(self) -> RefPage<'pager> {
        self.inner.release_write_lock_and_acquire_read_lock();
        let rf = RefPage(self.inner.clone());
        forget(self);
        rf  
    }

    pub fn tag(&self) -> &JarTag {
        self.inner.tag()
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
        self.inner.release_write_lock();
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
