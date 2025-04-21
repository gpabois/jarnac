use crate::{result::Result, tag::JarTag};
use std::{
    fmt::Debug,
    marker::PhantomData,
    ptr::NonNull,
    sync::atomic::{AtomicIsize, AtomicUsize, Ordering as SyncOrdering},
};

use super::{MutPage, PageSlice, RefPage};

pub struct PageDescriptor<'buf> {
    _pht: PhantomData<&'buf ()>,
    ptr: NonNull<PageDescriptorInner>,
}

impl Clone for PageDescriptor<'_> {
    fn clone(&self) -> Self {
        unsafe { Self::new(self.ptr) }
    }
}

impl Debug for PageDescriptor<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageDescriptor")
            .field("data", &self.ptr)
            .finish()
    }
}

impl Drop for PageDescriptor<'_> {
    fn drop(&mut self) {
        self.as_mut_inner()
            .ref_counter
            .fetch_sub(1, SyncOrdering::Relaxed);
    }
}

impl PageDescriptor<'_> {
    /// Crée un nouveau descripteur de page.
    pub(crate) unsafe fn new(data: NonNull<PageDescriptorInner>) -> Self {
        let page = Self {
            _pht: PhantomData,
            ptr: data,
        };

        page.as_mut_inner()
            .ref_counter
            .fetch_add(1, SyncOrdering::Relaxed);
        page
    }

    /// Initialise le descripteur de page.
    pub(crate) unsafe fn initialise(&mut self, tag: JarTag) {
        let inner = self.as_mut_inner();
        inner.tag = tag;
        inner.flags = 0;
        inner.rw_counter = AtomicIsize::new(0);
        inner.use_counter = AtomicUsize::new(0);
        inner.ref_counter = AtomicUsize::new(1);
        self.borrow_mut(true).fill(0);
    }

    /// Récupère le pointeur interne vers les données du descripteur.
    pub(crate) unsafe fn get_raw_ptr(&self) -> NonNull<PageDescriptorInner> {
        self.ptr
    }

    pub fn buf_id(&self) -> usize {
        self.as_ref_inner().buf_id
    }

    /// Retourne l'identifiant de la page.
    pub fn tag(&self) -> &JarTag {
        &self.as_ref_inner().tag
    }

    /// Lève le "new" flag de la page.
    pub fn set_new(&self) {
        self.as_mut_inner().set_new();
    }

    /// Efface les drapeaux de la page.
    pub fn clear_flags(&self) {
        self.as_mut_inner().clear_flags();
    }

    /// Lève le "dirty" flag de la page.
    pub fn set_dirty(&self) {
        self.as_mut_inner().set_dirty();
    }

    /// Est-ce que la page est nouvelle ?
    pub fn is_new(&self) -> bool {
        self.as_ref_inner().is_new()
    }

    /// Est-ce que la page est sale ?
    pub fn is_dirty(&self) -> bool {
        self.as_ref_inner().is_dirty()
    }

    /// La page est empruntée en écriture.
    pub fn is_mut_borrowed(&self) -> bool {
        self.as_ref_inner().is_mut_borrowed()
    }

    /// Le nombre de fois où cette page est référencée.
    pub fn get_ref_counter(&self) -> usize {
        self.as_ref_inner().get_ref_counter()
    }

    pub fn get_use_counter(&self) -> usize {
        self.as_ref_inner().use_counter.load(SyncOrdering::Relaxed)
    }

    /// Libère le verrou en écriture et récupère un verrou en lecture.
    ///
    /// La fonction panique si aucun verrou en écriture n'a été préalablement acquis.
    pub fn release_write_lock_and_acquire_read_lock(&self) {
        self.as_ref_inner()
            .rw_counter
            .compare_exchange(-1, 1, SyncOrdering::Relaxed, SyncOrdering::Relaxed)
            .unwrap();
    }

    /// Récupère un verrou en écriture.
    ///
    /// La fonction retourne *false* si le verrou n'a pas pu être récupéré.
    pub fn acquire_write_lock(&self) -> bool {
        self.as_ref_inner()
            .rw_counter
            .compare_exchange(0, -1, SyncOrdering::Relaxed, SyncOrdering::Relaxed)
            .is_ok()
    }

    /// Libère le verrou en écriture.
    ///
    /// La fonction panique si aucun verrou en écriture n'a été préalablement acquis.
    pub fn release_write_lock(&self) {
        self.as_ref_inner()
            .rw_counter
            .compare_exchange(-1, 0, SyncOrdering::Relaxed, SyncOrdering::Relaxed)
            .unwrap();
    }

    pub fn acquire_read_lock(&self) -> bool {
        let rw = self.get_rw_counter(SyncOrdering::Acquire);

        if rw < 0 {
            return false;
        }

        self.as_ref_inner()
            .rw_counter
            .fetch_add(1, SyncOrdering::Release)
            > 0
    }

    pub fn release_read_lock(&self) {
        unsafe {
            self.dec_rw_counter(SyncOrdering::Relaxed);
        }
    }

    /// Emprunte les données de la page en lecture.
    ///
    /// Panique si la page ne peut être empruntée en lecture.
    pub fn borrow(&self) -> RefPage<'_> {
        self.try_borrow().unwrap()
    }

    /// Emprunte les données de la page en lecture.
    pub fn try_borrow(&self) -> Result<RefPage<'_>> {
        RefPage::try_new(self.clone())
    }

    /// Emprunte les données de la page en écriture.
    ///
    /// Panique si la page ne peut être empruntée en écriture.
    pub fn borrow_mut(&self, dry: bool) -> MutPage<'_> {
        self.try_borrow_mut(dry).unwrap()
    }

    /// Emprunte les données de la page en écriture.
    pub fn try_borrow_mut(&self, dry: bool) -> Result<MutPage<'_>> {
        MutPage::try_new_with_options(self.clone(), dry)
    }

    /// Décrémente le RW lock
    ///
    /// Cela signifie qu'une nouvelle référence en lecture a été acquise.
    pub(crate) unsafe fn dec_rw_counter(&self, order: SyncOrdering) {
        self.as_mut_inner().rw_counter.fetch_sub(1, order);
    }

    pub fn get_rw_counter(&self, order: SyncOrdering) -> isize {
        self.as_ref_inner().rw_counter.load(order)
    }

    pub fn get_flags(&self) -> u8 {
        self.as_ref_inner().flags
    }

    pub fn set_flags(&self, flags: u8) {
        self.as_mut_inner().flags = flags
    }

    /// Retourne un pointeur vers la tranche de page
    ///
    /// # Safety
    /// C'est un pointeur.
    pub unsafe fn get_content_ptr(&self) -> NonNull<PageSlice> {
        self.as_ref_inner().content
    }

    pub(crate) fn as_ref_inner(&self) -> &PageDescriptorInner {
        unsafe { self.ptr.as_ref() }
    }

    #[allow(clippy::mut_from_ref)]
    pub(crate) fn as_mut_inner(&self) -> &mut PageDescriptorInner {
        unsafe { self.ptr.as_ptr().as_mut().unwrap() }
    }
}

pub(crate) type PageDescriptorPtr = NonNull<PageDescriptorInner>;

/// Page cachée
pub(crate) struct PageDescriptorInner {
    pub buf_id: usize,
    pub tag: JarTag,
    pub content: NonNull<PageSlice>,
    pub flags: u8,
    pub use_counter: AtomicUsize,
    pub rw_counter: AtomicIsize,
    pub ref_counter: AtomicUsize,
}

impl Debug for PageDescriptorInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPageData")
            .field("pid", &self.tag)
            .field("content", &self.content)
            .field("flags", &self.flags)
            .field("use_counter", &self.use_counter)
            .field("rw_counter", &self.rw_counter)
            .finish()
    }
}

impl PageDescriptorInner {
    const DIRTY_FLAGS: u8 = 0b1;
    const NEW_FLAGS: u8 = 0b11;

    pub fn new(buf_id: usize, tag: JarTag, content: NonNull<PageSlice>) -> Self {
        Self {
            buf_id,
            tag,
            content,
            flags: 0,
            use_counter: Default::default(),
            rw_counter: Default::default(),
            ref_counter: Default::default(),
        }
    }

    /// Efface les drapeaux de la page.
    pub fn clear_flags(&mut self) {
        self.flags = 0;
    }

    /// Lève le "dirty" flag de la page.
    pub fn set_dirty(&mut self) {
        self.flags |= Self::DIRTY_FLAGS;
    }

    /// Lève le "new" flag de la page.
    pub fn set_new(&mut self) {
        self.flags |= Self::NEW_FLAGS;
    }

    /// Est-ce que la page est nouvelle ?
    pub fn is_new(&self) -> bool {
        self.flags & Self::NEW_FLAGS == Self::NEW_FLAGS
    }

    /// Est-ce que la page est sale ?
    pub fn is_dirty(&self) -> bool {
        self.flags & Self::DIRTY_FLAGS == Self::DIRTY_FLAGS
    }

    /// La page est empruntée en écriture.
    pub fn is_mut_borrowed(&self) -> bool {
        self.rw_counter.load(SyncOrdering::Relaxed) < 0
    }

    /// Le nombre de fois où cette page est référencée.
    pub fn get_ref_counter(&self) -> usize {
        self.ref_counter.load(SyncOrdering::Relaxed)
    }
}
