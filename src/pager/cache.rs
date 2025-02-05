use std::{
    alloc::{alloc_zeroed, dealloc, Layout}, cell::RefCell, collections::HashMap, marker::PhantomData, mem::MaybeUninit, ops::{Deref, DerefMut}, ptr::NonNull
};

use itertools::Itertools;

use super::{error::{PagerError, PagerErrorKind}, page::PageId, stress::{BoxedPagerStress, IPagerStress}, PagerResult};

pub(super) struct PagerCache {
    /// The memory layout of the allocated space
    layout: Layout,
    /// The allocated cache space
    ptr: NonNull<u8>,
    /// The size of the cache
    size: usize,
    /// The tail of allocated space
    tail: RefCell<usize>,
    /// The size of a page
    page_size: usize,
    /// Free page cells
    free_list: RefCell<Vec<NonNull<CachedPageData>>>,
    /// Current cached pages
    pages: RefCell<HashMap<PageId, NonNull<CachedPageData>>>,
    /// Stratégie de gestion du stress mémoire
    /// Employé si le cache est plein
    stress: BoxedPagerStress
}

impl Drop for PagerCache {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_mut(), self.layout);
        }
    }
}

pub(super) struct PagerCacheIter<'cache>{
    ids: Vec<PageId>,
    cache: &'cache PagerCache
}

impl<'cache> Iterator for PagerCacheIter<'cache> {
    type Item = PagerResult<CachedPage<'cache>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids
        .pop()
        .map(|pid| self.cache.get(&pid))
    }
}

impl PagerCache {
    pub fn new<Ps: IPagerStress + 'static>(cache_size: usize, page_size: usize, stress_strategy: Ps) -> Self {
        unsafe {
            let layout = Layout::from_size_align(
                cache_size,
                (page_size + size_of::<CachedPageData>()).next_power_of_two(),
            )
            .unwrap();

            let ptr = NonNull::new(alloc_zeroed(layout)).unwrap();

            return Self {
                layout,
                ptr,
                size: cache_size,
                tail: RefCell::new(0),
                page_size,
                free_list: RefCell::default(),
                pages: RefCell::default(),
                stress: BoxedPagerStress::new(stress_strategy)
            };
        }
    }


    /// Alloue de l'espace dans le cache pour stocker une page.
    pub fn alloc<'cache>(&'cache self, pid: &PageId) -> PagerResult<CachedPage<'cache>> {
        // Déjà caché
        if self.pages.borrow().contains_key(pid) {
            return Err(PagerError::new(PagerErrorKind::PageAlreadyCached(*pid)))
        }      

        // On a un slot de libre
        if let Some(mut free) = self.free_list.borrow_mut().pop().map(CachedPage::new) {
            unsafe {
                free.flags = 0;
                free.use_counter = 0;
                free.rw_counter = 0;
                self.pages.borrow_mut().insert(*pid, free.leak());
                return Ok(free);
            }
        }

        let new_tail = self.page_size + size_of::<CachedPageData>();

        // Le cache est plein, on est dans un cas de stress mémoire
        // On va essayer de trouver de la place.
        if new_tail >= self.size {
            unsafe {
                let mut pcached = self.free_some_space()?;
                pcached.content.as_mut().fill(0);
                pcached.pid = *pid;
                pcached.flags = 0;
                pcached.rw_counter = 0;
                pcached.use_counter = 0;
                
                return Ok(pcached)
            }
        }

        // On ajoute une nouvelle entrée dans le cache
        unsafe {
            let ptr = self.ptr.add(*self.tail.borrow());
            let mut cell_ptr = ptr.cast::<MaybeUninit<CachedPageData>>();
            let content_ptr = ptr.add(size_of::<CachedPageData>());
            let content = NonNull::slice_from_raw_parts(content_ptr, self.page_size);
            let cell_ptr = NonNull::new_unchecked(cell_ptr.as_mut().write(CachedPageData::new(*pid, content)));
            self.pages.borrow_mut().insert(*pid, cell_ptr);
            *self.tail.borrow_mut() = new_tail;
            return Ok(CachedPage::new(cell_ptr));
        }
    }

    /// Libère une entrée du cache
    /// 
    /// L'opération échoue si la page est toujours empruntée.
    pub fn free(&self, id: &PageId) -> PagerResult<()> {
        if let Some(cell) = self.pages.borrow_mut().remove(id) {
            unsafe {
                if cell.as_ref().rw_counter != 0 {
                    return Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed));
                  }
              }
              self.free_list.borrow_mut().push(cell);
        }

        return Ok(())
    }

    /// Itère les pages cachées.
    /// 
    /// L'itération peut échouer si des pages déchargées ne peuvent être récupérées en mémoire.
    /// Voir [Self::free_some_space] pour plus d'explications.
    pub fn iter(&self) -> PagerCacheIter<'_> {
        PagerCacheIter { ids: self.pages.borrow().keys().copied().collect(), cache: self }
    }

    /// Récupère la page si elle est cachée, panique si elle n'existe pas.
    pub fn get(&self, pid: &PageId) -> PagerResult<CachedPage> {
        self
        .try_get(pid)
        .and_then(|opt| opt.ok_or_else(|| PagerError::new(PagerErrorKind::PageNotCached(*pid))))
    }
    

    /// Récupère la page si elle est cachée.
    /// 
    /// L'opération peut échouer si :
    /// - La page n'est pas cachée
    /// - La page est déchargée et aucune place n'a put être trouvée pour la récupérer en mémoire.
    pub fn try_get<'cache>(&'cache self, pid: &PageId) -> PagerResult<Option<CachedPage<'cache>>> {
        // La page est en cache, on la renvoie
        if let Some(stored) = self.pages.borrow().get(&pid).copied() {
            return Ok(Some(CachedPage::new(stored)));
        }

        // La page a été déchargée, on va essayer de la récupérer.
        if self.stress.contains(pid) {
            let mut pcache = self.alloc(pid)?;
            self.stress.retrieve(&mut pcache)?;
            return Ok(Some(pcache))
        }

        return Ok(None)
    }



}

impl PagerCache {
    /// Libère de la place :
    /// - soit en libérant une entrée du cache contenant une page propre ;
    /// - soit en déchargeant des pages quelque part (voir [IPagerStress]).
    /// 
    /// Si aucune page n'est libérable ou déchargeable, principalement car elles sont
    /// toutes empruntées, alors l'opération échoue et retourne l'erreur *CacheFull*.
    fn free_some_space(&self) -> PagerResult<CachedPage<'_>> {
        // On trouve une page propre non empruntée
        if let Some(cleaned) = self.pages.borrow().values().filter(|cell| unsafe {cell.as_ref().is_dirty() == false && cell.as_ref().rw_counter == 0}).sorted_by_key(|cell| unsafe {cell.as_ref().use_counter}).copied().next().map(CachedPage::new) {
            self.free(&cleaned.pid)?;

            return Ok(cleaned)
        }
        // on va décharger une page en mémoire
        if let Some(dischargeable) = self.pages.borrow().values().filter(|cell| unsafe {cell.as_ref().rw_counter == 0}).sorted_by_key(|cell| unsafe {cell.as_ref().use_counter}).copied().next().map(CachedPage::new) {
            self.stress.discharge(&dischargeable)?;
            self.free(&dischargeable.pid)?;

            return Ok(dischargeable);
        }

        return Err(PagerError::new(PagerErrorKind::CacheFull));
    }
}


#[derive(Clone, Copy)]
pub struct CachedPage<'cache> {
    _pht: PhantomData<&'cache ()>,
    data: NonNull<CachedPageData>
}

impl<'cache> CachedPage<'cache> {
    pub(super) fn new(data: NonNull<CachedPageData>) -> Self {
        Self {
            _pht: PhantomData,
            data
        }
    }

    pub unsafe fn leak(self) -> NonNull<CachedPageData> {
        self.data
    }
}

impl DerefMut for CachedPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.data.as_mut()
        }
    }
}

impl Deref for CachedPage<'_> {
    type Target = CachedPageData;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.data.as_ref()
        }
    }
}

/// Page cachée
pub struct CachedPageData {
    pub pid: PageId,
    pub content: NonNull<[u8]>,
    pub flags: u8,
    pub use_counter: usize,
    pub rw_counter: isize,
}

impl CachedPageData {
    const DIRTY_FLAGS: u8 = 0b1;
    const NEW_FLAGS: u8 = 0b11;

    pub fn borrow_mut_content(&mut self) -> &mut [u8] {
        unsafe {
            self.content.as_mut()
        }
    }

    pub fn borrow_content(&self) -> &[u8] {
        unsafe {
            self.content.as_ref()
        }
    }

    pub fn new(pid: PageId, content: NonNull<[u8]>) -> Self {
        Self {
            pid, 
            content, 
            flags: 0, 
            use_counter: 0, 
            rw_counter: 0
        }
    }

    pub fn id(&self) -> PageId {
        self.pid
    }

    pub fn clear_flags(&mut self) {
        self.flags = 0;
    }
    
    pub fn set_diry(&mut self) {
        self.flags |= Self::DIRTY_FLAGS;
    }

    pub fn set_new(&mut self) {
        self.flags |= Self::NEW_FLAGS;
    }

    pub fn is_new(&self) -> bool {
        self.flags & Self::NEW_FLAGS == Self::NEW_FLAGS
    }

    pub fn is_dirty(&self) -> bool {
        self.flags & Self::DIRTY_FLAGS == Self::DIRTY_FLAGS
    }

    pub fn is_mut_borrowed(&self) -> bool {
        self.rw_counter < 0
    }
    
}