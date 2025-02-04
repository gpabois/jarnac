use std::{
    alloc::{alloc_zeroed, dealloc, Layout}, 
    cell::RefCell, 
    collections::HashMap, 
    mem::MaybeUninit, 
    ptr::NonNull
};

use itertools::Itertools;

use super::{page::PageId, stress::{BoxedPagerStress, IPagerStress}, error::PagerError, PagerResult};

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
    free_list: RefCell<Vec<NonNull<PageCell>>>,
    /// Current cached pages
    pages: RefCell<HashMap<PageId, NonNull<PageCell>>>,
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

impl Iterator for PagerCacheIter<'_> {
    type Item = PagerResult<NonNull<PageCell>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.pop().map(|pid| self.cache.get(&pid))
    }
}

impl PagerCache {
    pub fn new<Ps: IPagerStress + 'static>(cache_size: usize, page_size: usize, stress_strategy: Ps) -> Self {
        unsafe {
            let layout = Layout::from_size_align(
                cache_size,
                (page_size + size_of::<PageCell>()).next_power_of_two(),
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

    /// Itère les pages cachées/déchargées
    pub fn iter_pages(&self) -> PagerCacheIter<'_> {
        PagerCacheIter { ids: self.pages.borrow().keys().copied().collect(), cache: self }
    }

    /// Libère une entrée du cache
    fn free(&self, id: &PageId) {
      let cell = self.pages.borrow_mut().remove(id).unwrap();
      self.free_list.borrow_mut().push(cell);
    }


    pub fn get(&self, pid: &PageId) -> PagerResult<NonNull<PageCell>> {
        self.try_get(pid).map(|opt| opt.unwrap())
    }
    

    /// Récupère la page si elle est cachée.
    pub fn try_get(&self, pid: &PageId) -> PagerResult<Option<NonNull<PageCell>>> {
        // La page est en cache, on la renvoie
        if let Some(stored) = self.pages.borrow().get(&pid).copied() {
            return Ok(Some(stored));
        }

        // La page a été déchargée, on va essayer de la récupérer.
        if self.stress.contains(pid) {
            let mut ptr = self.reserve(pid)?;
            unsafe {
                let cell: &mut PageCell = ptr.as_mut();
                self.stress.retrieve(pid, cell)?;
            }
        }

        return Ok(None)
    }

    /// Libère de la place :
    /// soit en libérant une entrée du cache contenant une page propre.
    /// soit en déchargeant le stress mémoire quelque part d'autre.
    fn free_some_space(&self) -> PagerResult<NonNull<PageCell>> {
        // On trouve une page propre non empruntée
        if let Some(cleaned) = self.pages.borrow().values().filter(|cell| unsafe {cell.as_ref().dirty  == false && cell.as_ref().rw_counter == 0}).sorted_by_key(|cell| unsafe {cell.as_ref().use_counter}).copied().next() {
            unsafe {
                self.free(&cleaned.as_ref().id);
            }

            return Ok(cleaned)
        }
        // on va décharger une page en mémoire
        if let Some(mut dischargeable) = self.pages.borrow().values().filter(|cell| unsafe {cell.as_ref().rw_counter == 0}).sorted_by_key(|cell| unsafe {cell.as_ref().use_counter}).copied().next() {
            unsafe {
                let cell = dischargeable.as_mut();
                self.stress.discharge(&cell.id, cell)?;
                self.free(&cell.id);
            }

            return Ok(dischargeable);
        }

        return Err(PagerError::CacheFull);
    }

    /// Réserve de l'espace dans le cache pour stocker une page.
    pub fn reserve(&self, pid: &PageId) -> PagerResult<NonNull<PageCell>> {
        // Déjà caché
        if self.pages.borrow().contains_key(pid) {
            return Err(PagerError::PageAlreadyCached)
        }      

        // On a un slot de libre
        if let Some(mut free) = self.free_list.borrow_mut().pop() {
            unsafe {
                let cell = free.as_mut();
                cell.dirty = false;
                cell.use_counter = 0;
                cell.rw_counter = 0;
                self.pages.borrow_mut().insert(*pid, free);
                return Ok(free);
            }
        }

        let new_tail = self.page_size + size_of::<PageCell>();

        // Le cache est plein, on est dans un cas de stress mémoire
        if new_tail >= self.size {
            unsafe {
                let mut ptr = self.free_some_space()?;
                let cell = ptr.as_mut();
                cell.content.as_mut().fill(0);
                cell.id = *pid;
                cell.dirty = false;
                cell.rw_counter = 0;
                cell.use_counter = 0;
                return Ok(ptr)
            }
        }

        // On ajoute une nouvelle entrée dans le cache
        unsafe {
            let ptr = self.ptr.add(*self.tail.borrow());
            let mut cell_ptr = ptr.cast::<MaybeUninit<PageCell>>();
            let content =
                NonNull::slice_from_raw_parts(ptr.add(size_of::<PageCell>()), self.page_size);
            let cell_ptr = NonNull::new_unchecked(cell_ptr.as_mut().write(PageCell {
                id: *pid,
                content,
                new: false,
                dirty: false,
                use_counter: 0,
                rw_counter: 0,
            }));
            self.pages.borrow_mut().insert(*pid, cell_ptr);
            *self.tail.borrow_mut() = new_tail;
            return Ok(cell_ptr);
        }
    }
}

/// Cached page
pub(super) struct PageCell {
    pub id: PageId,
    pub content: NonNull<[u8]>,
    pub dirty: bool,
    pub new: bool,
    pub use_counter: usize,
    pub rw_counter: isize,
}
