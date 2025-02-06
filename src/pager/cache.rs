use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt::Debug,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use itertools::Itertools;

use super::{
    error::{PagerError, PagerErrorKind},
    page::{MutPage, PageId, RefPage},
    stress::{BoxedPagerStress, IPagerStress},
    PagerResult,
};

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
    /// stored
    stored: RefCell<HashSet<PageId>>,
    /// Current cached pages that are in memory.
    in_memory: RefCell<HashMap<PageId, NonNull<CachedPageData>>>,
    /// Stratégie de gestion du stress mémoire
    /// Employé si le cache est plein
    stress: BoxedPagerStress,
}

impl Drop for PagerCache {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_mut(), self.layout);
        }
    }
}

pub(super) struct PagerCacheIter<'cache> {
    ids: Vec<PageId>,
    cache: &'cache PagerCache,
}

impl<'cache> Iterator for PagerCacheIter<'cache> {
    type Item = PagerResult<CachedPage<'cache>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.pop().map(|pid| self.cache.get(&pid))
    }
}

impl PagerCache {
    pub fn new<Ps: IPagerStress + 'static>(
        cache_size: usize,
        page_size: usize,
        stress_strategy: Ps,
    ) -> Self {
        unsafe {
            let layout = Layout::from_size_align(
                cache_size,
                (page_size + size_of::<CachedPageData>()).next_power_of_two(),
            )
            .unwrap();

            let ptr = NonNull::new(alloc_zeroed(layout)).unwrap();

            Self {
                layout,
                ptr,
                size: cache_size,
                tail: RefCell::new(0),
                page_size,
                stored: RefCell::default(),
                free_list: RefCell::default(),
                in_memory: RefCell::default(),
                stress: BoxedPagerStress::new(stress_strategy),
            }
        }
    }

    /// Alloue de l'espace dans le cache pour stocker une page.
    pub fn alloc<'cache>(&'cache self, pid: &PageId) -> PagerResult<CachedPage<'cache>> {
        // Déjà caché
        if self.in_memory.borrow().contains_key(pid) || self.stress.contains(pid) {
            return Err(PagerError::new(PagerErrorKind::PageAlreadyCached(*pid)));
        }

        self.alloc_in_memory(pid).inspect(|_| {
            self.stored.borrow_mut().insert(*pid);
        })
    }

    /// Itère les pages cachées.
    ///
    /// L'itération peut échouer si des pages déchargées ne peuvent être récupérées en mémoire.
    /// Voir [Self::free_some_space] pour plus d'explications.
    pub fn iter(&self) -> PagerCacheIter<'_> {
        PagerCacheIter {
            ids: self.stored.borrow().iter().copied().collect(),
            cache: self,
        }
    }

    /// Récupère la page si elle est cachée, panique si elle n'existe pas.
    pub fn get(&self, pid: &PageId) -> PagerResult<CachedPage> {
        self.try_get(pid)
            .and_then(|opt| opt.ok_or_else(|| PagerError::new(PagerErrorKind::PageNotCached(*pid))))
    }

    /// Récupère la page si elle est cachée.
    ///
    /// L'opération peut échouer si :
    /// - La page n'est pas cachée
    /// - La page est déchargée et aucune place n'a put être trouvée pour la récupérer en mémoire.
    pub fn try_get<'cache>(&'cache self, pid: &PageId) -> PagerResult<Option<CachedPage<'cache>>> {
        // La page est en cache, on la renvoie
        if let Some(stored) = self.in_memory.borrow().get(pid).copied() {
            return Ok(Some(CachedPage::new(stored)));
        }

        // La page a été déchargée, on va essayer de la récupérer.
        if self.stress.contains(pid) {
            let mut pcache = self.alloc_in_memory(pid)?;
            assert_eq!(pcache.id(), *pid);
            self.stress.retrieve(&mut pcache)?;
            return Ok(Some(pcache));
        }

        Ok(None)
    }
}

impl PagerCache {
    /// Alloue de l'espace dans la mémoire du cache pour stocker une page.
    ///
    /// Différent de [Self::alloc] dans le sens où cette fonction ne regarde pas
    /// si la page est cachée déchargée.
    fn alloc_in_memory<'cache>(&'cache self, pid: &PageId) -> PagerResult<CachedPage<'cache>> {
        // Déjà caché
        if self.in_memory.borrow().contains_key(pid) {
            return Err(PagerError::new(PagerErrorKind::PageAlreadyCached(*pid)));
        }

        // On a un slot de libre
        if let Some(mut free) = self.free_list.borrow_mut().pop().map(CachedPage::new) {
            free.flags = 0;
            free.use_counter = 0;
            free.rw_counter = 0;
            free.ref_counter = 1;
            self.in_memory.borrow_mut().insert(*pid, free.ptr);
            return Ok(free);
        }

        let current_tail = *self.tail.borrow();
        let size = size_of::<CachedPageData>() + self.page_size;

        // On ajoute une nouvelle entrée dans le cache comme on a de la place.
        if current_tail + size <= self.size {
            unsafe {
                let ptr = self.ptr.add(current_tail);
                let mut cell_ptr = ptr.cast::<MaybeUninit<CachedPageData>>();
                let content_ptr = ptr.add(size_of::<CachedPageData>());
                let content = NonNull::slice_from_raw_parts(content_ptr, self.page_size);
                let cell_ptr = NonNull::new_unchecked(
                    cell_ptr.as_mut().write(CachedPageData::new(*pid, content)),
                );
                self.in_memory.borrow_mut().insert(*pid, cell_ptr);
                *self.tail.borrow_mut() += size;
                return Ok(CachedPage::new(cell_ptr));
            }
        }

        // Le cache est plein, on est dans un cas de stress mémoire
        // On va essayer de trouver de la place.
        let mut pcached = self.manage_stress()?;
        pcached.pid = *pid;
        pcached.flags = 0;
        pcached.rw_counter = 0;
        pcached.use_counter = 0;
        pcached.ref_counter = 1;
        pcached.borrow_mut(true).fill(0);

        self.in_memory.borrow_mut().insert(*pid, pcached.ptr);
        Ok(pcached)
    }

    /// Libère de la place :
    /// - soit en libérant une entrée du cache contenant une page propre ;
    /// - soit en déchargeant des pages quelque part (voir [IPagerStress]).
    ///
    /// Si aucune page n'est libérable ou déchargeable, principalement car elles sont
    /// toutes empruntées, alors l'opération échoue et retourne l'erreur *CacheFull*.
    fn manage_stress(&self) -> PagerResult<CachedPage<'_>> {
        // On trouve une page propre non empruntée
        let maybe_clean_unborrowed_page = self
            .in_memory
            .borrow()
            .values()
            .copied()
            .map(CachedPage::new)
            .filter(|page| !page.is_dirty() && page.ref_counter <= 1)
            .sorted_by_key(|page| page.use_counter)
            .next();

        if let Some(cleaned) = maybe_clean_unborrowed_page {
            self.in_memory.borrow_mut().remove(&cleaned.id());
            return Ok(cleaned);
        }

        // on trouve une page sale non empruntée qu'on va devoir décharger
        let maybe_dirty_unborrowed_page = self
            .in_memory
            .borrow()
            .values()
            .copied()
            .map(CachedPage::new)
            .filter(|page| page.ref_counter <= 1)
            .sorted_by_key(|page| page.use_counter)
            .next();

        // on va décharger une page en mémoire
        if let Some(dischargeable) = maybe_dirty_unborrowed_page {
            self.stress.discharge(&dischargeable)?;
            self.in_memory.borrow_mut().remove(&dischargeable.id());
            return Ok(dischargeable);
        }

        Err(PagerError::new(PagerErrorKind::CacheFull))
    }
}

pub struct CachedPage<'cache> {
    _pht: PhantomData<&'cache ()>,
    ptr: NonNull<CachedPageData>,
}

impl Clone for CachedPage<'_> {
    fn clone(&self) -> Self {
        Self::new(self.ptr)
    }
}

impl Debug for CachedPage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPage")
            .field("data", &self.ptr)
            .finish()
    }
}

impl Drop for CachedPage<'_> {
    fn drop(&mut self) {
        self.ref_counter -= 1
    }
}

impl CachedPage<'_> {
    pub(super) fn new(data: NonNull<CachedPageData>) -> Self {
        let mut page = Self {
            _pht: PhantomData,
            ptr: data,
        };
        page.ref_counter += 1;
        page
    }

    pub fn borrow(&self) -> RefPage<'_> {
        RefPage::new(self.clone())
    }

    pub fn borrow_mut(&self, dry: bool) -> MutPage<'_> {
        self.try_borrow_mut(dry).expect("page is already borrowed")
    }

    pub fn try_borrow_mut(&self, dry: bool) -> PagerResult<MutPage<'_>> {
        MutPage::try_new_with_options(self.clone(), dry)
    }
}

impl DerefMut for CachedPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.ptr.as_mut() }
    }
}

impl Deref for CachedPage<'_> {
    type Target = CachedPageData;

    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}

/// Page cachée
pub struct CachedPageData {
    pub pid: PageId,
    pub content: NonNull<[u8]>,
    pub flags: u8,
    pub use_counter: usize,
    pub rw_counter: isize,
    pub ref_counter: usize,
}

impl Debug for CachedPageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPageData")
            .field("pid", &self.pid)
            .field("content", &self.content)
            .field("flags", &self.flags)
            .field("use_counter", &self.use_counter)
            .field("rw_counter", &self.rw_counter)
            .finish()
    }
}

impl CachedPageData {
    const DIRTY_FLAGS: u8 = 0b1;
    const NEW_FLAGS: u8 = 0b11;

    pub fn new(pid: PageId, content: NonNull<[u8]>) -> Self {
        Self {
            pid,
            content,
            flags: 0,
            use_counter: 0,
            rw_counter: 0,
            ref_counter: 0,
        }
    }

    pub fn id(&self) -> PageId {
        self.pid
    }

    pub fn clear_flags(&mut self) {
        self.flags = 0;
    }

    pub fn set_dirty(&mut self) {
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

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        collections::HashMap,
        error::Error,
        io::{Cursor, Write},
        ops::Deref,
        rc::Rc,
    };

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

    use crate::pager::{cache::CachedPageData, page::PageId, stress::IPagerStress};

    use super::PagerCache;

    #[derive(Default)]
    /// Bouchon récupérant les décharges du cache
    pub struct StressStub(RefCell<HashMap<PageId, Vec<u8>>>);

    impl IPagerStress for StressStub {
        fn discharge(&self, src: &super::CachedPage<'_>) -> crate::pager::PagerResult<()> {
            println!("décharge {0}", src.id());
            let mut buf = Vec::<u8>::new();
            buf.write_all(src.borrow().deref())?;
            self.0.borrow_mut().insert(src.id(), buf);
            Ok(())
        }

        fn retrieve(&self, dest: &mut super::CachedPage<'_>) -> crate::pager::PagerResult<()> {
            let pid = dest.id();
            println!("récupère {pid}");
            dest.borrow_mut(false)
                .open_mut_cursor()
                .write_all(self.0.borrow().get(&pid).unwrap())?;
            self.0.borrow_mut().remove(&dest.id());
            Ok(())
        }

        fn contains(&self, pid: &PageId) -> bool {
            self.0.borrow().contains_key(pid)
        }
    }

    #[test]
    /// Ce test vise à tester les capacités du cache à gérer le stess mémoire.
    pub fn test_cache_stress() -> Result<(), Box<dyn Error>> {
        let stress = Rc::new(StressStub::default());
        // taille du cache suffisant pour une seule page.
        let single_page_cache_size = size_of::<CachedPageData>() + 4_096;
        let cache = PagerCache::new(single_page_cache_size, 4_096, stress.clone());

        {
            println!("create page n° 100");
            // On va allouer une page
            let pcache = cache.alloc(&100).unwrap();
            // l'écriture va passer la page en état dirty
            // ce qui le force à devoir être déchargé de la mémoire.
            pcache
                .borrow_mut(false)
                .open_mut_cursor()
                .write_u64::<LittleEndian>(0x1234)?;

            assert!(pcache.is_dirty(), "la page n° 100 doit être dirty");
            drop(pcache);
        }

        // On vérifie que le cache est bien plein et qu'on est en situation
        // de stress mémoire.
        assert!(
            *cache.tail.borrow() >= cache.size,
            "le cache doit être plein"
        );

        {
            println!("create page n° 110");
            // On va allouer une seconde page
            // normalement la taille de cache est insuffisante pour stocker deux pages
            // le cache doit alors décharger la première page.
            let pcache = cache.alloc(&110).unwrap();
            pcache
                .borrow_mut(false)
                .open_mut_cursor()
                .write_u64::<LittleEndian>(0x5678)?;
            drop(pcache);
        }

        // On teste que la page 100 a été déchargée correctement.
        assert!(
            stress.contains(&100),
            "la page n° 100 doit être déchargée du cache"
        );

        println!("retrieve page n° 100 in memory");
        assert_eq!(
            Cursor::new(stress.0.borrow().get(&100).unwrap())
                .read_u64::<LittleEndian>()
                .unwrap(),
            0x1234u64,
            "le contenu de la page n° 100 déchargée doit être 0x1234"
        );

        // on va récupérer la page 100 en mémoire
        let pcache = cache.get(&100)?;
        let got = pcache.borrow().open_cursor().read_u64::<LittleEndian>()?;

        assert!(
            !stress.contains(&100),
            "la page n° 100 doit être récupérée de la mémoire"
        );

        assert!(
            stress.contains(&110),
            "la page n° 110 doit être déchargée de la mémoire"
        );

        assert_ne!(got, 0x5678);
        assert_eq!(got, 0x1234);

        Ok(())
    }
}

