use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    borrow::BorrowMut,
    cell::RefCell,
    collections::HashMap,
    fmt::Debug,
    io::Cursor,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use itertools::Itertools;

use super::{
    error::{PagerError, PagerErrorKind},
    page::PageId,
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

        self.alloc_in_memory(pid)
    }

    /// Libère une entrée du cache
    /// L'opération échoue si la page est toujours empruntée.
    pub fn free(&self, id: &PageId) -> PagerResult<()> {
        if let Some(cached) = self.in_memory.borrow_mut().remove(id).map(CachedPage::new) {
            if cached.is_borrowed() {
                return Err(PagerError::new(PagerErrorKind::PageCurrentlyBorrowed));
            }

            unsafe {
                self.free_list.borrow_mut().push(cached.leak());
            }
        }

        Ok(())
    }

    /// Itère les pages cachées.
    ///
    /// L'itération peut échouer si des pages déchargées ne peuvent être récupérées en mémoire.
    /// Voir [Self::free_some_space] pour plus d'explications.
    pub fn iter(&self) -> PagerCacheIter<'_> {
        PagerCacheIter {
            ids: self.in_memory.borrow().keys().copied().collect(),
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
            unsafe {
                free.flags = 0;
                free.use_counter = 0;
                free.rw_counter = 0;
                self.in_memory.borrow_mut().insert(*pid, free.leak());
                return Ok(free);
            }
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
        pcached.borrow_mut_content().fill(0);
        pcached.pid = *pid;
        pcached.flags = 0;
        pcached.rw_counter = 0;
        pcached.use_counter = 0;
        self.in_memory.borrow_mut().insert(*pid, pcached.data);
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
            .filter(|page| !page.is_dirty() && !page.is_borrowed())
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
            .filter(|page| !page.is_borrowed())
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

#[derive(Clone, Copy)]
pub struct CachedPage<'cache> {
    _pht: PhantomData<&'cache ()>,
    data: NonNull<CachedPageData>,
}

impl Debug for CachedPage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedPage")
            .field("data", &self.data)
            .finish()
    }
}

impl CachedPage<'_> {
    pub(super) fn new(data: NonNull<CachedPageData>) -> Self {
        Self {
            _pht: PhantomData,
            data,
        }
    }

    pub unsafe fn leak(self) -> NonNull<CachedPageData> {
        self.data
    }
}

impl DerefMut for CachedPage<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.data.as_mut() }
    }
}

impl Deref for CachedPage<'_> {
    type Target = CachedPageData;

    fn deref(&self) -> &Self::Target {
        unsafe { self.data.as_ref() }
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

    /// Accède aux données modifiables de la page
    ///
    /// Attention cela ne passe pas la page en statut dirty.
    /// Il faut utiliser [Self::open_mut_cursor] avec dirty = true.
    pub fn borrow_mut_content(&mut self) -> &mut [u8] {
        unsafe { self.content.as_mut() }
    }

    /// Ouvre un curseur pour modifier le contenu de la page cachée
    ///
    /// Deux possibilitées :
    /// - modifier la page qui devra être commit (dirty = true) ;
    /// - modifier le contenu de la page sans devoir être commit (dirty = false).
    ///
    /// La seconde possibilité est à réserver au cas de chargement/déchargement de la page de la mémoire.
    pub fn open_mut_cursor(&mut self, dirty: bool) -> Cursor<&mut [u8]> {
        if dirty {
            self.set_dirty();
        }
        Cursor::new(self.borrow_mut_content())
    }

    pub fn open_cursor(&self) -> Cursor<&[u8]> {
        Cursor::new(self.borrow_content())
    }

    pub fn borrow_content(&self) -> &[u8] {
        unsafe { self.content.as_ref() }
    }

    pub fn new(pid: PageId, content: NonNull<[u8]>) -> Self {
        Self {
            pid,
            content,
            flags: 0,
            use_counter: 0,
            rw_counter: 0,
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

    pub fn is_borrowed(&self) -> bool {
        self.rw_counter != 0
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
            buf.write_all(src.borrow_content())?;
            self.0.borrow_mut().insert(src.id(), buf);
            Ok(())
        }

        fn retrieve(&self, dest: &mut super::CachedPage<'_>) -> crate::pager::PagerResult<()> {
            let pid = dest.id();
            println!("récupère {pid}");
            dest.open_mut_cursor(false)
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

        println!("create page n° 100");
        // On va allouer une page
        let mut pcache = cache.alloc(&100).unwrap();
        // l'écriture va passer la page en état dirty
        // ce qui le force à devoir être déchargé de la mémoire.
        pcache
            .open_mut_cursor(true)
            .write_u64::<LittleEndian>(0x1234)?;

        assert!(pcache.is_dirty(), "la page n° 100 doit être dirty");

        // On vérifie que le cache est bien plein et qu'on est en situation
        // de stress mémoire.
        assert!(
            *cache.tail.borrow() >= cache.size,
            "le cache doit être plein"
        );

        println!("create page n° 110");
        // On va allouer une seconde page
        // normalement la taille de cache est insuffisante pour stocker deux pages
        // le cache doit alors décharger la première page.
        let mut pcache = cache.alloc(&110).unwrap();
        pcache
            .open_mut_cursor(true)
            .write_u64::<LittleEndian>(0x5678)?;

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
        let got = pcache.open_cursor().read_u64::<LittleEndian>()?;

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

