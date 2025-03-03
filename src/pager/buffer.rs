use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cell::RefCell,
    collections::{HashMap, HashSet},
    mem::MaybeUninit,
    ptr::NonNull,
};

use itertools::Itertools;

use super::{
    error::{PagerError, PagerErrorKind},
    page::{descriptor::{PageDescriptor, PageDescriptorInner}, PageId, PageSize},
    stress::{BoxedPagerStress, IPagerStress},
    PagerResult,
};

/// Trait représentant un tampon à pages.
pub trait IPageBuffer {
    /// Itérateur sur les pages stockées dans le tampon.
    type Iter<'buffer>: Iterator<Item=PagerResult<PageDescriptor<'buffer>>> where Self: 'buffer;

    /// Alloue de l'espace dans le tampon pour stocker une page.
    fn alloc<'buffer>(&'buffer self, pid: &PageId) -> PagerResult<PageDescriptor<'buffer>>;

    /// Récupère la page stockée dans le tampon s'il existe.
    fn try_get<'buffer>(&'buffer self, pid: &PageId) -> PagerResult<Option<PageDescriptor<'buffer>>>;

    /// Récupère une page stockée dans le tampon.
    /// 
    /// Panique si la page n'est pas stockée dans le tampon.
    fn get<'buffer>(&'buffer self, pid: &PageId) -> PagerResult<PageDescriptor<'buffer>> {
        self.try_get(&pid)
            .and_then(|opt| opt.ok_or_else(|| PagerError::new(PagerErrorKind::PageNotCached(*pid))))    
    }

    /// Itère sur les pages stockées dans le tampon.
    fn iter(&self) -> Self::Iter<'_>;
}

pub struct PageBuffer {
    /// The memory layout of the allocated space
    layout: Layout,
    /// The allocated buffer space
    ptr: NonNull<u8>,
    /// The size of the buffer
    size: usize,
    /// The tail of allocated space
    tail: RefCell<usize>,
    /// The size of a page
    page_size: PageSize,
    /// Free page cells
    free_list: RefCell<Vec<NonNull<PageDescriptorInner>>>,
    /// Stored page cells
    stored: RefCell<HashSet<PageId>>,
    /// Current cached pages that are in memory.
    in_memory: RefCell<HashMap<PageId, NonNull<PageDescriptorInner>>>,
    /// Stratégie de gestion du stress mémoire
    /// Employé si le cache est plein
    stress: BoxedPagerStress,
}

impl Drop for PageBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_mut(), self.layout);
        }
    }
}

impl IPageBuffer for PageBuffer {
    type Iter<'buffer> = PageBufferIter<'buffer> where Self: 'buffer;

    fn alloc<'buffer>(&'buffer self, pid: &PageId) -> PagerResult<PageDescriptor<'buffer>> {
        // Déjà caché
        if self.in_memory.borrow().contains_key(&pid) || self.stress.contains(&pid) {
            return Err(PagerError::new(PagerErrorKind::PageAlreadyCached(*pid)));
        }

        self.alloc_in_memory(&pid).inspect(|_| {
            self.stored.borrow_mut().insert(*pid);
        })
    }

    fn try_get<'buffer>(&'buffer self, pid: &PageId) -> PagerResult<Option<PageDescriptor<'buffer>>> {
        unsafe {
            // La page est en cache, on la renvoie
            if let Some(stored) = self.in_memory.borrow().get(pid).copied() {
                return Ok(Some(PageDescriptor::new(stored)));
            }

            // La page a été déchargée, on va essayer de la récupérer.
            if self.stress.contains(pid) {
                let mut pcache = self.alloc_in_memory(pid)?;
                assert_eq!(pcache.id(), pid);
                self.stress.retrieve(&mut pcache)?;
                return Ok(Some(pcache));
            }

            Ok(None)
        }
    }

    fn iter(&self) -> Self::Iter<'_> {
        PageBufferIter {
            ids: self.stored.borrow().iter().copied().collect(),
            cache: self,
        }
    }
}

impl PageBuffer {
    pub fn new<Ps: IPagerStress + 'static>(
        cache_size: usize,
        page_size: PageSize,
        stress_strategy: Ps,
    ) -> Self {
        unsafe {

            let align: usize = (page_size + size_of::<PageDescriptorInner>()).next_power_of_two();
            
            let layout = Layout::from_size_align(
                cache_size,
                align,
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
}

impl PageBuffer {
    /// Alloue de l'espace dans la mémoire du cache pour stocker une page.
    ///
    /// Différent de [Self::alloc] dans le sens où cette fonction ne regarde pas
    /// si la page est cachée déchargée.
    fn alloc_in_memory<'cache>(&'cache self, pid: &PageId) -> PagerResult<PageDescriptor<'cache>> {
        unsafe {
            // Déjà caché
            if self.in_memory.borrow().contains_key(pid) {
                return Err(PagerError::new(PagerErrorKind::PageAlreadyCached(*pid)));
            }

            // On a un slot de libre
            if let Some(free) = self.pop_free(pid) {
                self.in_memory.borrow_mut().insert(*pid, free.get_raw_ptr());
                return Ok(free);
            }

            let current_tail = *self.tail.borrow();
            let size =  self.page_size + size_of::<PageDescriptorInner>();

            // On ajoute une nouvelle entrée dans le cache comme on a de la place.
            if current_tail + size <= self.size {
                let ptr = self.ptr.add(current_tail);
                let mut cell_ptr = ptr.cast::<MaybeUninit<PageDescriptorInner>>();
                let content_ptr = ptr.add(size_of::<PageDescriptorInner>());
                let content = std::mem::transmute(NonNull::slice_from_raw_parts(content_ptr, self.page_size.into()));
                let cell_ptr = NonNull::new_unchecked(
                    cell_ptr.as_mut().write(PageDescriptorInner::new(*pid, content)),
                );
                self.in_memory.borrow_mut().insert(*pid, cell_ptr);
                *self.tail.borrow_mut() += size;
                return Ok(PageDescriptor::new(cell_ptr));
            }

            // Le cache est plein, on est dans un cas de stress mémoire
            // On va essayer de trouver de la place.
            let mut pcached = self.manage_stress()?;
            pcached.initialise(*pid);
            self.in_memory.borrow_mut().insert(*pid, pcached.get_raw_ptr());
            
            Ok(pcached)
        }
    }

    /// Récupère un emplacement libre
    unsafe fn pop_free(&self, pid: &PageId) -> Option<PageDescriptor<'_>> {
        self
            .free_list.borrow_mut()
            .pop()
            .map(|ptr| PageDescriptor::new(ptr) )
            .map(|mut page| {
                page.initialise(*pid);
                page
            })
    }

    /// Libère de la place :
    /// - soit en libérant une entrée du cache contenant une page propre ;
    /// - soit en déchargeant des pages quelque part (voir [IPagerStress]).
    ///
    /// Si aucune page n'est libérable ou déchargeable, principalement car elles sont
    /// toutes empruntées, alors l'opération échoue et retourne l'erreur *CacheFull*.
    fn manage_stress(&self) -> PagerResult<PageDescriptor<'_>> {
        // On trouve une page propre non empruntée
        let maybe_clean_unborrowed_page = self
            .in_memory
            .borrow()
            .values()
            .copied()
            .map(|ptr| unsafe { PageDescriptor::new(ptr) })
            .filter(|page| !page.is_dirty() && page.get_ref_counter() <= 1)
            .sorted_by_key(|page| page.get_use_counter())
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
            .map(|ptr| unsafe {PageDescriptor::new(ptr)})
            .filter(|page| page.get_ref_counter() <= 1)
            .sorted_by_key(|page| page.get_use_counter())
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


pub struct PageBufferIter<'buffer> {
    ids: Vec<PageId>,
    cache: &'buffer PageBuffer,
}

impl<'cache> Iterator for PageBufferIter<'cache> {
    type Item = PagerResult<PageDescriptor<'cache>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ids.pop().map(|pid| self.cache.get(&pid))
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

    use crate::pager::{buffer::{IPageBuffer, PageDescriptorInner}, page::{PageId, PageSize}, stress::IPagerStress};

    use super::PageBuffer;

    #[derive(Default)]
    /// Bouchon récupérant les décharges du cache
    pub struct StressStub(RefCell<HashMap<PageId, Vec<u8>>>);

    impl IPagerStress for StressStub {
        fn discharge(&self, src: &super::PageDescriptor<'_>) -> crate::pager::PagerResult<()> {
            println!("décharge {0}", src.id());
            let mut buf = Vec::<u8>::new();
            buf.write_all(src.borrow().deref())?;
            self.0.borrow_mut().insert(*src.id(), buf);
            Ok(())
        }

        fn retrieve(&self, dest: &mut super::PageDescriptor<'_>) -> crate::pager::PagerResult<()> {
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
        let single_page_cache_size = size_of::<PageDescriptorInner>() + 4_096;
        let cache = PageBuffer::new(single_page_cache_size, PageSize::new(4_096), stress.clone());

        {
            println!("create page n° 100");
            // On va allouer une page
            let pcache = cache.alloc(&PageId::from(100u64)).unwrap();
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
            let pcache = cache.alloc(&PageId::from(110u64)).unwrap();
            pcache
                .borrow_mut(false)
                .open_mut_cursor()
                .write_u64::<LittleEndian>(0x5678)?;
            drop(pcache);
        }

        // On teste que la page 100 a été déchargée correctement.
        assert!(
            stress.contains(&PageId::from(100u64)),
            "la page n° 100 doit être déchargée du cache"
        );

        println!("retrieve page n° 100 in memory");
        assert_eq!(
            Cursor::new(stress.0.borrow().get(&PageId::from(100u64)).unwrap())
                .read_u64::<LittleEndian>()
                .unwrap(),
            0x1234u64,
            "le contenu de la page n° 100 déchargée doit être 0x1234"
        );

        // on va récupérer la page 100 en mémoire
        let pcache = cache.get(&PageId::new(100))?;
        let got = pcache.borrow().open_cursor().read_u64::<LittleEndian>()?;

        assert!(
            !stress.contains(&PageId::from(100u64)),
            "la page n° 100 doit être récupérée de la mémoire"
        );

        assert!(
            stress.contains(&PageId::from(110u64)),
            "la page n° 110 doit être déchargée de la mémoire"
        );

        assert_ne!(got, 0x5678);
        assert_eq!(got, 0x1234);

        Ok(())
    }
}

