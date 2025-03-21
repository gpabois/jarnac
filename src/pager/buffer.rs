use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    cell::RefCell,
    collections::{HashMap, HashSet},
    mem::MaybeUninit,
    ptr::NonNull,
};

use itertools::Itertools;

use crate::{error::{Error, ErrorKind}, result::Result};

use super::{
    page::{descriptor::{PageDescriptor, PageDescriptorInner}, PageId, PageSize},
    stress::{BoxedPagerStress, IPagerStress},
};

/// Trait représentant un tampon à pages.
pub trait IPageBuffer {
    /// Itérateur sur les pages stockées dans le tampon.
    type Iter<'buffer>: Iterator<Item=Result<PageDescriptor<'buffer>>> where Self: 'buffer;

    /// Alloue de l'espace dans le tampon pour stocker une page.
    fn alloc<'buffer>(&'buffer self, pid: &PageId) -> Result<PageDescriptor<'buffer>>;

    /// Récupère la page stockée dans le tampon s'il existe.
    fn try_get<'buffer>(&'buffer self, pid: &PageId) -> Result<Option<PageDescriptor<'buffer>>>;

    /// Récupère une page stockée dans le tampon.
    /// 
    /// Panique si la page n'est pas stockée dans le tampon.
    fn get<'buffer>(&'buffer self, pid: &PageId) -> Result<PageDescriptor<'buffer>> {
        self.try_get(&pid)
            .and_then(|opt| opt.ok_or_else(|| Error::new(ErrorKind::PageNotCached(*pid))))    
    }

    /// Itère sur les pages stockées dans le tampon.
    fn iter(&self) -> Self::Iter<'_>;
}

pub struct PageBuffer {
    /// Le layout de l'espace allouée
    layout: Layout,
    /// L'espace allouée
    ptr: NonNull<u8>,
    /// La taille du tampon en octets.
    size: usize,
    /// Le bout de l'espace des pages allouées
    tail: RefCell<usize>,
    /// Taille d'une page
    page_size: PageSize,
    /// Emplacements libres dans l'espace des pages allouées.
    free_list: RefCell<Vec<NonNull<PageDescriptorInner>>>,
    /// Ensemble des pages stockées dans le tampon
    stored: RefCell<HashSet<PageId>>,
    /// Ensemble des pages stockées dans le tampon et chargées en mémoire.
    in_memory: RefCell<HashMap<PageId, NonNull<PageDescriptorInner>>>,
    /// Stratégie de gestion du stress mémoire
    /// Employé si le tampon est plein
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

    fn alloc<'buffer>(&'buffer self, pid: &PageId) -> Result<PageDescriptor<'buffer>> {
        // Déjà caché
        if self.contains(pid) {
            return Err(Error::new(ErrorKind::PageAlreadyCached(*pid)));
        }

        self.alloc_in_memory(&pid)
        .inspect(|_| {
            self.stored.borrow_mut().insert(*pid);
        })
    }

    fn try_get<'buffer>(&'buffer self, pid: &PageId) -> Result<Option<PageDescriptor<'buffer>>> {
        unsafe {
            // La page est en mémoire, on la renvoie
            if let Some(stored) = self.try_get_from_memory(pid) {
                return Ok(Some(PageDescriptor::new(stored)));
            }

            // La page a été déchargée, on va essayer de la récupérer.
            if self.stress.contains(pid) {
                let mut pcache = self.alloc_in_memory(pid)?;
                assert_eq!(pcache.id(), pid);
                self.stress.retrieve(&mut pcache)?;
                return Ok(Some(pcache));
            }

            if self.contains(pid) {
                panic!("page {pid} is stored in the buffer but nowhere to be found");
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
        buffer_size: usize,
        page_size: PageSize,
        stress_strategy: Ps,
    ) -> Self {
        unsafe {

            let align: usize = (page_size + size_of::<PageDescriptorInner>()).next_power_of_two();
            
            let layout = Layout::from_size_align(
                buffer_size,
                align,
            )
            .unwrap();

            let ptr = NonNull::new(alloc_zeroed(layout)).unwrap();

            Self {
                layout,
                ptr,
                size: buffer_size,
                tail: RefCell::new(0),
                page_size,
                stored: RefCell::default(),
                free_list: RefCell::default(),
                in_memory: RefCell::default(),
                stress: BoxedPagerStress::new(stress_strategy),
            }
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.stored.borrow().len()
    }

    pub fn contains(&self, pid: &PageId) -> bool {
        self.stored.borrow().contains(pid)
    }
}

impl PageBuffer {
    /// Alloue de l'espace dans la mémoire du cache pour stocker une page.
    ///
    /// Différent de [Self::alloc] dans le sens où cette fonction ne regarde pas
    /// si la page est cachée déchargée.
    fn alloc_in_memory<'cache>(&'cache self, pid: &PageId) -> Result<PageDescriptor<'cache>> {
        unsafe {
            // Déjà caché
            if self.is_in_memory(pid) {
                return Err(Error::new(ErrorKind::PageAlreadyCached(*pid)));
            }

            // On a un slot de libre
            if let Some(free) = self.pop_free(pid) {
                self.add_in_memory(free.get_raw_ptr());
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
                    cell_ptr
                        .as_mut()
                        .write(PageDescriptorInner::new(*pid, content)),
                );
                *self.tail.borrow_mut() += size;
                let desc = PageDescriptor::new(cell_ptr);
                self.add_in_memory(desc.get_raw_ptr());
                return Ok(desc);
            }

            // Le cache est plein, on est dans un cas de stress mémoire
            // On va essayer de trouver de la place.
            let mut pcached = self.manage_stress()?;
            pcached.initialise(*pid);
            self.add_in_memory(pcached.get_raw_ptr());
            
            Ok(pcached)
        }
    }

    fn try_get_from_memory(&self, pid: &PageId) -> Option<NonNull<PageDescriptorInner>> {
        self.in_memory.borrow().get(pid).copied()
    }


    /// Récupère un emplacement libre
    unsafe fn pop_free(&self, pid: &PageId) -> Option<PageDescriptor<'_>> {
        self.free_list.borrow_mut().pop()
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
    fn manage_stress(&self) -> Result<PageDescriptor<'_>> {
        // On trouve une page propre non empruntée
        let maybe_clean_unborrowed_page = self.in_memory.borrow().values().copied()
            .map(|ptr| unsafe { PageDescriptor::new(ptr) })
            .filter(|page| !page.is_dirty() && page.get_ref_counter() <= 1)
            .sorted_by_key(|page| page.get_use_counter())
            .next();

        if let Some(cleaned) = maybe_clean_unborrowed_page {
            unsafe {
                self.remove_from_memory(cleaned.get_raw_ptr());
            }
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
            unsafe {
                self.remove_from_memory(dischargeable.get_raw_ptr());
            }
            return Ok(dischargeable);
        }

        Err(Error::new(ErrorKind::CacheFull))
    }

    fn is_in_memory(&self, pid: &PageId) -> bool {
        self.in_memory.borrow().contains_key(pid)
    }

    fn add_in_memory(&self, desc: NonNull<PageDescriptorInner>) {
        unsafe {
            self.in_memory.borrow_mut().insert(desc.as_ref().pid, desc);
        }
    }

    unsafe fn remove_from_memory(&self, desc: NonNull<PageDescriptorInner>) {
        unsafe {
            self.in_memory.borrow_mut().remove(&desc.as_ref().pid);
        }
    }
}


pub struct PageBufferIter<'buffer> {
    ids: Vec<PageId>,
    cache: &'buffer PageBuffer,
}

impl<'cache> Iterator for PageBufferIter<'cache> {
    type Item = Result<PageDescriptor<'cache>>;

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
        rc::Rc,
    };

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt, LE};

    use crate::{pager::{buffer::{IPageBuffer, PageDescriptorInner}, page::{AsMutPageSlice, AsRefPageSlice, PageId, PageSize}, stress::IPagerStress}, result::Result};

    use super::PageBuffer;

    #[derive(Default)]
    /// Bouchon récupérant les décharges du cache
    pub struct StressStub(RefCell<HashMap<PageId, Vec<u8>>>);

    impl IPagerStress for StressStub {
        fn discharge(&self, src: &super::PageDescriptor<'_>) -> Result<()> {
            let mut buf = Vec::<u8>::new();
            buf.write_all(src.borrow().as_bytes())?;
            //println!("décharge {0} {buf:?}", src.id());
            self.0.borrow_mut().insert(*src.id(), buf);
            Ok(())
        }

        fn retrieve(&self, dest: &mut super::PageDescriptor<'_>) -> Result<()> {
            let pid = dest.id();
            let mut space = self.0.borrow_mut();
            let buf = space.get(&pid).unwrap();
            //println!("récupère {pid} {buf:?}");
            dest.borrow_mut(false)
                .as_mut_bytes()
                .write_all(buf)?;

            space.remove(&dest.id());
            Ok(())
        }

        fn contains(&self, pid: &PageId) -> bool {
            self.0.borrow().contains_key(pid)
        }
    }

    #[test]
    /// Ce test vise à tester les capacités du cache à gérer le stess mémoire.
    pub fn test_cache_stress() -> std::result::Result<(), Box<dyn Error>> {
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

    #[test]
    fn test_buffer_stress() {
        let ps = PageSize::new(8);
        let bs =  10 * 8;
        let buffer = PageBuffer::new(bs, ps, StressStub::default());

        for i in 0..200_000 {
            let pid = PageId::new(i + 1);
            let desc = buffer.alloc(&pid).unwrap();
            desc.borrow_mut(false).as_mut_bytes().write_u64::<LE>(i).unwrap();
        }

        for i in 0..200_000 {
            let pid = PageId::new(i + 1);
            let desc = buffer.try_get(&pid).unwrap().unwrap();
            let v = desc.borrow().as_bytes().read_u64::<LE>().unwrap();
            assert_eq!(v, i, "{}", pid);
        }
    }
}
