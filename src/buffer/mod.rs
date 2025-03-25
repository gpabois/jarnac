pub mod stress;

use std::{
    alloc::{alloc_zeroed, dealloc, Layout},
    mem::MaybeUninit,
    ptr::NonNull, sync::{atomic::{AtomicUsize, Ordering}, Arc, Mutex},
};

use dashmap::{DashMap, DashSet};
use itertools::Itertools;
use stress::BufferStressStrategy;

use crate::{
    error::{Error, ErrorKind}, 
    pager::page::{descriptor::{PageDescriptor, PageDescriptorInner, PageDescriptorPtr}, MutPage, PageSize, RefPage}, 
    result::Result, 
    tag::JarTag, utils::Flip
};

pub trait IBufferPool {
    /// Alloue de l'espace pour tamponner une page.
    fn alloc<'buf>(&'buf self, tag: &JarTag) -> Result<MutPage<'buf>>;

    /// Récupère une référence vers la page si elle est stockée dans le tampon.
    fn try_get_ref<'buf>(&'buf self, tag: &JarTag) -> Result<Option<RefPage<'buf>>> {
        unsafe {
            let desc = self.try_get_descriptor(tag)?;
            desc.map(|desc| RefPage::try_new(desc)).flip()
        }
    }

    /// Récupère une référence mutable vers la page si elle est stockée dans le tampon.
    fn try_get_mut<'buf>(&'buf self, tag: &JarTag) -> Result<Option<MutPage<'buf>>> {
        unsafe {
            let desc = self.try_get_descriptor(tag)?;
            desc.map(|desc| MutPage::try_new(desc)).flip()
        }
    }

    /// Essaye de récupérer une page stocker dans le tampon.
    unsafe fn try_get_descriptor<'buf>(&'buf self, tag: &JarTag) -> Result<Option<PageDescriptor<'buf>>>;
}

#[derive(Clone)]
pub struct SharedBufferPool(Arc<BufferPool>);

impl IBufferPool for SharedBufferPool {
    fn alloc<'buf>(&'buf self, tag: &JarTag) -> Result<MutPage<'buf>> {
        self.0.alloc(tag)
    }

    unsafe fn try_get_descriptor<'buf>(&'buf self, tag: &JarTag) -> Result<Option<PageDescriptor<'buf>>> {
        self.0.try_get_descriptor(tag)
    }
}

pub struct BufferPool {
    /// Le layout de l'espace allouée
    layout: Layout,
    /// L'espace allouée
    ptr: NonNull<u8>,
    /// La taille du tampon en octets.
    size: usize,
    /// The number of pages stored
    length: AtomicUsize,
    /// Le bout de l'espace des pages allouées
    tail: AtomicUsize,
    /// Taille d'une page
    page_size: PageSize,
    /// Emplacements libres dans l'espace des pages allouées.
    freelist: Mutex<Vec<PageDescriptorPtr>>,
    /// Ensemble des pages stockées dans le tampon
    stored: DashSet<JarTag>,
    /// Ensemble des pages stockées dans le tampon et chargées en mémoire.
    in_memory: DashMap<JarTag, PageDescriptorPtr>,
    /// Stratégie de gestion du stress mémoire
    /// Employé si le tampon est plein
    stress_strategy: BufferStressStrategy,
}

impl BufferPool {
    /// Crée un nouveau buffer pool.
    pub fn new(buffer_size: usize, page_size: PageSize, stress_strategy: BufferStressStrategy) -> Self {
        unsafe {

            let align: usize = (usize::from(page_size) + size_of::<PageDescriptorInner>()).next_power_of_two();
            
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
                length: Default::default(),
                tail: Default::default(),
                page_size,
                stored: Default::default(),
                freelist: Default::default(),
                in_memory: Default::default(),
                stress_strategy
            }
        }
    }

    pub fn len(&self) -> usize {
        self.stored.len()
    }

    pub fn contains(&self, tag: &JarTag) -> bool {
        self.stored.contains(tag)
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_mut(), self.layout);
        }
    }
}

impl IBufferPool for BufferPool {

    fn alloc<'buf>(&'buf self, tag: &JarTag) -> Result<MutPage<'buf>> {
        // Déjà caché
        if self.contains(tag) {
            return Err(Error::new(ErrorKind::PageAlreadyCached(*tag)));
        }
    
        self
            .alloc_from_memory(tag)
            .inspect(|_| {self.stored.insert(*tag);})
            .and_then(MutPage::try_new)

    }

    /// Essaye de récupérer une page stocker dans le tampon.
    unsafe fn try_get_descriptor<'buf>(&'buf self, tag: &JarTag) -> Result<Option<PageDescriptor<'buf>>> {
        // La page est en mémoire, on la renvoie
        if let Some(stored) = self.try_get_from_memory(tag) {
            return Ok(Some(PageDescriptor::new(stored)));
        }


        // La page a été déchargée, on va essayer de la récupérer.
        if self.stress_strategy.contains(tag) {
            let mut page = self.alloc_from_memory(tag)?;
            assert_eq!(page.tag(), tag);
            self.stress_strategy.retrieve(&mut page)?;
            return Ok(Some(page));
        }


        if self.contains(tag) {
            panic!("page #{tag} is stored in the buffer but nowhere to be found");
        }

        Ok(None)
    }
}


impl BufferPool {
    /// Alloue de l'espace dans la mémoire du cache pour stocker une page.
    ///
    /// Différent de [Self::alloc] dans le sens où cette fonction ne regarde pas
    /// si la page est cachée déchargée.
    fn alloc_from_memory<'buf>(&'buf self, tag: &JarTag) -> Result<PageDescriptor<'buf>> {
        unsafe {
            // Déjà caché
            if self.is_in_memory(tag) {
                return Err(Error::new(ErrorKind::PageAlreadyCached(*tag)));
            }

            // On a un slot de libre
            if let Some(free) = self.pop_free(tag) {
                self.add_in_memory(free.get_raw_ptr());
                return Ok(free);
            }

            
            let res = self.alloc_in_heap(tag);
            
            if let Err(ErrorKind::BufferFull) = res.as_ref().map_err(|err| &err.kind) {
                // Le cache est plein, on est dans un cas de stress mémoire
                // On va essayer de trouver de la place.
                let mut page = self.manage_stress()?;
                page.initialise(*tag);
                self.add_in_memory(page.get_raw_ptr());
                return Ok(page)
            }

            res
        }
    }

    /// Alloue sur le tas restant un emplacement pour stocker une page.
    /// 
    /// Cette fonction doit supposément être thread-safe via l'utilisation
    /// de compteurs atomiques.
    fn alloc_in_heap<'buf>(&'buf self, tag: &JarTag) -> Result<PageDescriptor<'buf>> {
        let size =  usize::from(self.page_size) + size_of::<PageDescriptorInner>();
        
        loop {
            let tail = self.tail.load(std::sync::atomic::Ordering::Acquire);
            let current_tail = self.tail.fetch_add(size, std::sync::atomic::Ordering::Release);

            if current_tail >= self.size {
                return Err(Error::new(ErrorKind::BufferFull));
            }

            // On est bon
            if tail == current_tail {
                if tail >= self.size {
                    return Err(Error::new(ErrorKind::BufferFull));
                }
                let new_tail = tail + size;
                unsafe {
                    let buf_id = self.length.fetch_add(1, Ordering::Release);
                    let ptr = self.ptr.add(new_tail);        
                    let mut cell_ptr = ptr.cast::<MaybeUninit<PageDescriptorInner>>();
                    let content_ptr = ptr.add(size_of::<PageDescriptorInner>());
                    let content = std::mem::transmute(NonNull::slice_from_raw_parts(content_ptr, self.page_size.into()));
                    let ptr = NonNull::new_unchecked(
                        cell_ptr
                            .as_mut()
                            .write(PageDescriptorInner::new(buf_id, *tag, content)),
                    );

                    return Ok(PageDescriptor::new(ptr));      
                }
            }
        }

    }

    /// Récupère un pointeur vers un tampon d'une page donnée, s'il existe.
    fn try_get_from_memory(&self, tag: &JarTag) -> Option<PageDescriptorPtr> {
        self.in_memory.get(tag).map(|kv| kv.value().to_owned())
    }


    /// Récupère un emplacement libre
    unsafe fn pop_free(&self, tag: &JarTag) -> Option<PageDescriptor<'_>> {
        self.freelist
            .lock()
            .unwrap()
            .pop()
            .map(|ptr| PageDescriptor::new(ptr) )
            .map(|mut page| {
                page.initialise(*tag);
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
        let maybe_clean_unborrowed_page = self.in_memory.iter()
            .map(|kv| kv.value().to_owned())
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
            .iter()
            .map(|kv| kv.value().to_owned())
            .map(|ptr| unsafe {PageDescriptor::new(ptr)})
            .filter(|page| page.get_ref_counter() <= 1)
            .sorted_by_key(|page| page.get_use_counter())
            .next();

        // on va décharger une page en mémoire
        if let Some(dischargeable) = maybe_dirty_unborrowed_page {
            self.stress_strategy.discharge(&dischargeable)?;
            unsafe {
                self.remove_from_memory(dischargeable.get_raw_ptr());
            }
            return Ok(dischargeable);
        }

        Err(Error::new(ErrorKind::BufferFull))
    }

    fn is_in_memory(&self, tag: &JarTag) -> bool {
        self.in_memory.contains_key(tag)
    }

    fn add_in_memory(&self, desc: NonNull<PageDescriptorInner>) {
        unsafe {
            self.in_memory.insert(desc.as_ref().tag, desc);
        }
    }

    unsafe fn remove_from_memory(&self, desc: NonNull<PageDescriptorInner>) {
        unsafe {
            self.in_memory.remove(&desc.as_ref().tag);
        }
    }
}


#[cfg(test)]
mod tests {

}
