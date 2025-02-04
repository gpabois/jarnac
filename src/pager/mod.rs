use std::{
    alloc::{alloc, dealloc, Layout}, cell::{Ref, RefCell}, io::{self, Cursor, Read, Seek, Write}, ops::Deref, ptr::NonNull
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use cache::{PageCell, PagerCache};
use free::{pop_free_page, push_free_page};
use page::{MutPage, PageId, PageLocation, RefPage};
use stress::FsPagerStress;
use transaction::PagerTransaction;

use crate::vfs::IFileSystem;

pub mod page;
pub mod error;
pub mod overflow;
mod logs;
mod free;
mod transaction;
mod cache;
mod stress;

use error::PagerError;

pub const MAGIC_NUMBER: u16 = 0xD334;

/// Taille par défaut du cache du pager (5 MB).
pub const DEFAULT_PAGER_CACHE_SIZE: usize = 5_000_000;

/// Localisation du pager header.
pub const PAGER_HEADER_LOC: u64 = 2;

/// Taille de l'entête du pager (en bytes).
pub const PAGER_HEADER_SIZE: u64 = 100;

/// Localisation de la première page
pub const PAGER_PAGES_BASE: u64 = PAGER_HEADER_LOC + PAGER_HEADER_SIZE;

pub type PagerResult<T> = Result<T, error::PagerError>;

/// Trait pour accéder aux fonctions internes du pager
/// Ce trait n'est pas exposé en dehors du module.
pub(self) trait IPagerInternals: IPager {
    /// Change la tête de la liste des pages libres.
    fn set_free_head(&self, head: Option<PageId>);
    /// Retourne la tête de la liste des pages libres.
    fn free_head(&self) -> Option<PageId>;
    /// Retourne la localisation de la page sur le disque.
    fn page_location(&self, pid: &PageId) -> PageLocation;
    /// Retourne le nombre de pages 
    fn page_count(&self) -> u64;
    /// Retourne la taille d'une page
    fn page_size(&self) -> usize;
    /// Ecrit l'entête du fichier dans le flux
    fn write_header<W: Write + Seek>(&self, stream: &mut W) -> io::Result<()>;
}

pub trait IPager {
    /// Crée une nouvelle page.
    fn new_page(&self) -> PagerResult<MutPage<'_>>;
    /// Supprime une page
    fn delete_page(&self, id: &PageId) -> PagerResult<()>;
    /// Récupère une page existante.
    fn get_page<'pager>(&'pager self, id: &PageId) -> PagerResult<RefPage<'pager>>;
    /// Récupère une page modifiable existante.
    fn get_mut_page<'pager>(&'pager self, id: &PageId) -> PagerResult<MutPage<'pager>>;
    /// Commit les pages modifiées.
    fn commit(&self) -> PagerResult<()>;
}

#[derive(Default)]
pub struct PagerOptions {
    cache_size: Option<usize>,
}

impl PagerOptions {
    pub fn set_cache_size(mut self, cache_size: usize) -> Self {
        self.cache_size = Some(cache_size);
        self
    }
}

pub struct PagerHeader {
    /// Taille d'une page
    pub(super) page_size: usize,
    /// Nombre de pages stockées dans le pager
    pub(super) page_count: u64,
    /// Début de la liste chaînée des pages libres
    pub(super) free_head: Option<PageId>,  
    /// Données de l'entête du pager.
    data: NonNull<[u8]>,
    layout: Layout
}

impl Drop for PagerHeader {
    fn drop(&mut self) {
        unsafe  {
            dealloc(self.data.cast::<u8>().as_ptr(), self.layout);
        }
    }
}

impl PagerHeader {
    pub fn new(page_size: usize) -> Self {
        unsafe {
            let layout = Layout::array::<u8>(PAGER_HEADER_SIZE.try_into().unwrap()).unwrap();
            let data = NonNull::slice_from_raw_parts(
                NonNull::new(alloc(layout)).unwrap(),
                PAGER_HEADER_SIZE.try_into().unwrap()
            );

            let header = Self {
                page_size,
                page_count: 0,
                free_head: None,
                layout,
                data
            };

            header.initialise();

            header
        }
    }
}

impl Deref for PagerHeader {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.data.as_ref()
        }
    }
}

impl PagerHeader {
    pub const PAGE_SIZE_LOC: u64 = 0;
    pub const PAGE_COUNT_LOC: u64 = 8;
    pub const PAGE_FREE_HEAD_LOC: u64 = 16;

    /// Ouvre un curseur sur les données de l'entête.
    fn cursor(&self) -> Cursor<&mut [u8]> {
        unsafe {
            let mut_data = self.data.as_ptr().as_mut().unwrap();
            Cursor::new(mut_data)
        }
    }

    /// Initialise une nouvelle entête
    fn initialise(&self) {
        unsafe {
            let mut_data = self.data.as_ptr().as_mut().unwrap();
            mut_data.fill(0);

            let mut cursor = self.cursor();
            cursor.write_u64::<LittleEndian>(self.page_size.try_into().unwrap()).unwrap();
        }
    }

    /// Réhydrate les attributs de l'entête depuis les données brutes
    fn hydrate(&mut self) -> io::Result<()> {
        let mut cursor = self.cursor();
        let page_size: usize = cursor.read_u64::<LittleEndian>()?.try_into().unwrap();
        let page_count = cursor.read_u64::<LittleEndian>()?;

        let free_head = if cursor.read_u8()? == 1 {
            Some(cursor.read_u64::<LittleEndian>()?)
        } else {
            cursor.read_u64::<LittleEndian>()?;
            None
        };     

        self.page_count = page_count;
        self.page_size = page_size;
        self.free_head = free_head;

        Ok(())
    }

    pub fn set_free_head(&mut self, head: Option<PageId>) {
        let mut cursor = self.cursor();
        cursor.seek(io::SeekFrom::Start(Self::PAGE_FREE_HEAD_LOC)).unwrap();

        if let Some(free_head) = head {
            cursor.write_u8(1).unwrap();
            cursor.write_u64::<LittleEndian>(free_head).unwrap();
        } else {
            cursor.write_u8(0).unwrap();
            cursor.write_u64::<LittleEndian>(0).unwrap();
        };

        self.free_head = head;
    }

    pub fn page_location(&self, pid: &PageId) -> PageLocation {
        let ps: u64 = self.page_size.try_into().unwrap();
        PAGER_HEADER_SIZE + (ps * (*pid))
    }
    
    /// Ecris l'entête dans un fichier paginé
    pub fn write<W: Write + Seek>(&self, stream: &mut W) -> io::Result<()> {
        unsafe {
            let data = self.data.as_ref();
            stream.seek(io::SeekFrom::Start(PAGER_HEADER_LOC))?;
            stream.write_all(&data)?;
    
            Ok(())
        }
    }

    /// Lit l'entête depuis un fichier paginé.
    pub fn read<R: Read + Seek>(stream: &mut R) -> io::Result<Self> {
        unsafe {
            let layout = Layout::array::<u8>(PAGER_HEADER_SIZE.try_into().unwrap()).unwrap();
            let mut data = NonNull::slice_from_raw_parts(
                NonNull::new(alloc(layout)).unwrap(),
                PAGER_HEADER_SIZE.try_into().unwrap()
            );

            stream.seek(io::SeekFrom::Start(PAGER_HEADER_LOC))?;
            stream.read_exact(data.as_mut())?;

            let mut header = Self {
                page_count: 0,
                page_size: 0,
                free_head: None,
                data, layout
            };

            header.hydrate()?;

            Ok(header)
        }

    }
}

pub struct Pager<Fs: IFileSystem> {
    header: RefCell<PagerHeader>,
    cache: PagerCache,
    path: String,
    fs: Fs,
}

impl<Fs> IPagerInternals for Pager<Fs> 
where Fs: IFileSystem + Clone + 'static
{
    fn set_free_head(&self, head: Option<PageId>) {
        self.header.borrow_mut().set_free_head(head);
    }

    fn page_location(&self, pid: &PageId) -> PageLocation {
        self.header.borrow().page_location(pid)
    }
    
    fn page_count(&self) -> u64 {
       self.header.borrow().page_count
    }
    
    fn free_head(&self) -> Option<PageId> {
        self.header.borrow().free_head
    }
    
    fn page_size(&self) -> usize {
        self.header.borrow().page_size
    }
    
    fn write_header<W: Write + Seek>(&self, stream: &mut W) -> io::Result<()> {
        self.header.borrow().write(stream)
    }
    
}

impl<Fs> IPager for Pager<Fs>
where
    Fs: IFileSystem + Clone + 'static,
{
    fn commit(&self) -> PagerResult<()>{

        // crée une transaction
        let tx = PagerTransaction::new(
            &format!("{0}-tx", self.path), 
            self.fs.clone(),
            self
        );

        let mut file = self.fs.open(&self.path)?;
        
        tx.commit(&mut file, self.cache.iter_pages())
    }

    fn new_page<'pager>(&'pager self) -> PagerResult<MutPage<'pager>> {
        let pid = pop_free_page(self)?.unwrap_or_else(|| self.page_count());

        let mut cell = self.cache.reserve(&pid)?;
        unsafe {
            cell.as_mut().new = true;
        }
        let mut page = MutPage::try_acquire(cell)?;
        page.fill(0);

        self.header.borrow_mut().page_count += 1;

        return Ok(page);
    }

    fn get_page<'pager>(&'pager self, id: &PageId) -> PagerResult<RefPage<'pager>> {
        self.get_page_cell(id).and_then(RefPage::try_acquire)
    }

    fn get_mut_page<'pager>(&'pager self, id: &PageId) -> PagerResult<MutPage<'pager>> {
        self.get_page_cell(id).and_then(MutPage::try_acquire)
    }
    
    fn delete_page(&self, pid: &PageId) -> PagerResult<()> {
        push_free_page(self, pid)?;
        Ok(())
    }
}

impl<Fs> Pager<Fs>
where
    Fs: IFileSystem + Clone + 'static,
{
    /// Crée un nouveau fichier paginé
    pub fn new(fs: Fs, path: &str, page_size: usize, options: PagerOptions) -> PagerResult<Self> 
    {
        let cache_size = options.cache_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);

        // Instantie le système de cache
        let cache = cache::PagerCache::new(
            cache_size, 
            page_size,
            FsPagerStress::new(
              fs.clone(), 
              &format!("{path}-pcache"),
              page_size
            )
        );

        // Initialise le fichier paginé
        let header = PagerHeader::new(page_size);
        let mut file = fs.open(path)?;
        file.write_u16::<LittleEndian>(MAGIC_NUMBER)?;
        header.write(&mut file)?;
        drop(file);

        Ok(Self {
            fs,
            cache,
            header: RefCell::new(header),
            path: path.to_owned(),
        })
    }

    /// Ouvre un fichier paginé
    pub fn open(fs: Fs, path: &str, options: PagerOptions) -> PagerResult<Self> {
        let cache_size = options.cache_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);
        let mut file = fs.open(path)?;
        assert_eq!(file.read_u16::<LittleEndian>()?, MAGIC_NUMBER);

        let header = PagerHeader::read(&mut file)?;
        drop(file);

        // Instantie le système de cache
        let cache = cache::PagerCache::new(
            cache_size, 
            header.page_size,
            FsPagerStress::new(
              fs.clone(), 
              &format!("{path}-pcache"),
              header.page_size
            )
        );

        Ok(Self {
            fs,
            cache,
            header: RefCell::new(header),
            path: path.to_owned(),
        })
    }

    fn get_page_cell(&self, id: &PageId) -> PagerResult<NonNull<PageCell>> {
        // Unexisting page
        if *id >= self.page_count() {
            return Err(PagerError::UnexistingPage);
        }

        // Cache success
        if let Some(page_cell) = self.cache.try_get(&id)? {
            return Ok(page_cell);
        }

        // Load the content of the 
        let cell = self.cache.reserve(id)?;
        let mut page = MutPage::try_acquire(cell)?;
        self.load_page_content(&mut page)?;
        Ok(page.into_cell())
    }

    /// Charge le contenu de la page depuis le système de stockage persistant
    fn load_page_content(&self, page: &mut MutPage<'_>) -> PagerResult<()> {
        let pid = page.id();
        let loc = self.page_location(&pid);

        let mut file = self.fs.open(&self.path)?;
        file.seek(std::io::SeekFrom::Start(loc))?;
        file.read_exact(page)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error, io::Cursor, ops::{Deref, DerefMut}, rc::Rc
    };

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

    use super::{Pager, PagerOptions};
    use crate::{pager::IPager, vfs::in_memory::InMemoryFs};

    #[test]
    pub fn test_new_page() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::new());
        let pager = Pager::new(vfs, "test", 4_096, PagerOptions::default())?;

        {
            let mut page = pager.new_page().unwrap();
            assert_eq!(page.id(), 0);
            drop(page);

            page = pager.new_page().unwrap();
            assert_eq!(page.id(), 1);
        }

        Ok(())
    }

    #[test]
    pub fn test_read_write_page() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::new());
        let pager = Pager::new(vfs, "test", 4_096, PagerOptions::default())?;
        
        let mut page = pager.new_page()?;
        let pid = page.id();
        let expected: u64 = 123456;
        
        Cursor::new(page.deref_mut()).write_u64::<LittleEndian>(expected)?;
        drop(page);

        let page = pager.get_page(&pid)?;
        let got = Cursor::new(page.deref()).read_u64::<LittleEndian>()?;

        assert_eq!(expected, got);
        Ok(())
    }

    #[test]
    pub fn test_commit() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::new());
        let pager = Pager::new(vfs, "test", 4_096, PagerOptions::default())?;
        
        let mut page = pager.new_page()?;
        let _pid = page.id();
        let expected: u64 = 123456;
        
        Cursor::new(page.deref_mut()).write_u64::<LittleEndian>(expected)?;
        drop(page);

        pager.commit()?;

        Ok(())
    }
}

