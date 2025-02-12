use zerocopy::TryFromBytes;
use zerocopy_derive::{Immutable, IntoBytes, KnownLayout, TryFromBytes};
use std::{
    cell::UnsafeCell,
    io::{self, Read, Seek, Write},
    ops::DerefMut,
};

use cache::{CachedPage, PagerCache};
use free::{pop_free_page, push_free_page};
use page::{MutPage, OptionalPageId, PageId, PageLocation, PageSize, RefPage};
use stress::FsPagerStress;
use transaction::PagerTransaction;

use crate::fs::{FileOpenOptions, FilePtr, IFileSystem, IPath};

mod cache;
pub mod cell;
pub mod error;
pub mod free;
mod logs;
pub mod page;
pub mod spill;
mod stress;
mod transaction;

use error::{PagerError, PagerErrorKind};

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
trait IPagerInternals: IPager {
    /// Change la tête de la liste des pages libres.
    fn set_free_head(&self, head: Option<PageId>);
    /// Retourne la tête de la liste des pages libres.
    fn free_head(&self) -> Option<PageId>;
    /// Retourne la localisation de la page sur le disque.
    fn page_location(&self, pid: &PageId) -> PageLocation;
    /// Retourne le nombre de pages
    fn page_count(&self) -> u64;
    /// Ecrit l'entête du fichier dans le flux
    fn write_header<W: Write + Seek>(&self, stream: &mut W) -> io::Result<()>;
    /// Itère sur les pages cachées
    fn iter_dirty_pages(&self) -> impl Iterator<Item = CachedPage<'_>>;
}

pub trait IPager {
    /// Crée une nouvelle page.
    fn new_page(&self) -> PagerResult<PageId>;
    /// Supprime une page
    fn delete_page(&self, id: &PageId) -> PagerResult<()>;
    /// Récupère une page existante.
    fn get_page<'pager>(&'pager self, id: &PageId) -> PagerResult<RefPage<'pager>>;
    /// Récupère une page modifiable existante.
    fn get_mut_page<'pager>(&'pager self, id: &PageId) -> PagerResult<MutPage<'pager>>;
    /// Nombre de pages stockées.
    fn len(&self) -> u64;
    /// Aucune page n'est stockée
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Reoturne la taille d'une page
    fn page_size(&self) -> PageSize;
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

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(packed)]
pub struct PagerHeader {
    /// Nombre magique
    magic_number: u16,
    /// Taille d'une page
    pub(super) page_size: PageSize,
    /// Nombre de pages stockées dans le pager
    pub(super) page_count: u64,
    /// Début de la liste chaînée des pages libres
    pub(super) free_head: OptionalPageId,
}

impl PagerHeader {
    fn initialise(&mut self, page_size: PageSize) {
        *self = Self {
            magic_number: MAGIC_NUMBER,
            page_size,
            page_count: 0,
            free_head: None.into()
        }
    }
}

impl PagerHeader {
    pub const PAGE_SIZE_LOC: u64 = 0;
    pub const PAGE_COUNT_LOC: u64 = 8;
    pub const PAGE_FREE_HEAD_LOC: u64 = 16;

    pub fn inc_page_count(&mut self) {
        self.page_count += 1;

    }

    pub fn set_free_head(&mut self, head: Option<PageId>) {
        self.free_head = head.into();
    }

    pub fn page_location(&self, pid: &PageId) -> PageLocation {
        let page_size = self.page_size;
        pid.get_location(PAGER_PAGES_BASE, &page_size)
    }
}

pub struct Pager<Fs: IFileSystem> {
    header: UnsafeCell<Box<[u8; size_of::<PagerHeader>()]>>,
    cache: PagerCache,
    file: FilePtr<Fs>,
}

impl<Fs> Pager<Fs> 
where Fs: IFileSystem
{
    fn borrow_header(&self) -> &PagerHeader {
        unsafe  {
            let bytes = self.header.get().as_ref().unwrap().as_slice();
            PagerHeader::try_ref_from_bytes(bytes).unwrap()
        }
    }

    fn borrow_mut_header(&self) -> &mut PagerHeader {
        unsafe  {
            let bytes = self.header.get().as_mut().unwrap().as_mut_slice();
            PagerHeader::try_mut_from_bytes(bytes).unwrap()
        }    
    }
}

impl<Fs> IPagerInternals for Pager<Fs>
where
    Fs: IFileSystem + Clone + 'static,
{
    fn set_free_head(&self, head: Option<PageId>) {
        self.borrow_mut_header().set_free_head(head);
    }

    fn page_location(&self, pid: &PageId) -> PageLocation {
        self.borrow_header().page_location(pid)
    }

    fn page_count(&self) -> u64 {
        self.borrow_header().page_count
    }

    fn free_head(&self) -> Option<PageId> {
        self.borrow_header().free_head.into()
    }

    fn write_header<W: Write + Seek>(&self, stream: &mut W) -> io::Result<()> {
        unsafe {
            stream.write_all(self.header.get().as_ref().unwrap().as_slice())
        }
    }

    fn iter_dirty_pages(&self) -> impl Iterator<Item = CachedPage<'_>> {
        self.cache
            .iter()
            .map(|cell| cell.unwrap())
            .filter(|cell| cell.is_dirty())
    }
}

impl<Fs> IPager for Pager<Fs>
where
    Fs: IFileSystem + Clone + 'static,
{
    fn page_size(&self) -> PageSize {
        self.borrow_header().page_size
    }

    fn commit(&self) -> PagerResult<()> {
        // crée une transaction
        let tx = PagerTransaction::new(
            self.file.path.modify_stem(|stem| format!("{stem}-tx")),
            self.file.fs.clone(),
            self,
        );

        let mut file = self
            .file
            .open(FileOpenOptions::new().write(true).read(true))?;
        tx.commit(&mut file)
    }

    fn new_page(&self) -> PagerResult<PageId> {
        let pid = pop_free_page(self)?.unwrap_or_else(|| PageId::new(self.page_count() + 1));
        let mut cpage = self.cache.alloc(&pid)?;

        cpage.set_new();
        self.borrow_mut_header().inc_page_count();

        Ok(pid)
    }

    fn get_page<'pager>(&'pager self, id: &PageId) -> PagerResult<RefPage<'pager>> {
        self.get_cached_page(id).and_then(RefPage::try_new)
    }

    fn get_mut_page<'pager>(&'pager self, id: &PageId) -> PagerResult<MutPage<'pager>> {
        self.get_cached_page(id).and_then(MutPage::try_new)
    }

    fn delete_page(&self, pid: &PageId) -> PagerResult<()> {
        push_free_page(self, pid)
    }

    fn len(&self) -> u64 {
        self.borrow_header().page_count
    }
}

impl<Fs> Pager<Fs>
where
    Fs: IFileSystem + Clone + 'static,
{
    /// Crée un nouveau fichier paginé
    pub fn new<Path: Into<Fs::Path>>(
        fs: Fs,
        path: Path,
        page_size: PageSize,
        options: PagerOptions,
    ) -> PagerResult<Self> {
        let file = FilePtr::new(fs, path);
        let cache_size = options.cache_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);

        // Instantie le système de cache
        let cache = cache::PagerCache::new(
            cache_size,
            page_size,
            FsPagerStress::new(
                file.fs.clone(),
                file.path.modify_stem(|stem| format!("{stem}-pcache")),
                page_size,
            ),
        );

        // Initialise le fichier paginé, il ne doit pas déjà exister !
        if file.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("file {0} already exists", file.path.to_string()),
            )
            .into());
        }

        let mut header_bytes = Box::<[u8; size_of::<PagerHeader>()]>::new([0; size_of::<PagerHeader>()]);
        let header = PagerHeader::try_mut_from_bytes(header_bytes.as_mut_slice()).unwrap();
        header.initialise(page_size);

        Ok(Self {
            file,
            cache,
            header: UnsafeCell::new(header_bytes)
        })
    }

    /// Ouvre un fichier paginé
    pub fn open<Path: Into<Fs::Path>>(
        fs: Fs,
        path: Path,
        options: PagerOptions,
    ) -> PagerResult<Self> {
        let file = FilePtr::new(fs, path);
        let cache_size = options.cache_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);

        let mut header_bytes = Box::<[u8; size_of::<PagerHeader>()]>::new([0; size_of::<PagerHeader>()]);

        // On récupère l'entête du fichier paginé.
        let header = {
            let mut stream = file.open(FileOpenOptions::new().read(true))?;
            stream.read_exact(header_bytes.as_mut_slice())?;

            let header = PagerHeader::try_mut_from_bytes(header_bytes.as_mut_slice()).unwrap();
            assert!(header.magic_number == MAGIC_NUMBER, "not a nac file");
            header
        };

        // Instantie le cache
        let cache = cache::PagerCache::new(
            cache_size,
            header.page_size,
            FsPagerStress::new(
                file.fs.clone(),
                file.path.modify_stem(|stem| format!("{stem}-pcache")),
                header.page_size,
            ),
        );

        Ok(Self {
            file,
            cache,
            header: UnsafeCell::new(header_bytes)
        })
    }

    fn get_cached_page<'pager>(&'pager self, pid: &PageId) -> PagerResult<CachedPage<'pager>> {
        // La page n'existe pas
        if *pid > self.page_count() {
            return Err(PagerError::new(PagerErrorKind::UnexistingPage(*pid)));
        }

        // La page a été cachée au préalable
        if let Some(cached) = self.cache.try_get(pid)? {
            return Ok(cached);
        }

        // On va charger la page depuis le fichier paginé,
        // et la mettre en cache;
        let mut cpage = self.cache.alloc(pid)?;
        self.load_page_content(&mut cpage)?;
        Ok(cpage)
    }

    /// Charge le contenu de la page depuis le système de stockage persistant
    fn load_page_content(&self, page: &mut CachedPage<'_>) -> PagerResult<()> {
        let pid = page.id();
        let loc = self.page_location(&pid);

        let mut file = self.file.open(FileOpenOptions::new().read(true))?;
        file.seek(std::io::SeekFrom::Start(loc.into()))?;
        file.read_exact(page.borrow_mut(true).deref_mut())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, rc::Rc};

    use byteorder::WriteBytesExt;
    use byteorder::{LittleEndian, ReadBytesExt};

    use super::page::PageSize;
    use super::{Pager, PagerOptions};
    use crate::{fs::in_memory::InMemoryFs, pager::IPager};

    #[test]
    pub fn test_new_page() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(vfs, "test", PageSize::new(4_096), PagerOptions::default())?;

        {
            let pid = pager.new_page().unwrap();
            assert_eq!(pid, 1);

            let cpage = pager.get_cached_page(&pid)?;
            assert!(cpage.is_new());
            assert_eq!(cpage.id(), pid);
        }

        Ok(())
    }

    #[test]
    pub fn test_read_write_page() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(vfs, "test", PageSize::new(4_096), PagerOptions::default())?;

        let pid = pager.new_page()?;
        let mut page = pager.get_mut_page(&pid)?;
        let expected: u64 = 123456;

        page.open_mut_cursor().write_u64::<LittleEndian>(expected)?;
        drop(page);

        let page = pager.get_page(&pid)?;
        let got = page.open_cursor().read_u64::<LittleEndian>()?;

        assert_eq!(expected, got);
        Ok(())
    }

    #[test]
    pub fn test_commit() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(
            vfs.clone(),
            "test",
            PageSize::new(4_096),
            PagerOptions::default(),
        )?;

        let pid = pager.new_page()?;
        let expected: u64 = 123456;

        pager
            .get_mut_page(&pid)?
            .open_mut_cursor()
            .write_u64::<LittleEndian>(expected)?;

        assert!(pager.get_cached_page(&pid)?.is_new());
        assert_eq!(
            pager
                .get_cached_page(&pid)?
                .borrow()
                .open_cursor()
                .read_u64::<LittleEndian>()?,
            expected,
            "le contenu de la page doit être 123456"
        );

        pager
            .commit()
            .inspect_err(|err| println!("{}", err.backtrace))?;

        drop(pager);

        let pager = Pager::open(vfs, "test", PagerOptions::default())?;
        assert_eq!(pager.len(), 1);

        let got = pager
            .get_page(&pid)?
            .open_cursor()
            .read_u64::<LittleEndian>()?;

        assert_eq!(expected, got);

        Ok(())
    }
}
