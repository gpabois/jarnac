use zerocopy::TryFromBytes;
use zerocopy_derive::{Immutable, IntoBytes, KnownLayout, TryFromBytes};
use std::{
    cell::UnsafeCell,
    io::{self, Read, Seek, Write}, ops::DerefMut,
};

use buffer::{IPageBuffer, PageBuffer};
use free::{pop_free_page, push_free_page};
use page::{descriptor::PageDescriptor, MutPage, OptionalPageId, PageId, PageLocation, PageSize, RefPage};
use stress::{BoxedPagerStress, FsPagerStress, IPagerStress};
use transaction::PagerTransaction;

use crate::{error::{Error, ErrorKind}, fs::{FileOpenOptions, FilePtr, IFileSystem, IPath}, result::Result};

mod buffer;
pub mod cell;
pub mod free;
mod logs;
pub mod page;
pub mod var;
mod stress;
mod transaction;


pub const MAGIC_NUMBER: u16 = 0xD334;

/// Taille par défaut du cache du pager (5 MB).
pub const DEFAULT_PAGER_CACHE_SIZE: usize = 5_000_000;

/// Localisation du pager header.
pub const PAGER_HEADER_LOC: u64 = 2;

/// Taille de l'entête du pager (en bytes).
pub const PAGER_HEADER_SIZE: u64 = 100;

/// Localisation de la première page
pub const PAGER_PAGES_BASE: u64 = PAGER_HEADER_LOC + PAGER_HEADER_SIZE;

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
    fn iter_dirty_pages(&self) -> impl Iterator<Item = PageDescriptor<'_>>;
}

pub trait IPager {
    /// Crée une nouvelle page.
    fn new_page(&self) -> Result<PageId>;
    /// Supprime une page
    fn delete_page(&self, id: &PageId) -> Result<()>;
    /// Récupère une page existante.
    fn borrow_page<'pager>(&'pager self, id: &PageId) -> Result<RefPage<'pager>>;
    /// Récupère une page modifiable existante.
    fn borrow_mut_page<'pager>(&'pager self, id: &PageId) -> Result<MutPage<'pager>>;
    /// Nombre de pages stockées.
    fn len(&self) -> u64;
    /// Retourne la taille d'une page
    fn page_size(&self) -> PageSize;
    /// Commit les pages modifiées.
    fn commit(&self) -> Result<()>;
    /// Aucune page n'est stockée
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type BoxedPager = Box<dyn IPager>;

#[derive(Default)]
pub struct PagerOptions {
    buffer_size: Option<usize>,
    buffer_stress_strategy: Option<BoxedPagerStress>
}

impl PagerOptions {
    pub fn set_buffer_size(mut self, cache_size: usize) -> Self {
        self.buffer_size = Some(cache_size);
        self
    }

    pub fn set_buffer_stress_strategy<Stress: IPagerStress + 'static>(mut self, strategy: Stress) -> Self {
        self.buffer_stress_strategy = Some(BoxedPagerStress::new(strategy));
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
    buffer: PageBuffer,
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

    fn iter_dirty_pages(&self) -> impl Iterator<Item = PageDescriptor<'_>> {
        self.buffer
            .iter()
            .map(|cell| cell.unwrap())
            .filter(|cell| cell.is_dirty())
    }
}

impl<Fs> Pager<Fs> 
where
    Fs: IFileSystem + Clone + 'static,
{
    pub fn into_boxed(self) -> BoxedPager {
        Box::new(self)
    }
}

impl<Fs> IPager for Pager<Fs>
where
    Fs: IFileSystem + Clone + 'static,
{
    fn page_size(&self) -> PageSize {
        self.borrow_header().page_size
    }

    fn commit(&self) -> Result<()> {
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

    fn new_page(&self) -> Result<PageId> {
        let pid = pop_free_page(self)?.unwrap_or_else(|| PageId::new(self.page_count() + 1));
        let page = self.buffer.alloc(&pid)?;

        page.set_new();
        self.borrow_mut_header().inc_page_count();

        Ok(pid)
    }

    fn borrow_page<'pager>(&'pager self, id: &PageId) -> Result<RefPage<'pager>> {
        self.get_cached_page(id).and_then(RefPage::try_new)
    }

    fn borrow_mut_page<'pager>(&'pager self, id: &PageId) -> Result<MutPage<'pager>> {
        self.get_cached_page(id).and_then(MutPage::try_new)
    }

    fn delete_page(&self, pid: &PageId) -> Result<()> {
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
    ) -> Result<Self> {
        let file = FilePtr::new(fs, path);
        let buffer_size = options.buffer_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);

        // Instantie le système de cache
        let buffer = buffer::PageBuffer::new(
            buffer_size,
            page_size,
            options
            .buffer_stress_strategy
            .unwrap_or_else(|| {
                BoxedPagerStress::new(FsPagerStress::new(
                    file.fs.clone(),
                    file.path.modify_stem(|stem| format!("{stem}-pcache")),
                    page_size,
                ))
            })
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
            buffer,
            header: UnsafeCell::new(header_bytes)
        })
    }

    /// Ouvre un fichier paginé
    pub fn open<Path: Into<Fs::Path>>(
        fs: Fs,
        path: Path,
        options: PagerOptions,
    ) -> Result<Self> {
        let file = FilePtr::new(fs, path);
        let cache_size = options.buffer_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);

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
        let cache = buffer::PageBuffer::new(
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
            buffer: cache,
            header: UnsafeCell::new(header_bytes)
        })
    }

    fn get_cached_page<'pager>(&'pager self, pid: &PageId) -> Result<PageDescriptor<'pager>> { 
        // La page n'existe pas
        if *pid >= self.page_count() {
            return Err(Error::new(ErrorKind::UnexistingPage(*pid)));
        }

        // La page a été cachée au préalable
        if let Some(cached) = self.buffer.try_get(pid)? {
            return Ok(cached);
        }

        // On va charger la page depuis le fichier paginé,
        // et la mettre en cache;
        let mut cpage = self.buffer.alloc(pid)?;
        self.load_page_content(&mut cpage)?;
        
        Ok(cpage)
    }

    /// Charge le contenu de la page depuis le système de stockage persistant
    fn load_page_content(&self, page: &mut PageDescriptor<'_>) -> Result<()> {
        let result: Result<()> = {
            let pid = page.id();
            let loc = self.page_location(&pid);

            let mut file = self.file.open(FileOpenOptions::new().read(true))?;
            file.seek(std::io::SeekFrom::Start(loc.into()))?;
            file.read_exact(page.borrow_mut(true).deref_mut())?;

            Ok(())
        };

        result.map_err(|err| Error::new(ErrorKind::PageLoadingFailed { id: *page.id(), source: Box::new(err) }))
        
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, rc::Rc};

    use byteorder::{WriteBytesExt, LE};
    use byteorder::{LittleEndian, ReadBytesExt};
    use zerocopy::IntoBytes;

    use super::fixtures::fixture_new_pager;
    use super::page::{AsMutPageSlice, PageId, PageSize};
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
            assert_eq!(*cpage.id(), pid);
        }

        Ok(())
    }

    #[test]
    pub fn test_stress_write() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();

        for i in 0..10_000u64 {
            let mut page = pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?;
            page.as_mut_bytes().write_u64::<LE>(i).unwrap();
        }

        for j in 0..10_000u64 {
            let pid = PageId::new(j + 1);
            let page = pager.borrow_page(&pid)?;
            let val = page.as_bytes().read_u64::<LE>().unwrap();
            assert_eq!(val, j);
        }

        Ok(())
    }

    #[test]
    pub fn test_read_write_page() -> Result<(), Box<dyn Error>> {
        let vfs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(vfs, "test", PageSize::new(4_096), PagerOptions::default())?;

        let pid = pager.new_page()?;
        let mut page = pager.borrow_mut_page(&pid)?;
        let expected: u64 = 123456;

        page.open_mut_cursor().write_u64::<LittleEndian>(expected)?;
        drop(page);

        let page = pager.borrow_page(&pid)?;
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
            .borrow_mut_page(&pid)?
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
            .borrow_page(&pid)?
            .open_cursor()
            .read_u64::<LittleEndian>()?;

        assert_eq!(expected, got);

        Ok(())
    }
}

pub mod fixtures {
    use std::rc::Rc;

    use crate::fs::in_memory::InMemoryFs;

    use super::{page::PageSize, stress::stubs::StressStub, BoxedPager, Pager, PagerOptions};

    pub fn fixture_new_pager() -> BoxedPager {
        let fs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(
            fs, "memory", 
            PageSize::new(4_096), 
            PagerOptions::default().set_buffer_stress_strategy(StressStub::default())
        ).expect("cannot create pager").into_boxed();
        pager
    }
}