use std::{
    cell::RefCell,
    io::{self, Read, Seek, Write},
    ptr::NonNull,
};

use cache::{PageCell, PagerCache};
use page::{MutPage, PageId, RefPage};
use stress::FsPagerStress;
use transaction::PagerTransaction;

use crate::vfs::IFileSystem;

pub mod page;
mod transaction;
mod cache;
mod stress;

pub const DEFAULT_PAGER_CACHE_SIZE: usize = 5_000_000;
pub const PAGER_HEADER_RESERVED: u64 = 100;

#[derive(Debug)]
pub enum PagerError {
    CacheFull,
    UnexistingPage,
    PageAlreadyCached,
    PageAlreadyBorrowed,
    IoError(io::Error),
}

impl From<io::Error> for PagerError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}

pub type PagerResult<T> = Result<T, PagerError>;

pub trait IPager {
    /// Commit les pages modifiées.
    fn commit(&self) -> PagerResult<()>;
    /// Crée une nouvelle page.
    fn new_page(&mut self) -> PagerResult<MutPage<'_>>;
    /// Récupère une page existante.
    fn get_page<'pager>(&'pager self, id: &PageId) -> PagerResult<RefPage<'pager>>;
    /// Récupère une page modifiable existante.
    fn get_mut_page<'pager>(&'pager self, id: &PageId) -> PagerResult<MutPage<'pager>>;
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

pub struct Pager<Fs: IFileSystem> {
    dirty: RefCell<bool>,
    page_size: usize,
    page_count: usize,
    cache: PagerCache,
    path: String,
    fs: Fs,
}

impl<Fs> IPager for Pager<Fs>
where
    Fs: IFileSystem + Clone,
{
    fn commit(&self) -> PagerResult<()>{
        let tx = PagerTransaction::new(
            &format!("{0}-tx", self.path), 
            self.fs.clone(),
            self.page_size
        );

        let mut file = self.fs.open(&self.path)?;
        tx.commit(&mut file, self.cache.iter_pages())
    }

    /// Crée une nouvelle page
    fn new_page<'pager>(&'pager mut self) -> PagerResult<MutPage<'pager>> {
        let pid = self.page_count;

        let cell = self.cache.reserve(&pid)?;
        let mut page = MutPage::try_acquire(cell)?;
        page.fill(0);

        self.page_count += 1;
        *self.dirty.borrow_mut() = true;

        return Ok(page);
    }

    /// Récupère une page non modifiable
    fn get_page<'pager>(&'pager self, id: &PageId) -> PagerResult<RefPage<'pager>> {
        self.get_page_cell(id).and_then(RefPage::try_acquire)
    }

    /// Récupère une page modifiable
    fn get_mut_page<'pager>(&'pager self, id: &PageId) -> PagerResult<MutPage<'pager>> {
        self.get_page_cell(id).and_then(MutPage::try_acquire)
    }
}

impl<Fs> Pager<Fs>
where
    Fs: IFileSystem,
{
    /// Crée un nouveau pager
    pub fn new(fs: Fs, path: &str, page_size: usize, options: PagerOptions) -> Self 
    where Fs: Clone + 'static
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

        Self {
            fs,
            dirty: RefCell::new(false),
            cache,
            page_size,
            path: path.to_owned(),
            page_count: 0,
        }
    }

    fn get_page_cell(&self, id: &PageId) -> PagerResult<NonNull<PageCell>> {
        // Unexisting page
        if *id >= self.page_count {
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
        let id = page.id();
        let loc = (self.page_size * id).try_into().unwrap();

        let mut file = self.fs.open(&self.path)?;
        file.seek(std::io::SeekFrom::Start(loc))?;
        file.read_exact(page)?;

        Ok(())
    }

    /// Flush l'entête du pager.
    fn flush_pager_header(&self) -> PagerResult<()> {
        let mut file = self.fs.open(&self.path)?;
        file.write_all(&0x1991_u16.to_le_bytes())?;
        file.write_all(&self.page_size.to_le_bytes())?;
        file.write_all(&self.page_count.to_le_bytes())?;
        *self.dirty.borrow_mut() = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Cursor, Write},
        ops::DerefMut,
        rc::Rc,
    };

    use super::{IPager, Pager, PagerOptions};
    use crate::vfs::in_memory::InMemoryFs;

    #[test]
    pub fn test_new_pager() {
        let vfs = Rc::new(InMemoryFs::new());
        let mut pager = Pager::new(vfs, "test", 4_096, PagerOptions::default());

        {
            let mut page = pager.new_page().unwrap();
            let mut cursor = Cursor::new(page.deref_mut());
            cursor.write_all(&0xD00D_u16.to_le_bytes()).unwrap();
        }
    }
}

