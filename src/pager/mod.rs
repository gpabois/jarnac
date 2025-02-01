use std::{cell::RefCell, fs::File, io::{self, Read, Seek, Write}};

use page::{Page, PageId};

use crate::vfs::IFileSystem;

pub mod page;
pub mod cache;

pub const DEFAULT_PAGER_CACHE_SIZE: usize = 5_000_000;

#[derive(Debug)]
pub enum PagerError {
  CacheFull,
  UnexistingPage,
  PageAlreadyBorrowed,
  IoError(io::Error)
}

impl From<io::Error> for PagerError {
    fn from(value: io::Error) -> Self {
      Self::IoError(value)
    }
}

pub type PagerResult<T> = Result<T, PagerError>;

pub trait IPager {
  /// Flush tout ce qui reste en mémoire
  fn flush(&self) -> PagerResult<()>;
  /// Flush la page
  fn flush_page(&self, page: &Page<'_>) -> PagerResult<()>;
  /// Crée une nouvelle page
  fn new_page<'pager>(&'pager mut self) -> PagerResult<Page<'pager>>;
  /// Récupère une page existante
  fn get_page<'pager>(&'pager self, id: PageId) -> PagerResult<Page<'pager>>;
}


#[derive(Default)]
pub struct PagerOptions {
  cache_size: Option<usize>
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
  cache: RefCell<cache::PagerCache>,
  path: String,
  fs: Fs
}

impl<Fs> IPager for Pager<Fs> where Fs: IFileSystem {
  /// Flush la page
  fn flush_page(&self, page: &Page<'_>) -> PagerResult<()> {
    if *self.dirty.borrow() {
      self.flush_pager_header()?;
    }

    if page.is_dirty() {
      let id = page.id();
      let loc = (self.page_size * id).try_into().unwrap();
      let mut file = File::open(&self.path)?;
      file.seek(std::io::SeekFrom::Start(loc))?;
      file.write_all(page)?;
      page.drop_dirty_flag();
    }
    Ok(())
  }

  /// Crée une nouvelle page
  fn new_page<'pager>(&'pager mut self) -> PagerResult<Page<'pager>> {
    let id = self.page_count;

    // Cache full
    if self.cache.borrow().is_full() {
      // Free some cache space
      self.free_cache_space()?;
    }

    let mut page = Page::try_acquire(self.cache.borrow_mut().reserve(id)?)?;
    page.fill(0);

    self.page_count += 1;
    *self.dirty.borrow_mut() = true;

    return Ok(page)
  }
  
  /// Récupère une page
  fn get_page<'pager>(&'pager self, id: PageId) -> PagerResult<Page<'pager>> {
    // Unexisting page
    if id >= self.page_count {
      return Err(PagerError::UnexistingPage);
    }

    // Cache success
    if let Some(page_acquisition_result) = self.cache.borrow().get(&id).map(Page::try_acquire) {
      return page_acquisition_result;
    }

    // Cache miss
    if self.cache.borrow().is_full() {
      // Free some cache space
      self.free_cache_space()?;
    }

    let mut page = Page::try_acquire(self.cache.borrow_mut().reserve(id)?)?;
    self.load_page_content(&mut page)?;

    return Ok(page);
  }

  fn flush(&self) -> PagerResult<()> {
    todo!()
  }

}

impl<Fs> Pager<Fs> where Fs: IFileSystem {
  /// Crée un nouveau pager
  pub fn new(fs: Fs, path: &str, page_size: usize, options: PagerOptions) -> Self {
    let cache_size = options.cache_size.unwrap_or(DEFAULT_PAGER_CACHE_SIZE);
    let cache = RefCell::new(cache::PagerCache::new(cache_size, page_size));
    
    Self {
      fs,
      dirty: RefCell::new(false),
      cache,
      page_size,
      path: path.to_owned(),
      page_count: 0
    }
  }

  /// Libère de l'espace cache pour stocker de nouvelles pages.
  fn free_cache_space(&self) -> PagerResult<()> {
    if let Some(candidate) = self.cache.borrow_mut().find_freeable_candidate() {
      let page = self.get_page(candidate)?;
      self.flush_page(&page)?;
      drop(page);

      self.cache.borrow_mut().free(&candidate);
      return Ok(())
    }

    return Err(PagerError::CacheFull);
  }

  /// Charge le contenu de la page depuis le système de stockage persistant
  fn load_page_content(&self, page: &mut Page<'_>) -> PagerResult<()>{
    let id = page.id();
    let loc = (self.page_size * id).try_into().unwrap();
    
    let mut file = self.fs.open(&self.path)?;
    file.seek(std::io::SeekFrom::Start(loc))?;
    file.read_exact(page)?;

    Ok(())
  }

  /// Flush l'entête du pager.
  fn flush_pager_header(&self) -> PagerResult<()> {
    let mut file =  self.fs.open(&self.path)?;
    file.write_all(&0x1991_u16.to_le_bytes())?;
    file.write_all(&self.page_size.to_le_bytes())?;
    file.write_all(&self.page_count.to_le_bytes())?;
    *self.dirty.borrow_mut() = false;
    Ok(())
  }
}


#[cfg(test)]
mod tests {
  use std::{io::{Cursor, Write}, ops::DerefMut, rc::Rc};

use crate::vfs::in_memory::InMemoryFs;
  use super::{IPager, Pager, PagerOptions};

  #[test]
  pub fn test_new_pager() {
    let vfs = Rc::new(InMemoryFs::new());
    let mut pager = Pager::new(vfs, "test", 4_000, PagerOptions::default());
    
    {
      let mut page = pager.new_page().unwrap();
      let mut cursor = Cursor::new(page.deref_mut());
      cursor.write_all(&0xD00D_u16.to_le_bytes()).unwrap();
    }

    pager.flush();
  }
}