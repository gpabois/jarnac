use std::{cell::RefCell, collections::HashMap, io::{Read, Seek, Write}};

use crate::vfs::IFileSystem;

use super::{page::PageId, PageCell, PagerResult};

/// Gestion du stress mémoire sur le système de pagination
/// Ce système permet de décharger/récupérer des pages depuis 
/// un endroit capable de stocker sur un plus grand volume
/// de données.
pub trait IPagerStress {
  /// Décharge une page de la mémoire
  fn discharge(&self, pid: &PageId, src: &PageCell) -> PagerResult<()>;
  /// Récupère une page en mémoire
  fn retrieve(&self, pid: &PageId, dest: &mut PageCell) -> PagerResult<()>;
  /// Contient une page déchargée
  fn contains(&self, pid: &PageId) -> bool;
}

/// Indirection
pub struct BoxedPagerStress(Box<dyn IPagerStress>);

impl BoxedPagerStress {
  pub fn new<Ps: IPagerStress + 'static>(imp: Ps) -> Self {
    Self(Box::new(imp))
  }
}

impl IPagerStress for BoxedPagerStress {
    fn discharge(&self, pid: &PageId, src: &PageCell) -> PagerResult<()> {
      self.0.discharge(pid, src)
    }

    fn retrieve(&self, pid: &PageId, dest: &mut PageCell) -> PagerResult<()> {
      self.0.retrieve(pid, dest)
    }

    fn contains(&self, pid: &PageId) -> bool {
        self.0.contains(pid)
    }
}

/// Gestion du stress mémoire du système de pagination
/// par décharge dans un système de fichiers
pub struct FsPagerStress<Fs: IFileSystem> {
  /// Localisation du fichier chargé de récupérer les données déchargées
  path: String,
  /// Le système de fichier qui stocke les données dégagées de la mémoire
  fs: Fs,
  /// Taille d'une page
  page_size: usize,
  /// Pages stockées
  pages: RefCell<HashMap<PageId, usize>>,
  /// Espaces libres
  freelist: RefCell<Vec<usize>>
}

impl<Fs: IFileSystem> FsPagerStress<Fs> {
  pub fn new(fs: Fs, path: &str, page_size: usize) -> Self {
    Self {
      path: path.to_owned(),
      fs,
      page_size,
      pages: Default::default(),
      freelist: Default::default()
    }
  }
}

impl<Fs: IFileSystem> IPagerStress for FsPagerStress<Fs> {
    fn discharge(&self, pid: &PageId, src: &PageCell)  -> PagerResult<()> {
        let offset = self.freelist.borrow_mut().pop().unwrap_or_else(|| self.pages.borrow().len());
        let mut file = self.fs.open(&self.path)?;
        let addr: u64 = (self.page_size * offset).try_into().unwrap();
        file.seek(std::io::SeekFrom::Start(addr))?;
        unsafe {
            file.write_all(src.content.as_ref())?;
        }
        self.pages.borrow_mut().insert(*pid, offset);
        Ok(())
    }

    fn retrieve(&self, pid: &PageId, dest: &mut PageCell) -> PagerResult<()> {
        let offset = self.pages.borrow().get(pid).copied().unwrap();
        let mut file = self.fs.open(&self.path)?;

        let addr: u64 = (self.page_size * offset).try_into().unwrap();
        file.seek(std::io::SeekFrom::Start(addr))?;
        dest.dirty = true;
        unsafe {
            file.read_exact(dest.content.as_mut())?;
        }
        
        self.freelist.borrow_mut().push(offset);
        self.pages.borrow_mut().remove(pid);

        Ok(())
    }
  
    fn contains(&self, pid: &PageId) -> bool {
        self.pages.borrow().contains_key(pid)
    }
}


