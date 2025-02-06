use std::{
    cell::RefCell,
    collections::HashMap,
    io::{Read, Seek, Write},
    ops::{Deref, DerefMut},
    rc::Rc,
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::fs::{FileOpenOptions, FilePtr, IFileSystem};

use super::{cache::CachedPage, page::PageId, PagerResult};

/// Gestion du *stress mémoire* sur le système de pagination.
///
/// L'interface définit une stratégie de décharge/récupération de page depuis
/// un endroit capable de stocker un plus grand volume
/// de données, généralement un disque mémoire.
pub trait IPagerStress {
    /// Décharge une page de la mémoire.
    fn discharge(&self, src: &CachedPage<'_>) -> PagerResult<()>;
    /// Récupère une page en mémoire.
    fn retrieve(&self, dest: &mut CachedPage<'_>) -> PagerResult<()>;
    /// Vérifie si la page est déchargée.
    fn contains(&self, pid: &PageId) -> bool;
}

impl<U> IPagerStress for Rc<U>
where
    U: IPagerStress,
{
    fn discharge(&self, src: &CachedPage<'_>) -> PagerResult<()> {
        self.deref().discharge(src)
    }

    fn retrieve(&self, dest: &mut CachedPage<'_>) -> PagerResult<()> {
        self.deref().retrieve(dest)
    }

    fn contains(&self, pid: &PageId) -> bool {
        self.deref().contains(pid)
    }
}

/// Indirection permettant de s'abstraire du type concret de la stratégie de gestion du stress mémoire.
pub struct BoxedPagerStress(Box<dyn IPagerStress>);

impl BoxedPagerStress {
    pub fn new<Ps: IPagerStress + 'static>(imp: Ps) -> Self {
        Self(Box::new(imp))
    }
}

impl IPagerStress for BoxedPagerStress {
    fn discharge(&self, src: &CachedPage<'_>) -> PagerResult<()> {
        self.0.discharge(src)
    }

    fn retrieve(&self, dest: &mut CachedPage<'_>) -> PagerResult<()> {
        self.0.retrieve(dest)
    }

    fn contains(&self, pid: &PageId) -> bool {
        self.0.contains(pid)
    }
}

/// Gestion du stress mémoire du système de pagination
/// par décharge via un système de fichier (cf [IFileSystem]).
pub struct FsPagerStress<Fs: IFileSystem> {
    /// Pointeur vers le fichier responsable de stocker les données déchargées
    file: FilePtr<Fs>,
    /// Taille d'une page
    page_size: usize,
    /// Pages stockées
    pages: RefCell<HashMap<PageId, usize>>,
    /// Espaces libres
    freelist: RefCell<Vec<usize>>,
}

impl<Fs: IFileSystem> FsPagerStress<Fs> {
    pub fn new<Path: Into<Fs::Path>>(fs: Fs, path: Path, page_size: usize) -> Self {
        let file = FilePtr::new(fs, path);

        Self {
            file,
            page_size,
            pages: Default::default(),
            freelist: Default::default(),
        }
    }
}

impl<Fs: IFileSystem> IPagerStress for FsPagerStress<Fs> {
    fn discharge(&self, src: &CachedPage<'_>) -> PagerResult<()> {
        let offset = self
            .freelist
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| self.pages.borrow().len());
        let mut file = self
            .file
            .open(FileOpenOptions::new().create(true).write(true))?;
        let addr: u64 = (self.page_size * offset).try_into().unwrap();
        file.seek(std::io::SeekFrom::Start(addr))?;
        file.write_u8(src.flags)?;
        unsafe {
            file.write_all(src.content.as_ref())?;
        }
        self.pages.borrow_mut().insert(src.id(), offset);
        Ok(())
    }

    fn retrieve(&self, dest: &mut CachedPage<'_>) -> PagerResult<()> {
        let offset = self.pages.borrow().get(&dest.id()).copied().unwrap();
        let mut file = self.file.open(FileOpenOptions::new().read(true))?;

        let addr: u64 = (self.page_size * offset).try_into().unwrap();
        file.seek(std::io::SeekFrom::Start(addr))?;
        dest.flags = file.read_u8()?;
        file.read_exact(dest.borrow_mut(true).deref_mut())?;

        self.freelist.borrow_mut().push(offset);
        self.pages.borrow_mut().remove(&dest.id());

        Ok(())
    }

    fn contains(&self, pid: &PageId) -> bool {
        self.pages.borrow().contains_key(pid)
    }
}
