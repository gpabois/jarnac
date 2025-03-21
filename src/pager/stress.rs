use std::{
    cell::RefCell,
    collections::HashMap,
    io::{Read, Seek, Write},
    ops::{Deref, DerefMut},
    rc::Rc,
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{fs::{FileOpenOptions, FilePtr, IFileSystem}, result::Result};

use super::page::{descriptor::PageDescriptor, PageId, PageSize};

/// Gestion du *stress mémoire* sur le système de pagination.
///
/// L'interface définit une stratégie de décharge/récupération de page depuis
/// un endroit capable de stocker un plus grand volume
/// de données, généralement un disque mémoire.
pub trait IPagerStress {
    /// Décharge une page de la mémoire.
    fn discharge(&self, src: &PageDescriptor<'_>) -> Result<()>;
    /// Récupère une page en mémoire.
    fn retrieve(&self, dest: &mut PageDescriptor<'_>) -> Result<()>;
    /// Vérifie si la page est déchargée.
    fn contains(&self, pid: &PageId) -> bool;
}

impl<U> IPagerStress for Rc<U>
where
    U: IPagerStress,
{
    fn discharge(&self, src: &PageDescriptor<'_>) -> Result<()> {
        self.deref().discharge(src)
    }

    fn retrieve(&self, dest: &mut PageDescriptor<'_>) -> Result<()> {
        self.deref().retrieve(dest)
    }

    fn contains(&self, pid: &PageId) -> bool 
    
    {
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
    fn discharge(&self, src: &PageDescriptor<'_>) -> Result<()> {
        self.0.discharge(src)
    }

    fn retrieve(&self, dest: &mut PageDescriptor<'_>) -> Result<()> {
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
    page_size: PageSize,
    /// Pages stockées sous la forme pager's pid vers stress's pid.
    pages: RefCell<HashMap<PageId, PageId>>,
    /// Espaces libres
    freelist: RefCell<Vec<PageId>>,
}

impl<Fs: IFileSystem> FsPagerStress<Fs> {
    pub fn new<Path: Into<Fs::Path>>(fs: Fs, path: Path, page_size: PageSize) -> Self {
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
    fn discharge(&self, src: &PageDescriptor<'_>) -> Result<()> {
        let pid: PageId = self
            .freelist
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| PageId::from(self.pages.borrow().len() + 1));

        let mut file = self
            .file
            .open(FileOpenOptions::new().create(true).write(true))?;

        let addr = pid *self.page_size;
        file.seek(std::io::SeekFrom::Start(addr.into()))?;
        file.write_u8(src.get_flags())?;
        unsafe {
            file.write_all(src.get_content_ptr().as_ref())?;
        }
        self.pages.borrow_mut().insert(*src.id(), pid);

        Ok(())
    }

    fn retrieve(&self, dest: &mut PageDescriptor<'_>) -> Result<()> {
        let pid = self.pages.borrow().get(&dest.id()).copied().map(PageId::from).unwrap();
        let mut file = self.file.open(FileOpenOptions::new().read(true))?;

        let addr = pid * self.page_size;
        file.seek(std::io::SeekFrom::Start(addr.into()))?;
        dest.set_flags(file.read_u8()?);
        file.read_exact(dest.borrow_mut(true).deref_mut())?;

        self.freelist.borrow_mut().push(pid);
        self.pages.borrow_mut().remove(&dest.id());

        Ok(())
    }

    fn contains(&self, pid: &PageId) -> bool {
        self.pages.borrow().contains_key(pid)
    }
}

pub mod stubs {
    use std::{cell::RefCell, collections::HashMap, io::Write};

    use crate::{pager::page::{AsMutPageSlice, AsRefPageSlice, PageId}, result::Result};

    use super::IPagerStress;

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

}

#[cfg(test)]
mod test {
    use std::{error::Error, io::Write};

    use byteorder::{ReadBytesExt, LE};

    use crate::pager::{fixtures::fixture_new_pager, page::{AsMutPageSlice, AsRefPageSlice, PageId}};

    #[test]
    fn test_stress() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();

        for i in 1..100_000u64 {
            let mut page = pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?;
            page.as_mut_bytes().write_all(&i.to_le_bytes()).unwrap();
        }

        for i in (1..100_000u64).into_iter().map(PageId::new) {
            let page = pager.borrow_page(&i)?;
            assert_eq!(page.as_bytes().read_u64::<LE>().unwrap(), u64::try_from(i).unwrap());
        }

        Ok(())
    }
}