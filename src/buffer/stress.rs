use std::{
    io::{Read, Seek, Write},
    ops::DerefMut,
    sync::Mutex,
};

use byteorder::{ReadBytesExt, WriteBytesExt};
use dashmap::DashMap;

use crate::{
    fs::{FileOpenOptions, FilePtr, IFileSystem},
    page::{descriptor::PageDescriptor, PageSize},
    result::Result,
    tag::JarTag,
};

/// Gestion du *stress mémoire* sur le système de pagination.
///
/// L'interface définit une stratégie de décharge/récupération de page depuis
/// un endroit capable de stocker un plus grand volume
/// de données, généralement un disque mémoire.
pub trait IBufferStressStrategy {
    /// Décharge une page de la mémoire.
    fn discharge(&self, src: &PageDescriptor<'_>) -> Result<()>;
    /// Récupère une page en mémoire.
    fn retrieve(&self, dest: &mut PageDescriptor<'_>) -> Result<()>;
    /// Vérifie si la page est déchargée.
    fn contains(&self, tag: &JarTag) -> bool;
}

pub type BufferStressStrategy = Box<dyn IBufferStressStrategy>;

/// Gestion du stress mémoire du système de pagination
/// par décharge via un système de fichier (cf [IFileSystem]).
pub struct FsPagerStress<Fs: IFileSystem> {
    /// Pointeur vers le fichier responsable de stocker les données déchargées
    file: FilePtr<Fs>,
    /// Taille d'une page
    page_size: PageSize,
    /// Pages stockées sous la forme pager's pid vers stress's pid.
    pages: DashMap<JarTag, usize>,
    /// Espaces libres
    freelist: Mutex<Vec<usize>>,
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

impl<Fs: IFileSystem> IBufferStressStrategy for FsPagerStress<Fs> {
    fn discharge(&self, src: &PageDescriptor<'_>) -> Result<()> {
        let id = self
            .freelist
            .lock()
            .unwrap()
            .pop()
            .unwrap_or_else(|| self.pages.len());

        let mut file = self
            .file
            .open(FileOpenOptions::new().create(true).write(true))?;

        let loc = u64::from(self.page_size) * u64::try_from(id).unwrap();
        file.seek(std::io::SeekFrom::Start(loc))?;
        file.write_u8(src.get_flags())?;
        unsafe {
            file.write_all(src.get_content_ptr().as_ref())?;
        }
        self.pages.insert(*src.tag(), id);

        Ok(())
    }

    fn retrieve(&self, dest: &mut PageDescriptor<'_>) -> Result<()> {
        let id = self.pages.get(dest.tag()).unwrap().to_owned();
        let mut file = self.file.open(FileOpenOptions::new().read(true))?;

        let addr = u64::from(self.page_size) * u64::try_from(id).unwrap();
        file.seek(std::io::SeekFrom::Start(addr))?;
        dest.set_flags(file.read_u8()?);
        file.read_exact(dest.borrow_mut(true).deref_mut())?;

        self.freelist.lock().unwrap().push(id);
        self.pages.remove(dest.tag());

        Ok(())
    }

    fn contains(&self, tag: &JarTag) -> bool {
        self.pages.contains_key(tag)
    }
}

pub mod stubs {
    use std::io::Write;

    use dashmap::DashMap;

    use crate::{
        page::{AsMutPageSlice, AsRefPageSlice},
        result::Result,
        tag::JarTag,
    };

    use super::IBufferStressStrategy;

    #[derive(Default)]
    /// Bouchon récupérant les décharges du cache
    pub struct StressStub(DashMap<JarTag, Vec<u8>>);

    impl IBufferStressStrategy for StressStub {
        fn discharge(&self, src: &super::PageDescriptor<'_>) -> Result<()> {
            let mut buf = Vec::<u8>::new();
            buf.write_all(src.borrow().as_bytes())?;
            //println!("décharge {0} {buf:?}", src.id());
            self.0.insert(*src.tag(), buf);
            Ok(())
        }

        fn retrieve(&self, dest: &mut super::PageDescriptor<'_>) -> Result<()> {
            let tag = dest.tag();
            let buf = self.0.get(tag).unwrap();
            //println!("récupère {pid} {buf:?}");
            dest.borrow_mut(false).as_mut_bytes().write_all(&buf)?;

            self.0.remove(dest.tag());
            Ok(())
        }

        fn contains(&self, tag: &JarTag) -> bool {
            self.0.contains_key(tag)
        }
    }
}

#[cfg(test)]
mod test {}

