use std::{
    io::{self, Read, Seek, Write},
    iter,
    ops::Deref,
};

use crate::{error::{Error, ErrorKind}, fs::IFileSystem, result::Result};

use super::{
    logs::PagerLogs,
    IPagerInternals, 
};

/// Transaction atomique sur le fichier paginé.
pub(super) struct PagerTransaction<'pager, Fs: IFileSystem, Pager: IPagerInternals> {
    path: Fs::Path,
    fs: Fs,
    pager: &'pager Pager,
}

impl<'pager, Fs, Pager: IPagerInternals> PagerTransaction<'pager, Fs, Pager>
where
    Fs: IFileSystem,
    Pager: IPagerInternals,
{
    /// Crée une nouvelle transaction pour le fichier paginé
    pub fn new(path: Fs::Path, fs: Fs, pager: &'pager Pager) -> Self {
        Self { path, fs, pager }
    }

    /// Commit toutes les pages au sein du disque.
    /// Rollback en cas d'erreur.
    pub fn commit(self, file: &mut Fs::File<'_>) -> Result<()> {
        let mut logs = PagerLogs::open(&self.path, &self.fs)?;
        // Sauvegarde l'entête du fichier.
        logs.log_pager_header(file)?;

        let mut buf: Box<[u8]> = Box::from(
            iter::repeat(0u8)
                .take(self.pager.page_size().into())
                .collect::<Vec<_>>(),
        );

        self.pager
            .iter_dirty_pages()
            .try_for_each(|cpage| {
                // On filtre les pages propres
                if !cpage.is_dirty() {
                    return Ok(());
                }

                // Aïe, une page sale est toujours empruntée en écriture...
                if cpage.is_mut_borrowed() {
                    return Err(Error::new(ErrorKind::PageCurrentlyBorrowed));
                }

                
                let loc = self.pager.page_location(cpage.id());

                // Si la page n'est pas nouvelle, alors elle existe déjà dans le fichier paginé
                // donc on sauvegarde la version originale de la page.
                if !cpage.is_new() {
                    file.seek(io::SeekFrom::Start(loc.into()))?;
                    file.read_exact(&mut buf)?;
                    logs.log_page(&cpage.id(), &buf)?;
                }

                file.seek(io::SeekFrom::Start(loc.into()))?;
                file.write_all(cpage.borrow().deref())?;
                cpage.clear_flags();

                Ok(())
            })
            .and_then(|_| {
                file.seek(io::SeekFrom::Start(0))?;
                self.pager.write_header(file)?;
                Ok(())
            })
            .inspect_err(|_| logs.rollback(file).expect("erreur lors du rollback"))
            .inspect(|_| {
                self.pager
                    .iter_dirty_pages()
                    .for_each(|cpage| cpage.clear_flags());
            })
    }
}
