use std::iter;

use crate::{error::{Error, ErrorKind}, fs::{FileOpenOptions, IFileSystem}, result::Result};

use super::{
    logs::PagerLogs, page::AsRefPageSlice, storage::IPagerStorageHandle, IPager, IPagerInternals, Pager
};

pub trait IPagerTransaction {
    fn commit(&self, pager: &Pager) -> Result<()>;
}

/// Transaction atomique sur le fichier paginé.
pub(super) struct PagerTransaction<Fs: IFileSystem> {
    path: Fs::Path,
    fs: Fs,
}

impl<'pager, Fs> PagerTransaction<Fs>
where
    Fs: IFileSystem,
{
    /// Crée une nouvelle transaction pour le fichier paginé
    pub fn new(path: Fs::Path, fs: Fs) -> Self {
        Self { path, fs }
    }

    /// Commit toutes les pages au sein du disque.
    /// Rollback en cas d'erreur.
    pub fn commit(self, pager: &Pager) -> Result<()> {
        let mut dest = pager.storage.open(FileOpenOptions::new().write(true).read(true))?;

        let mut logs = PagerLogs::open(&self.path, &self.fs)?;
        // Sauvegarde l'entête du fichier.
        logs.log_pager_header(dest)?;

        // tampon pour transférer des pages.
        let mut buf: Box<[u8]> = Box::from(
            iter::repeat(0u8)
                .take(self.pager.page_size().into())
                .collect::<Vec<_>>(),
        );

        self.pager
            .iter_dirty_pages()
            .try_for_each(|page| {
                // On filtre les pages propres
                if !page.is_dirty() {
                    return Ok(());
                }

                // Aïe, une page sale est toujours empruntée en écriture...
                if page.is_mut_borrowed() {
                    return Err(Error::new(ErrorKind::PageCurrentlyBorrowed));
                }

                // Si la page n'est pas nouvelle, alors elle existe déjà dans le fichier paginé
                // donc on sauvegarde la version originale de la page.
                if !page.is_new() {
                    dest.read_page(page.tag(), &mut buf)?;
                    logs.log_page(page.tag(), &buf)?;
                }

                dest.write_page(page.tag(), page.borrow().as_bytes())?;
                page.clear_flags();

                Ok(())
            })
            .and_then(|_| {
                dest.write_meta(self.pager.as_meta())
            })
            .inspect_err(|_| logs.rollback(dest).expect("erreur lors du rollback"))
            .inspect(|_| {
                self.pager.iter_dirty_pages().for_each(|page| page.clear_flags());
            })
    }
}
