use std::{io::{self, Read, Seek, Write}, iter, ptr::NonNull};

use crate::vfs::IFileSystem;

use super::{error::PagerError, logs::PagerLogs, IPagerInternals, PageCell, PagerResult};


/// Représente une transaction atomique pour le pager
/// Permet de rollback si l'écriture des pages modifiées
/// échouent pour diverses raisons.
pub(super) struct PagerTransaction<'pager, Fs: IFileSystem, Pager: IPagerInternals> {
    path: String,
    fs: Fs,
    pager: &'pager Pager
}

impl<'pager, Fs, Pager: IPagerInternals> PagerTransaction<'pager, Fs, Pager> 
where Fs: IFileSystem, Pager: IPagerInternals
{
    /// Crée une nouvelle transaction pour le fichier paginé
    pub fn new(path: &str, fs: Fs, pager: &'pager Pager) -> Self {
        Self {path: path.to_owned(), fs, pager}
    }

    /// Commit toutes les pages au sein du disque.
    /// Rollback en cas d'erreur.
    pub fn commit<'a>(self, file: &mut Fs::File<'_>, pages: impl Iterator<Item=PagerResult<NonNull<PageCell>>>) -> PagerResult<()> {
        let mut logs = PagerLogs::open(&self.path, &self.fs)?;
        // Sauvegarde l'entête du fichier.
        logs.log_pager_header(file)?;

        let mut buf: Box<[u8]> = Box::from(
            iter::repeat(0u8)
            .take(self.pager.page_size())
            .collect::<Vec<_>>()
        );

        pages
        .map(|res| {
            let mut ptr = res?;
            unsafe {
                let cell = ptr.as_mut();

                // On filtre les pages propres
                if cell.dirty == false {
                    return Ok(());
                }

                // Aïe, une page sale est toujours empruntée en écriture...
                if cell.rw_counter < 0 {
                    return Err(PagerError::PageAlreadyBorrowed)
                }

                let loc = self.pager.page_location(&cell.id);
                
                // si la page n'est pas nouvelle, alors elle existe déjà dans
                // le fichier paginé.
                if cell.new == false {
                    file.seek(io::SeekFrom::Start(loc))?;
                    file.read_exact(&mut buf)?;
                    // Sauvegarde la version originale de la page;
                    logs.log_page(&cell.id, cell.content.as_ref())?;
                }

                file.seek(io::SeekFrom::Start(loc))?;
                file.write_all(cell.content.as_ref())?;
                cell.dirty = false;
                cell.new = false;
            }

            Ok(())
        })
        .collect::<PagerResult<()>>()
        .and_then(|_| {
            file.seek(io::SeekFrom::Start(0))?;
            self.pager.write_header(file)?;
            Ok(())
        })
        .inspect_err(|_| logs.rollback(file).expect("erreur lors du rollback"))
    }

}
