use std::{io::{self, Read, Seek, Write}, iter, ptr::NonNull};

use crate::vfs::IFileSystem;

use super::{PageCell, PagerError, PagerResult, PAGER_HEADER_RESERVED};


/// Représente une transaction atomique pour le pager
/// Permet de rollback si l'écriture des pages modifiées
/// échouent pour diverses raisons.
pub(super) struct PagerTransaction<Fs: IFileSystem> {
    path: String,
    fs: Fs,
    page_size: usize
}

impl<Fs: IFileSystem> PagerTransaction<Fs> {
    pub fn new(path: &str, fs: Fs, page_size: usize) -> Self {
        Self {path: path.to_owned(), fs, page_size}
    }

    /// Commit toutes les pages au sein du disque.
    /// Rollback en cas d'erreur.
    pub fn commit<'a>(self, file: &mut Fs::File<'_>, pages: impl Iterator<Item=PagerResult<NonNull<PageCell>>>) -> PagerResult<()> {
        let mut rollback_file = self.fs.open(&self.path)?;

        let mut buf: Box<[u8]> = Box::from(iter::repeat(0u8).take(self.page_size).collect::<Vec<_>>());

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

                let mut offset: u64 = (cell.content.len() * cell.id).try_into().unwrap();
                offset += PAGER_HEADER_RESERVED;

                file.seek(io::SeekFrom::Start(offset))?;
                file.read_exact(&mut buf)?;
                let ps: i64 = self.page_size.try_into().unwrap();
                file.seek(io::SeekFrom::Current(-ps));

                rollback_file.write_all(&cell.id.to_le_bytes())?;
                rollback_file.write_all(&buf)?;

                file.write_all(cell.content.as_ref())?;
                cell.dirty = false;
            }

            Ok(())
        }).collect::<PagerResult<_>>()
        .inspect_err(|_| Self::rollback(&mut rollback_file, file))
    }

    /// Rollback
    fn rollback(src: &mut Fs::File<'_>, dest: &mut Fs::File<'_>) {

    }
}
