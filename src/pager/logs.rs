use std::{
    io::{self, Read, Seek, Write},
    iter,
};

use byteorder::{LittleEndian, WriteBytesExt};
use zerocopy::FromBytes;

use crate::{fs::{FileOpenOptions, IFileSystem}, result::Result};

use super::{page::PageId, storage::IPagerStorageHandle, PagerMetadata, PAGER_HEADER_SIZE};

const PAGER_LOGS_HEADER_SIZE: u64 = 16;
const PAGER_LOGS_PAGER_HEADER_LOC: u64 = PAGER_LOGS_HEADER_SIZE;
const PAGER_LOGS_PAGES_BASE_LOC: u64 = PAGER_LOGS_PAGER_HEADER_LOC + PAGER_HEADER_SIZE;

/// Journal du système de pagination.
///
/// Le journal stocke les versions initiales des pages
/// modifiées lors d'une transaction sur le fichier paginé.
/// Cela permet de revenir en arrière en cas d'erreur.
pub struct PagerLogs<'fs, Fs: IFileSystem + 'fs>(Fs::File<'fs>);

impl<'fs, Fs> PagerLogs<'fs, Fs>
where
    Fs: IFileSystem + 'fs,
{
    /// Ouvre le journal
    pub fn open(path: &Fs::Path, fs: &'fs Fs) -> Result<Self> {
        let result = fs.open(
            path,
            FileOpenOptions::new().create(true).read(true).write(true),
        )
        .map(Self)?;

        Ok(result)
    }

    /// Annule les modifications appliquées au fichier paginé.
    pub fn rollback<Storage: IPagerStorageHandle>(&mut self, dest: &mut Storage) -> Result<()> {
        self.restore_page_header(dest)?;
        self.restore_pages(dest)
    }

    /// Ecrit l'entête du système de dans le journal
    pub fn log_pager_header<Storage: IPagerStorageHandle>(&mut self, src: &mut Storage) -> Result<()> {
        let mut buf: [u8; size_of::<PagerMetadata>()] = [0; size_of::<PagerMetadata>()] ;
        src.read_meta(PagerMetadata::mut_from_bytes(&mut buf).unwrap());

        self.0.seek(io::SeekFrom::Start(PAGER_LOGS_PAGER_HEADER_LOC))?;
        self.0.write_all(&buf)?;

        Ok(())
    }

    /// Ecrit une page dans le journal
    pub fn log_page(&mut self, pid: &PageId, page: &[u8]) -> io::Result<()> {
        self.write_page_size(page.len().try_into().unwrap())?;
        let ps: u64 = page.len().try_into().unwrap();
        let loc = self.inc_page_count()? * ps;

        self.0.seek(io::SeekFrom::Start(PAGER_LOGS_PAGES_BASE_LOC + loc))?;
        self.0.write_u64::<LittleEndian>((*pid).into())?;
        self.0.write_all(page)
    }
}

impl<Fs> PagerLogs<'_, Fs>
where
    Fs: IFileSystem,
{
    fn read_page_size(&mut self) -> io::Result<u64> {
        self.0.seek(io::SeekFrom::Start(0))
    }

    fn write_page_size(&mut self, page_size: u64) -> io::Result<()> {
        self.0.seek(io::SeekFrom::Start(8))?;
        self.0.write_u64::<LittleEndian>(page_size)
    }

    fn read_page_count(&mut self) -> io::Result<u64> {
        self.0.seek(io::SeekFrom::Start(8))
    }

    fn write_page_count(&mut self, count: u64) -> io::Result<()> {
        self.0.seek(io::SeekFrom::Start(8))?;
        self.0.write_u64::<LittleEndian>(count)
    }

    fn inc_page_count(&mut self) -> io::Result<u64> {
        let count = self.read_page_count()? + 1;
        self.write_page_count(count)?;
        Ok(count - 1)
    }

    /// Restaure l'entête journalisé du pager
    fn restore_page_header<Storage: IPagerStorageHandle>(&mut self, dest: &mut Storage) -> Result<()> {
        let mut buf: [u8; size_of::<PagerMetadata>()] = [0; size_of::<PagerMetadata>()] ;

        self.0.seek(io::SeekFrom::Start(PAGER_LOGS_PAGER_HEADER_LOC))?;
        self.0.read_exact(&mut buf)?;

        dest.write_meta(PagerMetadata::ref_from_bytes(&buf).unwrap())
    }

    /// Restaure les pages journalisées.
    fn restore_pages<Storage: IPagerStorageHandle>(&mut self, dest: &mut Storage) -> Result<()> {
        let page_count = self.read_page_count()?;
        let page_size = self.read_page_size()?;
        let mut buf: Box<[u8]> = Box::from(
            iter::repeat(0u8)
                .take(page_size.try_into().unwrap())
                .collect::<Vec<_>>(),
        );

        for i in 0..page_count {
            let loc = i * page_size + PAGER_LOGS_PAGES_BASE_LOC;
            self.0.seek(io::SeekFrom::Start(loc))?;
            self.0.read_exact(&mut buf)?;

            dest.write_page(&PageId::from(i), &buf)?;
        }

        Ok(())
    }
}

