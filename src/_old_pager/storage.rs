use std::io::{Read, Seek, Write};

use zerocopy::IntoBytes;

use crate::{fs::{FileOpenOptions, FilePtr, IFileSystem}, result::Result};

use super::{page::{PageId, PageLocation, PageSize}, PagerMetadata, PAGER_BASE};

pub type StorageOpenOptions = FileOpenOptions;

pub trait IPagerStorage {
    /// Ouvre le stockage
    fn open(&self, options: StorageOpenOptions) -> Result<PagerStorageHandle<'_>>;
}

pub trait IPagerStorageHandle {
    fn write_meta(&mut self, src: &PagerMetadata) -> Result<()>;
    fn read_meta(&mut self, dest: &mut PagerMetadata) -> Result<()>;

    fn write_page(&mut self, id: &PageId, src: &[u8]) -> Result<()>;
    fn read_page(&mut self, id: &PageId, dest: &mut [u8]) -> Result<()>;   
}

pub type PagerStorage = Box<dyn IPagerStorage>;
pub type PagerStorageHandle<'file> = Box<dyn IPagerStorageHandle + 'file>;

pub struct FsPagerStorage<Fs>(FilePtr<Fs>) where Fs: IFileSystem + 'static;

impl<Fs> FsPagerStorage<Fs> where Fs: IFileSystem + 'static {
    pub fn new<Path>(fs: Fs, path: Path) -> Self where Fs::Path: From<Path>{
        Self(FilePtr::new(fs, path))
    }

    pub fn into_boxed(self) -> PagerStorage {
        Box::new(self)
    }
}

impl<Fs> IPagerStorage for FsPagerStorage<Fs> where Fs: IFileSystem + 'static {
    fn open(&self, options: StorageOpenOptions) -> Result<PagerStorageHandle<'_>> {
        let hdl = FsPagerStorageHandle::<Fs>(self.0.open(options)?).into_boxed();
        Ok(hdl)
    }
}

pub struct FsPagerStorageHandle<'file, Fs>(Fs::File<'file>) where Fs: IFileSystem + 'static;


impl<Fs> IPagerStorageHandle for FsPagerStorageHandle<'_, Fs> where Fs: IFileSystem {
    fn write_meta(&mut self, src: &PagerMetadata) -> Result<()> {
        self.0.seek(std::io::SeekFrom::Start(0))?;
        self.0.write_all(src.as_bytes())?;
        Ok(())
    }

    fn read_meta(&mut self, dest: &mut PagerMetadata) -> Result<()> {
        self.0.seek(std::io::SeekFrom::Start(0))?;
        self.0.read_exact(dest.as_mut_bytes())?;
        Ok(())
    }

    fn write_page(&mut self, pid: &PageId, src: &[u8]) -> Result<()> {
        let loc = self.loc(*pid, PageSize::from(src.len()));
        self.0.seek(std::io::SeekFrom::Start(loc.into()))?;
        self.0.write_all(src)?;

        Ok(())
    }

    fn read_page(&mut self, pid: &PageId, dest: &mut [u8]) -> Result<()> {
        let loc = self.loc(*pid, PageSize::from(dest.len()));
        self.0.seek(std::io::SeekFrom::Start(loc.into()))?;
        self.0.read_exact(dest)?;
        Ok(())
    }
}

impl<'fs, Fs> FsPagerStorageHandle<'fs, Fs> where Fs: IFileSystem {
    fn loc(&self, pid: PageId, size: PageSize) -> PageLocation {
        (pid * size) + PAGER_BASE
    }

    pub fn into_boxed(self) -> PagerStorageHandle<'fs> {
        Box::new(self)
    }
}

