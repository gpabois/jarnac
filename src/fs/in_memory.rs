use std::{
    borrow::Borrow,
    cell::{RefCell, UnsafeCell},
    collections::HashMap,
    fmt::Display,
    io::{self, Cursor, ErrorKind, Read, Seek, Write},
    ops::Deref,
    pin::Pin,
    ptr::NonNull,
    rc::Rc,
};

use super::{FileOpenOptions, IFileSystem, IPath};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InMemoryPath(String);

impl From<&str> for InMemoryPath {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl Display for InMemoryPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for InMemoryPath {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for InMemoryPath {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for InMemoryPath {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl InMemoryPath {
    fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split("/")
    }
}

impl IPath for InMemoryPath {
    fn parent(&self) -> Self {
        let mut segments = self.segments().collect::<Vec<_>>();
        segments.pop();
        Self(segments.join("/"))
    }

    fn join(&self, rhs: Self) -> Self {
        let mut segments = self.segments().collect::<Vec<_>>();
        segments.extend(rhs.segments());
        Self(segments.join("/"))
    }

    fn append(&self, path: &str) -> Self {
        let mut segments = self.segments().collect::<Vec<_>>();
        segments.push(path);
        Self(segments.join("/"))
    }

    fn tail(&self) -> String {
        let mut segments = self.segments().collect::<Vec<_>>();
        segments.pop().unwrap_or_default().to_string()
    }
}

struct FileData(Pin<Box<UnsafeCell<Vec<u8>>>>);

impl Default for FileData {
    fn default() -> Self {
        Self(Box::pin(UnsafeCell::new(Vec::default())))
    }
}

impl FileData {
    unsafe fn get_mut_ptr(&self) -> NonNull<Vec<u8>> {
        NonNull::new(self.0.get() as *mut _).unwrap()
    }
}

pub struct InMemoryFile<'fs>(Cursor<&'fs mut Vec<u8>>);

impl Seek for InMemoryFile<'_> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

impl Read for InMemoryFile<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for InMemoryFile<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

#[derive(Default)]
pub struct InMemoryFs(RefCell<HashMap<String, FileData>>);

impl IFileSystem for Rc<InMemoryFs> {
    type File<'fs> = <InMemoryFs as IFileSystem>::File<'fs>;
    type Path = InMemoryPath;

    fn open<'fs>(
        &'fs self,
        path: &Self::Path,
        options: FileOpenOptions,
    ) -> io::Result<Self::File<'fs>> {
        self.deref().open(path, options)
    }

    fn rm(&self, path: &Self::Path) -> io::Result<()> {
        self.deref().rm(path)
    }

    fn exists(&self, path: &Self::Path) -> bool {
        self.deref().exists(path)
    }
}

impl IFileSystem for InMemoryFs {
    type File<'fs> = InMemoryFile<'fs>;
    type Path = InMemoryPath;

    fn open<'fs>(
        &'fs self,
        path: &Self::Path,
        options: FileOpenOptions,
    ) -> std::io::Result<Self::File<'fs>> {
        let mut map = self.0.borrow_mut();

        if !map.contains_key(path.as_ref()) {
            if options.is_create() {
                map.insert(path.to_string(), FileData::default());
            } else {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("file {path} does not exist"),
                ));
            }
        }

        map.get(path.as_ref())
            .map(|data| unsafe { InMemoryFile(Cursor::new(data.get_mut_ptr().as_mut())) })
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn rm(&self, path: &Self::Path) -> std::io::Result<()> {
        self.0.borrow_mut().remove(path.as_ref());
        Ok(())
    }

    fn exists(&self, path: &Self::Path) -> bool {
        self.0.borrow().contains_key(path.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

    use crate::fs::{FileOpenOptions, IFileSystem};

    use super::{InMemoryFs, InMemoryPath};

    #[test]
    fn test_read_write() -> Result<(), Box<dyn Error>> {
        let fs = InMemoryFs::default();

        fs.open(
            &InMemoryPath::from("test"),
            FileOpenOptions::default().create(true).write(true),
        )?
        .write_u64::<LittleEndian>(0x1234)?;

        assert_eq!(
            fs.open(
                &InMemoryPath::from("test"),
                FileOpenOptions::default().read(true)
            )?
            .read_u64::<LittleEndian>()?,
            0x1234
        );

        Ok(())
    }
}

