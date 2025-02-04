use std::{cell::{RefCell, UnsafeCell}, collections::HashMap, io::{self, Cursor, Read, Seek, Write}, ops::Deref, pin::Pin, ptr::NonNull, rc::Rc};

use super::IFileSystem;

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

pub struct InMemoryFs(RefCell<HashMap<String, FileData>>);

impl IFileSystem for Rc<InMemoryFs> {
    type File<'fs> = <InMemoryFs as IFileSystem>::File<'fs>;

    fn open<'fs>(&'fs self, path: &str) -> io::Result<Self::File<'fs>> {
        self.deref().open(path)
    }

    fn delete(&self, path: &str) -> io::Result<()> {
        self.deref().delete(path)
    }
    
    fn directory(&self, pth: &str) -> String {
        self.deref().directory(pth)
    }
    
    fn join(&self, lhs: &str, rhs: &str) -> String {
        self.deref().join(lhs, rhs)  
    }
    
    fn exists(&self, path: &str) -> bool {
        self.deref().exists(path)
    }
}

impl InMemoryFs {
  pub fn new() -> Self {
    Self(RefCell::new(HashMap::default()))
  }
}

impl IFileSystem for InMemoryFs {
    type File<'fs> = InMemoryFile<'fs>;

    fn open<'fs>(&'fs self, path: &str) -> std::io::Result<Self::File<'fs>> {
        let mut map =  self.0.borrow_mut();
         
        if map.contains_key(path) == false {
            map.insert(path.to_owned(), FileData::default());
        }
        
        map.get(path).map(|data|  unsafe {InMemoryFile(Cursor::new(data.get_mut_ptr().as_mut())) }).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file not found"))
    }

    fn directory(&self, pth: &str) -> String {
        let opth: String = pth.to_owned();
        let mut segments: Vec<_> = opth.split("/").collect();
        segments.pop();
        segments.join("/")
    }
    
    fn join(&self, lhs: &str, rhs: &str) -> String {
        Vec::<String>::from([lhs.to_owned(), rhs.to_owned()]).join("/")
    }
    
    fn delete(&self, path: &str) -> std::io::Result<()> {
        self.0.borrow_mut().remove(path);
        Ok(())
    }
    
    fn exists(&self, path: &str) -> bool {
        self.0.borrow().contains_key(path)
    }
}