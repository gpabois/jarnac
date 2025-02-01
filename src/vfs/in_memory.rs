use std::{cell::UnsafeCell, collections::HashMap, io::{self, Cursor}, ops::Deref, rc::Rc};

use super::IFileSystem;


pub struct InMemoryFs(UnsafeCell<HashMap<String, Vec<u8>>>);

impl IFileSystem for Rc<InMemoryFs> {
  type File<'fs> = Cursor<&'fs mut Vec<u8>>; 

  fn open<'fs>(&'fs self, path: &str) -> io::Result<Self::File<'fs>> {
    self.deref().open(path)
  }
}

impl InMemoryFs {
  pub fn new() -> Self {
    Self(UnsafeCell::new(HashMap::default()))
  }
}

impl IFileSystem for InMemoryFs {
  type File<'fs> = Cursor<&'fs mut Vec<u8>>;

  fn open<'fs>(&'fs self, path: &str) -> std::io::Result<Self::File<'fs>> {
    unsafe {
      let map =  self.0.get().as_mut().unwrap();
      if map.contains_key(path) == false {
        map.insert(path.to_owned(), Vec::default());
      }
      
      map.get_mut(path).map(Cursor::new).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not found"))
    }
  }
}