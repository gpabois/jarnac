use std::{cell::UnsafeCell, collections::HashMap, io::{self, Cursor}, ops::Deref, rc::Rc};

use super::IFileSystem;


pub struct InMemoryFs(UnsafeCell<HashMap<String, Vec<u8>>>);

impl IFileSystem for Rc<InMemoryFs> {
  type File<'fs> = Cursor<&'fs mut Vec<u8>>; 

  fn open<'fs>(&'fs self, path: &str) -> io::Result<Self::File<'fs>> {
    self.deref().open(path)
  }
  
  fn directory(&self, pth: &str) -> String {
    self.deref().directory(pth)
  }
  
  fn join(&self, lhs: &str, rhs: &str) -> String {
    self.deref().join(lhs, rhs)  
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
  
  fn directory(&self, pth: &str) -> String {
    let opth: String = pth.to_owned();
    let mut segments: Vec<_> = opth.split("/").collect();
    segments.pop();
    segments.join("/")
  }
  
  fn join(&self, lhs: &str, rhs: &str) -> String {
    Vec::<String>::from([lhs.to_owned(), rhs.to_owned()]).join("/")
  }
}