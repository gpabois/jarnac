use std::{cell::RefCell, collections::HashMap, io::{self, Cursor}, ops::Deref, pin::Pin, rc::Rc};

use super::IFileSystem;



pub struct InMemoryFs(RefCell<HashMap<String, Pin<Vec<u8>>>>);

impl IFileSystem for Rc<InMemoryFs> {
  type File<'fs> = Cursor<&'fs mut Vec<u8>>; 

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
}

impl InMemoryFs {
  pub fn new() -> Self {
    Self(RefCell::new(HashMap::default()))
  }
}

impl IFileSystem for InMemoryFs {
    type File<'fs> = Cursor<&'fs mut Vec<u8>>;

    fn open<'fs>(&'fs self, path: &str) -> std::io::Result<Self::File<'fs>> {
        let mut map =  self.0.borrow_mut();
        
        let pinned = Pin::new(Vec::<u8>::default());
        
        if map.contains_key(path) == false {
            map.insert(path.to_owned(), pinned);
        }
        
        map.get_mut(path).map(Cursor::new).ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "not found"))
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
}