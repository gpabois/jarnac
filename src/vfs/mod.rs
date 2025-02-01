use std::io::{Read, Result, Seek, Write};

pub mod in_memory;

pub trait IFileSystem {
  type File<'fs>: Seek + Write + Read where Self: 'fs;
  fn open<'fs>(&'fs self, path: &str) -> Result<Self::File<'fs>>;
}


