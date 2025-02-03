use std::io::{Read, Result, Seek, Write};

pub mod in_memory;

pub trait IFileSystem {
  type File<'fs>: Seek + Write + Read where Self: 'fs;

  /// Ouvre le fichier.
  fn open<'fs>(&'fs self, path: &str) -> Result<Self::File<'fs>>;

  /// Retourne le répertoire à partir du chemin.
  fn directory(&self, pth: &str) -> String;
  
  /// Joint deux bouts de chemin ensemble
  fn join(&self, lhs: &str, rhs: &str) -> String;
}


