use std::io::{self, Read, Result, Seek, Write};

pub mod in_memory;

pub trait IPath: Clone + PartialEq + ToString {
    /// Retourne le répertoire à partir du chemin.
    fn parent(&self) -> Self;

    /// Joint deux bouts de chemin ensemble
    fn join(&self, rhs: Self) -> Self;

    /// Ajoute un chemin
    fn append(&self, path: &str) -> Self;

    /// Retourne le dernier élément du chemin
    fn tail(&self) -> String;

    /// Modifie le stem du chemin
    fn modify_stem<F: FnOnce(&str) -> String>(&self, modifier: F) -> Self {
        let parent = self.parent();
        let extension = self.extension().unwrap_or_else(|| "".to_owned());
        let stem = modifier(&self.stem());

        parent.append(&format!("{stem}{extension}"))
    }

    /// Retourne le nom du fichier sans l'extension.
    fn stem(&self) -> String {
        let tail = self.tail();
        let mut parts = tail.split(".").collect::<Vec<_>>();

        if parts.len() <= 1 {
            return parts.join(".");
        }

        parts.pop();
        parts.join(".")
    }

    /// Retourne l'extension du fichier s'il existe.
    fn extension(&self) -> Option<String> {
        let tail = self.tail();
        let parts = tail.split(".").collect::<Vec<_>>();

        if parts.len() <= 1 {
            return None;
        }

        Some(parts.last().unwrap().to_string())
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub struct FileOpenOptions(u8);

impl FileOpenOptions {
    const CREATE_FLAG: u8 = 0b1;
    const READ_FLAG: u8 = 0b01;
    const WRITE_FLAG: u8 = 0b100;

    pub fn new() -> Self {
        Self(0)
    }

    pub fn is_read(&self) -> bool {
        self.0 & Self::READ_FLAG == Self::READ_FLAG
    }

    pub fn is_create(&self) -> bool {
        self.0 & Self::CREATE_FLAG == Self::CREATE_FLAG
    }

    pub fn is_write(&self) -> bool {
        self.0 & Self::READ_FLAG == Self::READ_FLAG
    }

    pub fn create(self, value: bool) -> Self {
        if value {
            Self(self.0 | Self::CREATE_FLAG)
        } else {
            Self(self.0 & !Self::CREATE_FLAG)
        }
    }

    pub fn read(self, value: bool) -> Self {
        if value {
            Self(self.0 | Self::READ_FLAG)
        } else {
            Self(self.0 & !Self::READ_FLAG)
        }
    }

    pub fn write(self, value: bool) -> Self {
        if value {
            Self(self.0 | Self::WRITE_FLAG)
        } else {
            Self(self.0 & !Self::WRITE_FLAG)
        }
    }
}

/// Interface vers un système de fichier.
pub trait IFileSystem {
    type File<'fs>: Seek + Write + Read where Self: 'fs;
    type Path: IPath;

    /// Ouvre le fichier.
    fn open<'fs>(&'fs self, path: &Self::Path, options: FileOpenOptions)
        -> Result<Self::File<'fs>>;

    /// Le noeud du système de fichier existe.
    fn exists(&self, path: &Self::Path) -> bool;

    /// Supprime le fichier/répertoire
    fn rm(&self, path: &Self::Path) -> std::io::Result<()>;
}

/// Un pointeur vers un fichier dans un système de fichier.
///
/// Permet d'éviter d'avoir un tuple (path, fs) à répercuter partout ailleurs.
pub struct FilePtr<Fs>
where
    Fs: IFileSystem,
{
    pub path: Fs::Path,
    pub fs: Fs,
}

impl<Fs> FilePtr<Fs>
where
    Fs: IFileSystem,
{
    pub fn new<Path: Into<Fs::Path>>(fs: Fs, path: Path) -> Self {
        Self {
            path: path.into(),
            fs,
        }
    }

    pub fn open(&self, options: FileOpenOptions) -> io::Result<Fs::File<'_>> {
        self.fs.open(&self.path, options)
    }

    pub fn exists(&self) -> bool {
        self.fs.exists(&self.path)
    }
}

