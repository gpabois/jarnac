use std::{error::Error, fmt::Display, io};

use super::page::{PageId, PageKind};

#[derive(Debug)]
pub enum PagerError {
    CacheFull,
    UnexistingPage(PageId),
    PageAlreadyCached,
    PageAlreadyBorrowed,
    InvalidPageKind,
    InvalidFormat,
    WrongPageKind{expected: PageKind, got: PageKind},
    IoError(io::Error),
}

impl Display for PagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PagerError::CacheFull => write!(f, "pager cache is full"),
            PagerError::UnexistingPage(id) => write!(f, "page {id} does not exist"),
            PagerError::PageAlreadyCached => write!(f, "page is already cached"),
            PagerError::PageAlreadyBorrowed => write!(f, "page is already borrowed"),
            PagerError::InvalidPageKind => write!(f, "unknown page kind"),
            PagerError::InvalidFormat => write!(f, "invalid pager format"),
            PagerError::WrongPageKind { expected, got } => write!(f, "wrong page kind, expecting {0}, got {1}", expected, got),
            PagerError::IoError(_) => write!(f, "an io error occured"),
        }
    }
}

impl Error for PagerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Self::IoError(error) = self {
            return Some(error)
        }

        None
    }
}

impl From<io::Error> for PagerError {
    fn from(value: io::Error) -> Self {
        Self::IoError(value)
    }
}