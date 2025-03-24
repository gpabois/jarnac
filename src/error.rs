use std::{
    backtrace::Backtrace,
    error::Error as IError,
    fmt::{Debug, Display},
    io,
};

use zerocopy::TryFromBytes;

use crate::{tag::JarTag, value::ValueKind};
use crate::pager::page::{PageId, PageKind};

pub struct Error {
    pub backtrace: Backtrace,
    pub kind: ErrorKind,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagerError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl Error {
    pub fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }
}

impl IError for Error {
    fn source(&self) -> Option<&(dyn IError + 'static)> {
        if let ErrorKind::IoError(error) = &self.kind {
            return Some(error);
        }

        None
    }
}

impl<Src, Dest> From<zerocopy::TryCastError<Src, Dest>> for Error 
where Dest: TryFromBytes + ?Sized
{
    fn from(value: zerocopy::TryCastError<Src, Dest>) -> Self {
        match value {
            zerocopy::ConvertError::Alignment(_) => todo!(),
            zerocopy::ConvertError::Size(_) => todo!(),
            zerocopy::ConvertError::Validity(_) => todo!(),
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::new(ErrorKind::IoError(value))
    }
}

#[derive(Debug)]
/// Représente une erreur du système de pagination.
pub enum ErrorKind {
    BufferFull,
    UnexistingPage(JarTag),
    PageAlreadyCached(JarTag),
    PageNotCached(JarTag),
    PageCurrentlyBorrowed,
    PageLoadingFailed {
        tag: JarTag, 
        source: Box<Error>
    },
    InvalidPageKind(u8),
    InvalidFormat,
    SpilledVar,
    WrongPageKind { expected: PageKind, got: PageKind },
    CellPageOverflow,
    CellPageFull,
    WrongValueKind {expected: ValueKind, got: ValueKind},
    IoError(io::Error),
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::BufferFull => write!(f, "pager cache is full"),
            ErrorKind::UnexistingPage(id) => write!(f, "page {id} does not exist"),
            ErrorKind::PageAlreadyCached(id) => write!(f, "page {id} is already cached"),
            ErrorKind::PageCurrentlyBorrowed => write!(f, "page is already borrowed"),
            ErrorKind::InvalidPageKind(invalid_kind) => write!(f, "unknown page kind, got {0}", invalid_kind),
            ErrorKind::InvalidFormat => write!(f, "invalid pager format"),
            ErrorKind::WrongPageKind { expected, got } => {
                                                write!(f, "wrong page kind, expecting {0}, got {1}", expected, got)
                                            }
            ErrorKind::IoError(_) => write!(f, "an io error occured"),
            ErrorKind::PageNotCached(id) => write!(f, "page {id} not cached"),
            ErrorKind::CellPageFull => write!(f, "cell page is full"),
            ErrorKind::SpilledVar => write!(f, "var data has spilled"),
            ErrorKind::CellPageOverflow => write!(f, "cell space overflows allocated page space"),
            ErrorKind::WrongValueKind { expected, got } => write!(f, "expecting value type {expected}, got {got} instead"),
            ErrorKind::PageLoadingFailed { tag: id, source } => write!(f, "failed to load page {id}, reason: {source}"),
        }
    }
}
