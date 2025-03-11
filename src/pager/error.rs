use std::{
    backtrace::Backtrace,
    error::Error,
    fmt::{Debug, Display},
    io,
};

use zerocopy::TryFromBytes;

use super::page::{PageId, PageKind};

pub struct PagerError {
    pub backtrace: Backtrace,
    kind: PagerErrorKind,
}

impl Display for PagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl Debug for PagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagerError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl PagerError {
    pub fn new(kind: PagerErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }
}

impl Error for PagerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let PagerErrorKind::IoError(error) = &self.kind {
            return Some(error);
        }

        None
    }
}

impl<Src, Dest> From<zerocopy::TryCastError<Src, Dest>> for PagerError 
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

impl From<io::Error> for PagerError {
    fn from(value: io::Error) -> Self {
        Self::new(PagerErrorKind::IoError(value))
    }
}

#[derive(Debug)]
/// Représente une erreur du système de pagination.
pub enum PagerErrorKind {
    CacheFull,
    UnexistingPage(PageId),
    PageAlreadyCached(PageId),
    PageNotCached(PageId),
    PageCurrentlyBorrowed,
    InvalidPageKind(u8),
    InvalidFormat,
    SpilledVar,
    WrongPageKind { expected: PageKind, got: PageKind },
    CellPageOverflow,
    CellPageFull,
    IoError(io::Error),
}

impl Display for PagerErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PagerErrorKind::CacheFull => write!(f, "pager cache is full"),
            PagerErrorKind::UnexistingPage(id) => write!(f, "page {id} does not exist"),
            PagerErrorKind::PageAlreadyCached(id) => write!(f, "page {id} is already cached"),
            PagerErrorKind::PageCurrentlyBorrowed => write!(f, "page is already borrowed"),
            PagerErrorKind::InvalidPageKind(invalid_kind) => write!(f, "unknown page kind, got {0}", invalid_kind),
            PagerErrorKind::InvalidFormat => write!(f, "invalid pager format"),
            PagerErrorKind::WrongPageKind { expected, got } => {
                                write!(f, "wrong page kind, expecting {0}, got {1}", expected, got)
                            }
            PagerErrorKind::IoError(_) => write!(f, "an io error occured"),
            PagerErrorKind::PageNotCached(id) => write!(f, "page {id} not cached"),
            PagerErrorKind::CellPageFull => write!(f, "cell page is full"),
            PagerErrorKind::SpilledVar => write!(f, "var data has spilled"),
            PagerErrorKind::CellPageOverflow => write!(f, "expected cell space overflows allocated page space"),
        }
    }
}
