use std::ops::{Deref, Range};

pub trait Shift<T> {
    fn shift(self, value: T) -> Self;
}

impl Shift<usize> for Range<usize> {
    fn shift(self, value: usize) -> Self {
        (value + self.start)..(value + self.end)
    }
}

/// Permet d'exécuter un flip Result<Option> vers Option<Result>
pub trait Flip {
    type To;
    
    fn flip(self) -> Self::To;
}

impl<T, E> Flip for Option<std::result::Result<T,E>> {
    type To = std::result::Result<Option<T>, E>;
    
    fn flip(self) -> Self::To {
        self.map_or(Ok(None), |v| v.map(Some))
    }
}


impl<T, E> Flip for std::result::Result<Option<T>, E> {
    type To = Option<std::result::Result<T, E>>;
    
    fn flip(self) -> Self::To {
        self.map_or(None, |v| v.map(Ok))
    }
}

/// Type utilisé pour assurer des types de données valides.
pub struct Valid<T>(pub(crate) T);

impl<T> Valid<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

pub enum Either<L,R> {
    Left(L),
    Right(R)
}