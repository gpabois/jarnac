use std::ops::{Deref, DerefMut};


/// Permet d'exécuter un flip Result<Option> vers Option<Result>
pub trait Flip {
    type To;
    
    fn flip(self) -> Self::To;
}

impl<T, E> Flip for std::result::Result<Option<T>, E> {
    type To = Option<std::result::Result<T, E>>;
    
    fn flip(self) -> Self::To {
        self.map_or(None, |v| v.map(Ok))
    }
}
