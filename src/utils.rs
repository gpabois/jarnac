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
pub struct Valid<T>(T);

#[derive(Clone, Copy)]
/// Type utilisé pour assurer que le type de donnée ait une taille déterminée.
pub struct Sized<T>(pub(crate)T);