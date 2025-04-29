use zerocopy::LE;

use crate::{knack::document::Document, page::PageId, tag::JarId};


/// Description d'un pot de trucs
pub struct JarMeta {
    /// Identifiant du pot
    id: JarId,
    /// Tête de la liste des pages libres
    freelist: Option<PageId>,
    /// Nombre de pages du pot
    len: zerocopy::U64<LE>,
}
