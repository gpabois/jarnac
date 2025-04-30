use zerocopy::LE;

use crate::{knack::{builder::KnackBuilder, document::{DocBuilder, DocCow, Document}}, page::PageId, pager::Pager, prelude::IntoKnackBuf, tag::JarId};

pub struct Jar<'buf>(Pager<'buf>);

#[repr(C, packed)]
/// Metadonnées d'un pot
pub struct JarMeta {
    /// Identifiant du pot
    id: JarId,
    /// Tête de la liste des pages libres
    freelist: Option<PageId>,
    /// Nombre de pages du pot
    len: zerocopy::U64<LE>,
}

/// Description d'un pot (contient les indexes)
/// 
/// # Structure
/// - name: Nom du pot ;
/// - indexes : Dictionnaire de [JarIndex] ;
/// - schema (optional) : JarSchema
pub struct JarDescription<'a>(DocCow<'a>);

impl JarDescription<'_> {
    /// Créé une nouvelle description du pot.
    pub fn new(name: &str) -> Self {
        let mut doc = DocBuilder::default();
        
        doc.insert("indexes", DocBuilder::default());
        doc.insert("name", name);

        Self(doc.into())
    }
}

//// Description d'un index 
/// 
/// # Structure :
/// - name: Nom de l'index ;
/// - kind: Type d'index (BPlusTree, ...) ;
/// - fields: Liste de champs [KnackPath] indexés ;
/// - unique: Oblige chaque entrée de l'index a ne posséder qu'une seule valeur.
pub struct JarIndex<'a>(DocCow<'a>);

/// Description d'un schéma
pub struct JarSchema<'a>(DocCow<'a>);