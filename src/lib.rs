//! Base de données à documents.
//! 
//! Le cahier des charges actuel :
//! - Réalise ses opérations en étant [ACID](https://en.wikipedia.org/wiki/ACID) ;
//! - Permet d'indexer :
//!     + Géospatialement
//!     + Texte
//!     + Par valeur numérique d'une taille d'au plus 64 bits.
pub mod fs;
pub mod page;
pub mod var;
pub mod cell;
pub mod pager;
pub mod knack;
pub mod prelude;
pub mod error;
pub mod result;
pub mod utils;
pub mod jar;
pub mod buffer;
pub mod tag;
pub mod arena;
pub mod bpt;
pub mod free;
