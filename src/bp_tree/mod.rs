//! Arbre B+
//! 
//! Le système permet d'indexer une valeur de taille variable ou fixe avec une clé signée/non-signée d'une taille d'au plus 64 bits (cf [crate::value::numeric]).
mod interior;
mod leaf;

use interior::*;
use leaf::*;

use zerocopy::{IntoBytes, TryFromBytes};
use zerocopy_derive::{Immutable, KnownLayout, TryFromBytes};

use crate::{
    pager::{
        cell::{Cell, CellPage, CellPageHeader, GlobalCellId}, 
        page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, PageSlice, RefPageSlice}, 
        spill::VarHeader, 
        IPager, 
        PagerResult
    },
    value::numeric::{IntoNumericSpec, Numeric, NumericKind},
};

pub type BPTreeKey = Numeric;

/// Calcule les paramètres des arbres B+
fn compute_b_plus_tree_parameters(
    page_size: PageSize,
    key_spec: NumericKind,
    data_size: Option<u16>,
) -> BPlusTreeHeader {
    let key_size = key_spec.size();
    let u32_key_size = u16::from(key_size);

    let u32_btree_interior_page_header_size = u16::try_from(size_of::<BPTreeInteriorPageHeader>()).unwrap();
    let u32_btree_leaf_page_header_size = u16::try_from(size_of::<BPTreeLeafPageHeader>()).unwrap();
    let u32_page_id_size = u16::try_from(size_of::<PageId>()).unwrap();
    let u32_dsd = u16::try_from(size_of::<VarHeader>()).unwrap();

    // taille d'une cellule d'un noeud intérieur.
    let interior_cell_size = u32_key_size + u32_page_id_size;
    // taille de l'entête d'une cellule de la feuille
    let leaf_cell_header_size = u32_dsd + u32_key_size;

    // deux choses à calculer :
    // 1. la taille d'une cellule stockant les clés pour les noeuds intermédiaires ;
    // 2. la taille d'une cellule stockant les clés/valeurs pour les feuilles ;

    // Taille maximale de l'espace des cellules des noeuds intérieurs
    let interior_cell_size_space = page_size - u32_btree_interior_page_header_size;

    // On détermine K le nombre de clés possibles dans un noeud.
    // K1: Nombre de clés en calculant sur la base de la taille réservée aux cellules des noeuds intérieurs.
    // K2: Nombre de clés en calculant sur la base de la taille réservée aux cellules des feuilles.
    let k1 = u16::div_ceil(
        interior_cell_size_space,
        interior_cell_size,
    );

    // On démarre hypothèse K = K1.
    let mut k = k1;

    let node_leaf_size_space = page_size - u32_btree_leaf_page_header_size;

    // Deux grands cas : on a passé une taille de données, donc on peut voir lequel des deux K est le plus utile.
    // Pas de taille donnée, donc on prend K = K1, et on calcule la taille maximale admissible des données
    // Une taille donnée, et là :
    // - Si K2 < 2, ça ne vaut pas le coups, on recalcule en prenant K1
    // - Si K2 >= 2, alors on doit vérifier qu'on ne dépasse pas la taille allouée pour les cellules des noeuds intérieurs.
    let data_size = if let Some(data_size) = data_size {
        let k2 = u16::div_ceil(node_leaf_size_space, u32_key_size + data_size);
        
        // On a un K >= 2, on peut tenter le coup, l'avantage est de ne pas avoir de débordement possible.
        // On doit juste vérifier que ça tiendra avec les noeuds intérieurs
        if k2 >= 2 {
            // Avec K=K2, on recalcule l'espace que ça va occuper pour les noeuds intérieurs.
            // Si cela excède la taille maximale, on retient K=K1, 
            // et on recalcule la taille de la valeur stockable en cellule avant débordement.
            // Sinon
            if interior_cell_size_space < k2 * interior_cell_size {
                u16::div_ceil(
                    node_leaf_size_space - k * leaf_cell_header_size,
                    k,
                )
            } else {
                k = k2;
                data_size
            }
        } else {
            u16::div_ceil(
                node_leaf_size_space - k * leaf_cell_header_size,
                k,
            )
        }
    } else {
        u16::div_ceil(
            node_leaf_size_space - k * leaf_cell_header_size,
            k,
        )
    };

    let interior_cell_size = k * interior_cell_size;
    let leaf_cell_size = k * (leaf_cell_header_size + data_size);

    BPlusTreeHeader {
        kind: BPlusTreePageKind::Kind,
        interior_cell_size,
        leaf_cell_size,
        key_spec,
        data_size,
        k: u16::try_from(k).unwrap(),
        root: None.into(),
    }
}

/// Arbre B+
pub struct BPlusTree<'pager, Pager: IPager, Page: 'pager> where Page: AsRefPageSlice {
    pager: &'pager Pager,
    page: Page,
}

impl<'pager, Pager> BPlusTree<'pager, Pager, MutPage<'pager>>
where
    Pager: IPager,
{
    /// Crée un nouvel arbre B+
    ///
    /// key_size: puissance de 2 (2^key_size), maximum 3
    /// key_signed: la clé est signée +/-
    /// data_size: la taille de la donnée à stocker, None si la taille est dynamique ou indéfinie.
    pub fn new<Key: IntoNumericSpec>(
        pager: &'pager Pager,
        data_size: Option<u16>,
    ) -> PagerResult<Self> {
        let key_spec = Key::kind();
        let header = compute_b_plus_tree_parameters(pager.page_size(), key_spec, data_size);
        
        let pid = pager.new_page()?;
        let mut page = pager.get_mut_page(&pid)?;
        page.fill(0);
        page.as_mut_bytes()[0] = PageKind::BPlusTree as u8;

        let bp_page = BPlusTreePage::try_mut_from_bytes(&mut page).unwrap();
        bp_page.header = header;

        Ok(Self { pager, page })
    }

}

impl<Pager, Page> AsRef<BPlusTreePage> for BPlusTree<'_, Pager, Page> 
where
    Pager: IPager,
    Page: AsRefPageSlice {
    
    fn as_ref(&self) -> &BPlusTreePage {
        BPlusTreePage::try_ref_from_bytes(self.page.as_ref()).unwrap()
    }
}

impl<Pager, Page> AsMut<BPlusTreePage> for BPlusTree<'_, Pager, Page> 
where
    Pager: IPager,
    Page: AsMutPageSlice {
    
    fn as_mut(&mut self) -> &mut BPlusTreePage {
        BPlusTreePage::try_mut_from_bytes(self.page.as_mut()).unwrap()
    }
}


impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager,
    Page: AsRefPageSlice
{

    fn node_kind(&self, pid: &PageId) -> PagerResult<BPTreeNodeKind> {
        let page = self.pager.get_page(pid)?;
        let node_page = BPTreeNodePage::try_ref_from_bytes(&page)?;
        Ok(node_page.kind)
    }

    pub fn search(&self, key: &BPTreeKey) -> PagerResult<Option<GlobalCellId>> {
        let maybe_pid = self.search_leaf(key)?;

        Ok(match maybe_pid {
            Some(pid) => {
                let leaf: BPTreeLeaf<_> = self.pager.get_page(&pid)?.into();

                let gid = leaf
                    .iter()
                    .filter(|cell| cell == key)
                    .map(|cell| GlobalCellId::new(pid, cell.cid()))
                    .next();
                
                gid        
            },
            None => None
        })
    }

    /// Recherche une feuille contenant potentiellement la clé
    fn search_leaf(&self, key: &Numeric) -> PagerResult<Option<PageId>> {
        let bp_page = self.as_ref();

        let mut current: Option<PageId> = bp_page.header.root.into();
        
        // Le type de la clé passée en argument doit être celle supportée par l'arbre.
        assert_eq!(key.kind(), &bp_page.header.key_spec, "wrong key type");

        while let Some(pid) = current.as_ref() {
            if self.node_kind(pid)? == BPTreeNodeKind::Leaf {
                return Ok(Some(*pid));
            } else {
                let interior: BPTreeInterior<_> = self.pager.get_page(pid)?.into();
                current = Some(interior.search_child(key))
            }
        }

        Ok(None)
    }

}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager,
    Page: AsMutPageSlice
{
    pub fn insert(&mut self, key: Numeric, value: &[u8]) -> PagerResult<()> {
        let key_spec = &self.as_ref().header.key_spec;

        assert_eq!(key.kind(), key_spec, "wrong key type");

        let pid = match self.search_leaf(&key)? {
            Some(pid) => {
                pid
            },
            None => {
                let leaf_pid = self.insert_leaf()?;
                self.as_mut().header.root = Some(leaf_pid).into();
                leaf_pid
            }
        };
        

        let mut leaf: BPTreeLeaf<_> = self.pager.get_mut_page(&pid)?.into();
        let before = leaf
            .iter()
            .filter(|cell| cell <= &key)
            .map(|cell| cell.cid())
            .last();
        

        let cid = match before {
            Some(before) => leaf.insert_before(&before)?,
            None => leaf.push()?
        };

        let mut cell: BPTreeLeafCell<_> = leaf.borrow_mut_cell(&cid);
        *cell.borrow_mut_key() = key;
        cell.borrow_mut_value().set(value, self.pager)?;        

        Ok(())
    }

    /// Divise un noeud
    /// 
    /// Cette opération est utilisée lors d'une insertion si le noeud est plein.
    fn split(&mut self, pid: &PageId) -> PagerResult<()> {
        match self.node_kind(pid)? {
            BPTreeNodeKind::Interior => {
                let interior: BPTreeInterior<_> = self.pager.get_mut_page(&pid)?.into();
                
                // on ne divise pas un noeud intérieur qui n'est pas plein.
                if !interior.is_full() {
                    return Ok(())
                }

                Ok(())
            },
            BPTreeNodeKind::Leaf => {
                let leaf: BPTreeLeaf<_> = self.pager.get_mut_page(&pid)?.into();
                
                // on ne divise pas un noeud qui n'est pas plein.
                if !leaf.is_full() {
                    return Ok(())
                }

                Ok(())
            },
        }
    }
    

    /// Insère une nouvelle feuille dans l'arbre.
    fn insert_leaf(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let page = self.pager.get_mut_page(&pid)?;

        BPTreeLeaf::new(page,
            self.as_ref().header.k.try_into().unwrap(),
            self.as_ref().header.interior_cell_size.into(), 
        );

        Ok(pid)
    }

    /// Insère un noeud intérieur dans l'arbre.
    fn insert_interior(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let page = self.pager.get_mut_page(&pid)?;

        BPTreeInterior::new(page, 
            self.as_ref().header.k.try_into().unwrap(), 
            self.as_ref().header.leaf_cell_size.into()
        );

        Ok(pid)
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
enum BPlusTreePageKind {
    Kind = PageKind::BPlusTree as u8,
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BPlusTreeHeader {
    kind: BPlusTreePageKind,
    /// Spécification de la clé (signée, taille, etc.)
    key_spec: NumericKind,
    /// Taille d'une cellule d'une feuille
    leaf_cell_size: u16,
    /// Taille d'une cellule d'un noeud intérieur
    interior_cell_size: u16,
    /// La taille de la donnée stockable dans une cellule d'une feuille
    data_size: u16,
    /// Nombre maximum de clés dans l'arbre B+
    k: u16,
    /// Pointeur vers la racine
    root: OptionalPageId,
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// La page contenant la définition de l'arbre B+.
pub struct BPlusTreePage {
    header: BPlusTreeHeader,
    body: [u8],
}

#[derive(TryFromBytes, KnownLayout, Immutable, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
enum BPTreeNodeKind {
    #[allow(dead_code)]
    Interior = PageKind::BPlusTreeInterior as u8,
    #[allow(dead_code)]
    Leaf = PageKind::BPlusTreeLeaf as u8,
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BPTreeNodePage {
    kind: BPTreeNodeKind,
    body: [u8]
}
