//! Arbre B+
//! 
//! Le système permet d'indexer une valeur de taille variable ou fixe avec une clé signée/non-signée d'une taille d'au plus 64 bits (cf [crate::value::numeric]).
mod interior;
mod leaf;
pub mod cursor;

use interior::*;
use leaf::*;

use zerocopy::{IntoBytes, TryFromBytes};
use zerocopy_derive::{Immutable, KnownLayout, TryFromBytes};

use crate::{
    pager::{
        cell::GlobalCellId, 
        page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, RefPage}, 
        spill::VarHeader, 
        IPager, 
        PagerResult
    },
    value::numeric::{Numeric, NumericKind},
};

pub type BPTreeKey = Numeric;

/// Calcule les paramètres des arbres B+
fn compute_b_plus_tree_parameters(page_size: PageSize, key_spec: NumericKind, data_size: Option<u16>) -> BPlusTreeHeader {
    let key_size = key_spec.size();
    let u32_key_size = u16::from(key_size);

    let u32_btree_interior_page_header_size = u16::try_from(size_of::<BPTreeInteriorHeader>()).unwrap();
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

/// Identifiant d'une cellule contenant une paire clé/valeur.
pub type BPlusTreeCellId = GlobalCellId;

/// Trait permettant de manipuler un arbre B+ en lecture.
pub trait IRefBPlusTree {
    /// Cherche une cellule clé/valeur à partir de la clé passée en argument.
    fn search(&self, key: &Numeric) -> PagerResult<Option<BPlusTreeCellId>>;

    /// Cherche la cellule la plus proche dont la valeur est supérieure ou égale.
    fn search_nearest_ceil(&self, key: &Numeric) -> PagerResult<Option<BPlusTreeCellId>> {
        Ok(self.search_nearest_floor(key)?.and_then(|gcid| self.next_sibling(&gcid).unwrap()))
    }
    
    /// Cherche la cellule la plus proche dont la valeur est inférieure ou égale.
    fn search_nearest_floor(&self, key: &Numeric) -> PagerResult<Option<BPlusTreeCellId>>;

    /// Retourne la cellule clé/valeur la plus à gauche de l'arbre.
    fn head(&self) -> PagerResult<Option<BPlusTreeCellId>>;

    /// Retourne la cellule clé/valeur suivante.
    fn next_sibling(&self, gcid: &BPlusTreeCellId) -> PagerResult<Option<BPlusTreeCellId>>;
    
    /// Retourne la cellule clé/valeur précédente.
    fn prev_sibling(&self, gcid: &BPlusTreeCellId) -> PagerResult<Option<BPlusTreeCellId>>;

    /// Emprunte la cellule clé/valeur.
    fn borrow_cell(&self, gcid: &BPlusTreeCellId) -> PagerResult<BPTreeLeafCell<RefPage<'_>>>;
}

/// Trait permettant de manipuler un arbre B+ en écriture.
pub trait IMutBPlusTree: IRefBPlusTree {
    /// Insère une nouvelle valeur
    fn insert(&mut self, key: Numeric, value: &[u8]) -> PagerResult<()>;
}

/// Trait permettant d'obtenir une référence sur un arbre B+
/// 
/// Le trait permet d'éviter des types complexes.
pub trait AsBPlusTreeRef: AsRef<Self::BPlusTree> {
    type BPlusTree: IRefBPlusTree;
}

/// Arbre B+
pub struct BPlusTree<'pager, Pager: IPager, Page: 'pager> where Page: AsRefPageSlice {
    pager: &'pager Pager,
    page: Page,
}

impl<'pager, Pager, Page> IRefBPlusTree for BPlusTree<'pager, Pager, Page> where Pager: IPager, Page: AsRefPageSlice {

    fn search(&self, key: &BPTreeKey) -> PagerResult<Option<GlobalCellId>> {
        let maybe_pid = self.search_leaf(key)?;

        Ok(match maybe_pid {
            Some(pid) => {
                let leaf: BPTreeLeaf<_> = self.pager.borrow_page(&pid)?.into();

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

    fn search_nearest_floor(&self, key: &Numeric) -> PagerResult<Option<BPlusTreeCellId>> {
        let maybe_pid = self.search_leaf(key)?;

        
        Ok(match maybe_pid {
            Some(pid) => {
                let leaf: BPTreeLeaf<_> = self.pager.borrow_page(&pid)?.into();

                let gid = leaf
                    .iter()
                    .filter(|cell| cell <= key)
                    .map(|cell| GlobalCellId::new(pid, cell.cid()))
                    .next();
                
                gid        
            },
            None => None
        })
    }

    fn head(&self) -> PagerResult<Option<BPlusTreeCellId>> {
        Ok(self.search_head_leaf()?.and_then(|pid| {
            let leaf= self.borrow_leaf(&pid).unwrap();
            let head= leaf.iter().map(|cell| BPlusTreeCellId::new(pid, cell.cid())).next();
            head
        }))
    }

    fn next_sibling(&self, gcid: &BPlusTreeCellId) -> PagerResult<Option<BPlusTreeCellId>> {
        let leaf = self.borrow_leaf(gcid.pid())?;
        let cell =leaf.borrow_cell(gcid.cid());

        match cell.as_cell().next_sibling() {
            Some(cid) => Ok(Some(BPlusTreeCellId::new(*leaf.as_page().id(), *cid))),
            None => {
                Ok(leaf.header.next.as_ref()
                .and_then(|next_pid| 
                    self.borrow_leaf(&next_pid).unwrap()
                    .iter()
                    .map(|cell| BPlusTreeCellId::new(next_pid, cell.cid()))
                    .next()
                ))
            },
        }
    }

    fn prev_sibling(&self, gcid: &BPlusTreeCellId) -> PagerResult<Option<BPlusTreeCellId>> {
        let leaf = self.borrow_leaf(gcid.pid())?;
        let cell =leaf.borrow_cell(gcid.cid());

        match cell.as_cell().prev_sibling() {
            Some(cid) => Ok(Some(BPlusTreeCellId::new(*leaf.as_page().id(), *cid))),
            None => {
                Ok(leaf.header.prev.as_ref()
                .and_then(|prev_pid| 
                    self.borrow_leaf(&prev_pid).unwrap()
                    .iter()
                    .map(|cell| BPlusTreeCellId::new(prev_pid, cell.cid()))
                    .last()
                ))
            },
        }
    }

    fn borrow_cell(&self, gcid: &BPlusTreeCellId) -> PagerResult<BPTreeLeafCell<RefPage<'_>>> {
        todo!()
    }
    

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
    pub fn new(
        pager: &'pager Pager,
        key_kind: &NumericKind,
        data_size: Option<u16>,
    ) -> PagerResult<Self> {
        // on génère les données qui vont bien pour gérer notre arbre B+
        let header = compute_b_plus_tree_parameters(pager.page_size(), *key_kind, data_size);
        
        let pid = pager.new_page()?;
        
        let mut page = pager.borrow_mut_page(&pid)?;
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
        let page = self.pager.borrow_page(pid)?;
        let node_page = BPTreeNodePageData::try_ref_from_bytes(&page)?;
        Ok(node_page.kind)
    }

    fn search_head_leaf(&self) -> PagerResult<Option<PageId>> {
        let mut current: Option<PageId> = self.as_ref().header.root.into();

        while let Some(pid) = current.as_ref() {
            match self.node_kind(&pid)? {
                BPTreeNodeKind::Interior => {
                    let interior: BPTreeInterior<_> = self.pager.borrow_page(pid)?.into();
                    current = interior.iter().flat_map(|cell| cell.as_ref().left.as_ref().clone()).next();
                },
                BPTreeNodeKind::Leaf => {
                    return Ok(Some(*pid));
                },
            }
        }    

        Ok(None)
    }

    /// Recherche une feuille contenant potentiellement la clé
    fn search_leaf(&self, key: &Numeric) -> PagerResult<Option<PageId>> {
        let mut current: Option<PageId> = self.as_ref().header.root.into();
        
        // Le type de la clé passée en argument doit être celle supportée par l'arbre.
        assert_eq!(key.kind(), &self.as_ref().header.key_spec, "wrong key type");

        while let Some(pid) = current.as_ref() {
            if self.node_kind(pid)? == BPTreeNodeKind::Leaf {
                return Ok(Some(*pid));
            } else {
                let interior: BPTreeInterior<_> = self.pager.borrow_page(pid)?.into();
                current = Some(interior.search_child(key))
            }
        }

        Ok(None)
    }

    
    fn borrow_leaf(&self, pid: &PageId) -> PagerResult<BPTreeLeaf<RefPage<'pager>>> {
        self.pager.borrow_page(pid).map(BPTreeLeaf::from)
    }
}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager,
    Page: AsMutPageSlice
{
    /// Insère une nouvelle clé/valeur
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
        

        let mut leaf: BPTreeLeaf<_> = self.pager.borrow_mut_page(&pid)?.into();

        if leaf.is_full() {
            self.split(leaf.as_mut())?;
        }

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

}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager,
    Page: AsMutPageSlice
{
    
    /// Divise un noeud
    /// 
    /// Cette opération est utilisée lors d'une insertion si le noeud est plein.
    fn split(&mut self, page: &mut MutPage<'_>) -> PagerResult<()> {
        let node: BPTreeNode<_> = page.into();

        match node.kind() {
            BPTreeNodeKind::Interior => {
                let mut left: BPTreeInterior<_> = node.into_inner().into();
                
                // on ne divise pas un noeud intérieur qui n'est pas plein.
                if !left.is_full() {
                    return Ok(())
                }

                let mut right = self.insert_interior().and_then(|pid| self.borrow_mut_interior(&pid))?;
                let key = left.split_into(&mut right)?;

                let parent_id = match left.header.parent.as_ref() {
                    Some(parent_id) => {*parent_id},
                    None => {
                        let pid = self.insert_interior()?;
                        self.as_mut().header.root = Some(pid).into();
                        left.header.parent = Some(pid).into();
                        pid
                    },
                };

                right.header.parent = left.header.parent;
                let mut parent = self.borrow_mut_interior(&parent_id)?;              
                self.insert_in_interior(&mut parent, *left.as_page().id(), key, *right.as_page().id())?;      

                Ok(())
            },

            BPTreeNodeKind::Leaf => {
                let mut left: BPTreeLeaf<_> = node.into_inner().into();
                
                // On ne divise pas un noeud qui n'est pas plein.
                if !left.is_full() {
                    return Ok(())
                }
                
                let mut right =  self.insert_leaf().and_then(|pid| self.borrow_mut_leaf(&pid))?;
  
                let key = left.split_into(&mut right)?;

                right.header.prev = Some(*left.as_page().id()).into();
                left.header.next = Some(*right.as_page().id()).into();

                let parent_id = match left.header.parent.as_ref() {
                    Some(parent_id) => {
                        *parent_id
                    },
                    None => {
                        let pid = self.insert_interior()?;
                        self.as_mut().header.root = Some(pid).into();
                        left.header.parent = Some(pid).into();
                        pid
                    },
                };

                right.header.parent = left.header.parent;


                let mut parent = self.borrow_mut_interior(&parent_id)?;              
                self.insert_in_interior(&mut parent, *left.as_page().id(), key, *right.as_page().id())?;
 
                Ok(())
            },
        }
    }

    /// Insère un triplet {gauche | clé | droit} dans le noeud intérieur.
    /// 
    /// Split si le noeud est complet.
    fn insert_in_interior(&mut self, interior: &mut BPTreeInterior<MutPage<'pager>>, left: PageId, key: Numeric, right: PageId) -> PagerResult<()> {
        if interior.is_full() {
            self.split(interior.as_mut())?;
        }

        interior.insert(left, key, right)?;

        interior.header.parent.as_ref().iter().try_for_each(|parent_id| {
            let mut page = self.pager.borrow_mut_page(parent_id)?;
            self.split(&mut page)
        })?;
        
        Ok(())
    }

    fn borrow_mut_interior(&self, pid: &PageId) -> PagerResult<BPTreeInterior<MutPage<'pager>>> {
        self.pager.borrow_mut_page(pid).map(BPTreeInterior::from)
    }

    fn borrow_mut_leaf(&self, pid: &PageId) -> PagerResult<BPTreeLeaf<MutPage<'pager>>> {
        self.pager.borrow_mut_page(pid).map(BPTreeLeaf::from)
    }

    /// Insère une nouvelle feuille dans l'arbre.
    fn insert_leaf(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let page = self.pager.borrow_mut_page(&pid)?;

        BPTreeLeaf::new(page,
            self.as_ref().header.k.try_into().unwrap(),
            self.as_ref().header.interior_cell_size.into(), 
        );

        Ok(pid)
    }

    /// Insère un noeud intérieur dans l'arbre.
    fn insert_interior(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let page = self.pager.borrow_mut_page(&pid)?;

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
pub(super) enum BPTreeNodeKind {
    #[allow(dead_code)]
    Interior = PageKind::BPlusTreeInterior as u8,
    #[allow(dead_code)]
    Leaf = PageKind::BPlusTreeLeaf as u8,
}

pub struct BPTreeNode<Page>(Page) where Page: AsRefPageSlice;

impl<Page> BPTreeNode<Page> where Page: AsRefPageSlice {
    pub fn into_inner(self) -> Page {
        self.0
    }

    pub(super) fn kind(&self) -> &BPTreeNodeKind {
        &self.as_ref().kind
    }
}

impl<Page> From<Page> for BPTreeNode<Page> where Page: AsRefPageSlice {
    fn from(value: Page) -> Self {
        Self(value)
    }
}

impl<Page> AsRef<BPTreeNodePageData> for BPTreeNode<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &BPTreeNodePageData {
        BPTreeNodePageData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BPTreeNodePageData {
    kind: BPTreeNodeKind,
    body: [u8]
}

