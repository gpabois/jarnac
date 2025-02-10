
use std::{borrow::Cow, ops::DerefMut, u64};

use zerocopy::{try_transmute_ref, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, KnownLayout, TryFromBytes};

use crate::{pager::{page::{OptionalPageId, PageId, PageKind, PageSize}, spill::{read_dynamic_sized_data, DynamicSizedDataHeader}, IPager, PagerResult}, value::numeric::NumericSpec};

/// Calcule les paramètres des arbres B+
fn compute_b_plus_tree_parameters(page_size: PageSize, key_spec: NumericSpec, data_size: Option<u64>) -> BPlusTreeHeader {
    let key_size = key_spec.size();
    let u_key_size: usize = usize::try_from(key_size).unwrap();

    // deux choses à calculer :
    // 1. la taille d'une cellule stockant les clés pour les noeuds intermédiaires ;
    // 2. la taille d'une cellule stockant les clés/valeurs pour les feuilles ;
     
    // taille maximale de l'espace des cellules des noeuds intérieurs
    let node_cell_size_space = page_size - size_of::<BTreeNodeHeader>();

    // On détermine K le nombre de clés possibles dans un noeud.
    // K1: Nombre de clés en calculant sur la base de la taille réservée aux cellules des noeuds intérieurs.
    // K2: Nombre de clés en calculant sur la base de la taille réservée aux cellules des feuilles.
    let k1 = usize::div_ceil(node_cell_size_space - size_of::<PageId>(), u_key_size + size_of::<PageId>());
    // On démarre hypothèse K = K1.
    let mut k = k1;

    let node_leaf_size_space = page_size - (size_of::<BTreeNodeHeader>() + size_of::<BTreeLeafHeader>());
    
    // Deux grands cas : on a passé une taille de données, donc on peut voir lequel des deux K est le plus utile.
    // Pas de taille donnée, donc on prend K = K1, et on calcule la taille maximale admissible des données
    // Une taille donnée, et là :
    // - Si K2 < 2, ça ne vaut pas le coups, on recalcule en prenant K1
    // - Si K2 >= 2, alors on doit vérifier qu'on ne dépasse pas la taille allouée pour les cellules des noeuds intérieurs.
    if let Some(data_size) = data_size {
        let u_data_size = usize::try_from(data_size).unwrap();
        let k2 = usize::div_ceil(node_leaf_size_space, u_key_size + u_data_size);
        // On a un K >= 2, on peut tenter le coup, l'avantage est de ne pas avoir de débordement possible.
        // On doit juste vérifier que ça tiendra avec les noeuds intérieurs
        if k2 >= 2 {
            if node_cell_size_space < k2 * u_key_size + (k2 + 1) * size_of::<PageId>() {
                usize::div_ceil(node_leaf_size_space - k * (u_key_size + size_of::<DynamicSizedDataHeader>()), k)
            } else {
                k = k2;
                u_data_size
            }

        } else {
            usize::div_ceil(node_leaf_size_space - k * (u_key_size + size_of::<DynamicSizedDataHeader>()), k)
        }

    } else {
        usize::div_ceil(node_leaf_size_space - k * (u_key_size + size_of::<DynamicSizedDataHeader>()), k)
    };        

    BPlusTreeHeader {
        kind: BPlusTreePageKind::Kind,
        key_spec,
        k: u64::try_from(k).unwrap(),
        root: None.into()
    }
}

/// Arbre B+
pub struct BPlusTree<'pager, Pager: IPager> {
    pager: &'pager Pager,
    pid: PageId
}

impl<'pager, Pager> BPlusTree<'pager, Pager> where Pager: IPager {
    /// Crée un nouvel arbre B+
    /// 
    /// key_size: puissance de 2 (2^key_size), maximum 3
    /// key_signed: la clé est signée +/-
    /// data_size: la taille de la donnée à stocker, None si la taille est dynamique ou indéfinie. 
    pub fn new (pager: &'pager Pager, key_spec: NumericSpec, data_size: Option<u64>) -> PagerResult<Self> {
        let header = compute_b_plus_tree_parameters(pager.page_size(), key_spec, data_size);
        let pid = pager.new_page()?;
        let mut page = pager.get_mut_page(&pid)?;

        page[0] = PageKind::BPlusTree as u8;
        
        let bp_page = BPlusTreePage::try_mut_from_bytes(&mut page).unwrap();
        bp_page.header = header;

        Ok(Self{pager, pid})
    }

    /// Recherche une feuille
    fn search_leaf(&self, key: &[u8]) -> PagerResult<Option<PageId>> {
        let page = self.pager.get_page(&self.pid)?;
        let bp_page = BPlusTreePage::try_ref_from_bytes(&page).unwrap();

        let current = bp_page.header.root;
        let key_ref = bp_page.header.key_spec.into_ref(key);

        while let Some(pid) = current.as_ref() {
            let page = self.pager.get_page(pid)?;
            let node = BPTreeNode::try_ref_from_bytes(&page).unwrap();
            
            if node.header.kind == BTreeAllowedNodeKinds::Leaf {
                return Ok(Some(*pid))
            } else {
                
            }
        }

        Ok(None)
    }

    /// Insère une nouvelle feuille dans l'arbre.
    fn insert_leaf(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let mut page = self.pager.get_mut_page(&pid)?;
        page[0] = PageKind::BPlusTreeLeaf as u8;
        Ok(pid)
    }

    /// Insère un noeud intérieur dans l'arbre.
    fn insert_interior(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let mut page = self.pager.get_mut_page(&pid)?;
        page[0] = PageKind::BPlusTreeInterior as u8;
        Ok(pid)       
    }
}


#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
enum BPlusTreePageKind {
    Kind = PageKind::BPlusTree as u8
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BPlusTreeHeader {
    kind: BPlusTreePageKind,
    /// (10000)(111: taille de la clé)
    /// 8, 16, 32, 64, 128 respectivement définit comme
    /// 0, 1, 2, 3, 4
    /// Le bit de poids le plus fort définit si la clé est
    /// une valeur signée ou non.
    key_spec: NumericSpec,
    /// Nombre maximum de clés dans l'arbre B+
    k: u64,
    /// Pointeur vers la racine
    root: OptionalPageId
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// La page contenant la définition de l'arbre B+.
pub struct BPlusTreePage {
    header: BPlusTreeHeader,
    body: [u8]
}

#[derive(TryFromBytes, KnownLayout, Immutable, PartialEq, Eq)]
#[repr(u8)]
enum BTreeAllowedNodeKinds {
    Interior = PageKind::BPlusTreeInterior as u8,
    Leaf = PageKind::BPlusTreeLeaf as u8
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// L'entête d'un noeud d'un arbre B+
pub struct BTreeNodeHeader {
    kind: BTreeAllowedNodeKinds,
    cell_count: u16,    
}

/// Données permettant d'accéder aux cellules d'un noeud d'un arbre B+
pub struct BPTreeCellSpec {
    /// Taille d'une page
    page_size: PageSize,
    /// Nombre de clés maximum
    k: u64,
    /// Spec de la clé
    key_spec: NumericSpec
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représentation englobant les noeuds intermédiaires et les feuilles
pub struct BPTreeNode {
    header: BTreeNodeHeader,
    cells: [u8]
}

impl BPTreeNode {

}

#[derive(TryFromBytes, KnownLayout, Immutable)]
pub struct BTreeLeafHeader {
    previous: OptionalPageId,
    next: OptionalPageId,
}


#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Page d'une feuille d'un arbre B+
pub struct BTreeLeafPage {
    common: BTreeNodeHeader,
    leaf: BTreeLeafHeader,
    cells: [u8]
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BPlusTreeCell
{
    data_header: DynamicSizedDataHeader,
    data: [u8]
}

impl BPlusTreeCell {
    /// Récupère une référence sur les données stockées dans la cellule.
    pub fn get(&self, pager: &impl IPager) -> PagerResult<Cow<'_, [u8]>> {
        // Les données ont débordées ailleurs
        // on va devoir créer un tampon pour stocker tout ça.
        if self.data_header.has_spilled() {
            let mut buf = Vec::<u8>::default();
            read_dynamic_sized_data(&self.data_header, &mut buf, self.get_in_page_data(), pager)?;
            return Ok(Cow::Owned(buf))
        }

        Ok(Cow::Borrowed(self.get_in_page_data()))
    }

    /// Retourne une référence sur la portion stockée dans la cellule.
    fn get_in_page_data(&self) -> &[u8] {
        &self.data[..self.data_header.in_page_size.try_into().unwrap()]
    }
}