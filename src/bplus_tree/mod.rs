//! Arbre B+
//!
//! Le système permet d'indexer une valeur de taille variable ou fixe avec une clé signée/non-signée d'une taille d'au plus 64 bits (cf [crate::value::numeric]).
//pub mod cursor;
mod interior;
mod leaf;
mod descriptor;


use descriptor::{BPTreeDescriptor, BPlusTreeHeader};
use interior::*;
use leaf::*;

use zerocopy::TryFromBytes;
use zerocopy_derive::{Immutable, KnownLayout, TryFromBytes};

use crate::{
    pager::{
        cell::{CellHeader, CellPageHeader, GlobalCellId}, 
        page::{
            AsMutPageSlice, AsRefPageSlice, MutPage, PageId, PageKind, PageSize, RefPage, RefPageSlice
        }, var::{Var, VarHeader}, IPager, PagerResult
    },
    value::{Value, ValueKind},
};

pub type BPTreeKey = Value;

pub const MIN_K: u16 = 2;
pub const LEAF_HEADER_SIZE: usize = size_of::<BPTreeLeafHeader>() + 1 + size_of::<CellPageHeader>();
pub const INTERIOR_HEADER_SIZE: usize = size_of::<BPTreeInteriorHeader>() + 1 + size_of::<CellPageHeader>();

fn within_available_interior_cell_space_size(key: ValueKind, page_size: PageSize, k: u16) -> bool {
    max_interior_cell_space_size(page_size) >= compute_interior_cell_space_size(key, k)
}

fn within_available_leaf_cell_space_size(key: ValueKind, page_size: PageSize, k: u16, data_size: u16) -> bool {
    max_leaf_cell_space_size(page_size) >= compute_leaf_cell_space_size(k, key, data_size)
}

/// Calcule l'espace maximal alloué aux cellules d'un noeud intérieur
#[inline(always)]
fn max_interior_cell_space_size(page_size: PageSize) -> u16 {
    page_size - u16::try_from(INTERIOR_HEADER_SIZE).unwrap()
}

fn compute_interior_cell_size(key: ValueKind) -> u16 {
    let key_size: u16 = key.full_size().unwrap().try_into().unwrap();
    let page_id_size: u16 = size_of::<PageId>().try_into().unwrap();
    let cell_header: u16 = size_of::<CellHeader>().try_into().unwrap();
    cell_header + key_size + page_id_size
}

#[inline(always)]
fn compute_interior_cell_space_size(key: ValueKind, k: u16) -> u16 {
    k * compute_interior_cell_size(key)
}

#[inline(always)]
fn compute_leaf_cell_size(key: ValueKind, data_size: u16) -> u16 {
    let cell_header: u16 = size_of::<CellHeader>().try_into().unwrap();
    let key_size = u16::try_from(key.full_size().unwrap()).unwrap();
    let value_var_header: u16 = size_of::<VarHeader>().try_into().unwrap();
    cell_header + key_size + data_size + value_var_header
}

#[inline(always)]
fn compute_leaf_cell_space_size(k: u16, key: ValueKind, data_size: u16) -> u16 {
    k * compute_leaf_cell_size(key, data_size)
}

/// Calcule l'espace maximal alloué aux cellules d'une feuille
#[inline(always)]
fn max_leaf_cell_space_size(page_size: PageSize) -> u16 {
    page_size - u16::try_from(LEAF_HEADER_SIZE).unwrap()
}

#[inline(always)]
/// Calcule la taille maximale possible de la valeur stockée dans l'arbre B+ au sein de la page.
fn max_leaf_value_size(key: ValueKind, k: u16, page_size: PageSize) -> u16 {
    let leaf_cell_space_size = max_leaf_cell_space_size(page_size);

    let cell_header: u16 = size_of::<CellHeader>().try_into().unwrap();
    let key_size = u16::try_from(key.full_size().unwrap()).unwrap();

    let leaf_cell_header_size = cell_header + key_size;

    u16::div_ceil(leaf_cell_space_size.saturating_sub(k * leaf_cell_header_size), k)
}

/// Calcule les paramètres des arbres B+
fn compute_b_plus_tree_parameters(
    page_size: PageSize,
    key: ValueKind,
    value: ValueKind,
) -> BPlusTreeHeader {
    let (k, data_size): (u8, u16) = if let Some(data_size) = value.full_size() {
        let k = (1..u8::MAX).into_iter()
        .filter(|&k| within_available_interior_cell_space_size(key, page_size, k.into()))
        .filter(|&k| within_available_leaf_cell_space_size(key, page_size, k.into(), data_size.try_into().unwrap()))
        .last()
        .unwrap_or_default();

        (k, data_size.try_into().unwrap())
    } else {
        // On va essayer de trouver le tuple (k, data_size) 
        // qui maximise k * (data_size + leaf_cell_header) 
        // tout en étant inférieure à leaf_cell_space_size
        (1..u8::MAX)
            .into_iter()
            .filter(|&k | within_available_interior_cell_space_size(key, page_size, k.into()))
            .map(|k| (k, max_leaf_value_size(key, k.into(), page_size)))
            .map(|(k, mut data_size)| {
                let key_size = u16::try_from(key.full_size().unwrap()).unwrap();
                let var_size = u16::try_from(size_of::<VarHeader>()).unwrap();
                let cell_header = u16::try_from(size_of::<CellHeader>()).unwrap();
                let header = key_size + cell_header + var_size;
                data_size = (header + data_size) - header;
                (k, data_size)
            })
            .filter(|&(k, data_size)| within_available_leaf_cell_space_size(key, page_size, k.into(), data_size))
            .max_by_key(|&(_, data_size)| data_size)
            .unwrap_or_default()
    };

    assert!(k > 0, "k is zero");
    assert!(data_size > 0, "data size is zero");

    let cell_header = u16::try_from(size_of::<CellHeader>()).unwrap();

    let interior_cell_size = compute_interior_cell_size(key) - cell_header;
    let leaf_cell_size = compute_leaf_cell_size(key, data_size) - cell_header;

    BPlusTreeHeader {
        interior_cell_size,
        leaf_cell_size,
        key,
        value,
        data_size,
        k,
        root: None.into(),
    }
}

/// Identifiant d'une cellule contenant une paire clé/valeur.
pub type BPlusTreeCellId = GlobalCellId;

/// Trait permettant de manipuler un arbre B+ en lecture.
pub trait IRefBPlusTree<'pager> {
    /// Cherche une cellule clé/valeur à partir de la clé passée en argument.
    fn search(&self, key: &Value) -> PagerResult<Option<Var<RefPageSlice<'pager>>>>;

    /// Cherche la cellule la plus proche dont la valeur est supérieure ou égale.
    fn search_nearest_ceil(&self, key: &Value) -> PagerResult<Option<BPlusTreeCellId>> {
        Ok(self
            .search_nearest_floor(key)?
            .and_then(|gcid| self.next_sibling(&gcid).unwrap()))
    }

    /// Cherche la cellule la plus proche dont la valeur est inférieure ou égale.
    fn search_nearest_floor(&self, key: &Value) -> PagerResult<Option<BPlusTreeCellId>>;

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
pub trait IMutBPlusTree<'pager>: IRefBPlusTree<'pager> {
    /// Insère une nouvelle valeur
    fn insert(&mut self, key: &Value, value: &Value) -> PagerResult<()>;
}

/// Arbre B+
pub struct BPlusTree<'pager, Pager: IPager + ?Sized, Page: 'pager>
where
    Page: AsRefPageSlice,
{
    /// Le pager où sont stockées les noeuds de l'arbre
    pager: &'pager Pager,
    /// Descripteur de l'arbre, contient les métadonnées nécessaires à sa manipulation
    desc: BPTreeDescriptor<Page>,
}

impl<'pager, Pager, Page> IRefBPlusTree<'pager> for BPlusTree<'pager, Pager, Page>
where
    Pager: IPager + ?Sized,
    Page: AsRefPageSlice,
{
    fn search(&self, key: &BPTreeKey) -> PagerResult<Option<Var<RefPageSlice<'pager>>>> {
        let maybe_pid = self.search_leaf(key)?;

        if let Some(pid) = maybe_pid {
            return Ok(self.borrow_leaf(&pid)?.into_value(key))
        }
        
        Ok(None)
    }

    fn search_nearest_floor(&self, key: &Value) -> PagerResult<Option<BPlusTreeCellId>> {
        let maybe_pid = self.search_leaf(key)?;

        Ok(match maybe_pid {
            Some(pid) => {
                let leaf: BPTreeLeaf<_> = self.pager.borrow_page(&pid).and_then(BPTreeLeaf::try_from)?;

                let gid = leaf
                    .iter()
                    .filter(|&cell| cell <= key)
                    .map(|cell| GlobalCellId::new(pid, cell.cid()))
                    .next();

                gid
            }
            None => None,
        })
    }

    fn head(&self) -> PagerResult<Option<BPlusTreeCellId>> {
        Ok(self.search_head_leaf()?.and_then(|pid| {
            let leaf = self.borrow_leaf(&pid).unwrap();
            let head = leaf
                .iter()
                .map(|cell| BPlusTreeCellId::new(pid, cell.cid()))
                .next();
            head
        }))
    }

    fn next_sibling(&self, gcid: &BPlusTreeCellId) -> PagerResult<Option<BPlusTreeCellId>> {
        let leaf = self.borrow_leaf(gcid.pid())?;
        let cell = &leaf[gcid.cid()];

        match cell.as_cell().next_sibling() {
            Some(cid) => Ok(Some(BPlusTreeCellId::new(*leaf.as_page().id(), cid))),
            None => Ok(leaf.get_next().and_then(|next_pid| {
                self.borrow_leaf(&next_pid)
                    .unwrap()
                    .iter()
                    .map(|cell| BPlusTreeCellId::new(next_pid, cell.cid()))
                    .next()
            })),
        }
    }

    fn prev_sibling(&self, gcid: &BPlusTreeCellId) -> PagerResult<Option<BPlusTreeCellId>> {
        let leaf = self.borrow_leaf(gcid.pid())?;
        let cell = &leaf[gcid.cid()];

        match cell.as_cell().prev_sibling() {
            Some(cid) => Ok(Some(BPlusTreeCellId::new(*leaf.as_page().id(), cid))),
            None => Ok(leaf.get_prev().and_then(|prev_pid| {
                self.borrow_leaf(&prev_pid)
                    .unwrap()
                    .iter()
                    .map(|cell| BPlusTreeCellId::new(prev_pid, cell.cid()))
                    .last()
            })),
        }
    }

    fn borrow_cell(&self, _gcid: &BPlusTreeCellId) -> PagerResult<BPTreeLeafCell<RefPage<'_>>> {
        todo!()
    }
}

impl<'pager, Pager> BPlusTree<'pager, Pager, MutPage<'pager>>
where
    Pager: IPager + ?Sized,
{
    /// Crée un nouvel arbre B+
    ///
    /// key_size: puissance de 2 (2^key_size), maximum 3
    /// key_signed: la clé est signée +/-
    /// data_size: la taille de la donnée à stocker, None si la taille est dynamique ou indéfinie.
    pub fn new(
        pager: &'pager Pager,
        key_kind: &ValueKind,
        value_kind: &ValueKind,
    ) -> PagerResult<Self> {
        assert!(key_kind.full_size().is_some(), "the key must be of a sized-type");

        // on génère les données qui vont bien pour gérer notre arbre B+
        let header = compute_b_plus_tree_parameters(pager.page_size(), *key_kind, *value_kind);

        let page = pager.new_page()
            .and_then(|pid| pager.borrow_mut_page(&pid))
            .and_then(|page| BPTreeDescriptor::new(page, header))?;

        Ok(Self { pager, desc: page })
    }
}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager + ?Sized,
    Page: AsRefPageSlice,
{
    pub fn desc(&self) -> &BPTreeDescriptor<Page> {
        &self.desc
    }

    fn node_kind(&self, pid: &PageId) -> PagerResult<BPTreeNodeKind> {
        let page = self.pager.borrow_page(pid)?;
        let node_page = BPTreeNodePageData::try_ref_from_bytes(&page)?;
        Ok(node_page.kind)
    }

    fn search_head_leaf(&self) -> PagerResult<Option<PageId>> {
        let mut current = self.desc.root();

        while let Some(pid) = current.as_ref() {
            match self.node_kind(&pid)? {
                BPTreeNodeKind::Interior => {
                    let interior = self.borrow_interior(pid)?;
                    current = interior
                        .iter()
                        .flat_map(|cell| cell.left().clone())
                        .next();
                }
                BPTreeNodeKind::Leaf => {
                    return Ok(Some(*pid));
                }
            }
        }

        Ok(None)
    }

    /// Recherche une feuille contenant potentiellement la clé
    fn search_leaf(&self, key: &Value) -> PagerResult<Option<PageId>> {
        let mut current: Option<PageId> = self.desc.root();

        // Le type de la clé passée en argument doit être celle supportée par l'arbre.
        assert_eq!(key.kind(), &self.desc.key_kind(), "wrong key type");

        while let Some(pid) = current.as_ref() {
            if self.node_kind(pid)? == BPTreeNodeKind::Leaf {
                return Ok(Some(*pid));
            } else {
                let interior: BPTreeInterior<_> = self.borrow_interior(pid)?;
                current = Some(interior.search_child(key))
            }
        }

        Ok(None)
    }

    fn borrow_leaf(&self, pid: &PageId) -> PagerResult<BPTreeLeaf<RefPage<'pager>>> {
        self.pager.borrow_page(pid).and_then(BPTreeLeaf::try_from)
    }

    fn borrow_interior(&self, pid: &PageId) -> PagerResult<BPTreeInterior<RefPage<'pager>>> {
        self.pager.borrow_page(pid).and_then(BPTreeInterior::try_from)
    }
}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager + ?Sized,
    Page: AsMutPageSlice,
{
    /// Insère une nouvelle clé/valeur
    pub fn insert(&mut self, key: &Value, value: &Value) -> PagerResult<()> {

        assert_eq!(key.kind(), &self.desc.key_kind(), "wrong key kind");
        assert_eq!(value.kind(), &self.desc.value_kind(), "wrong value kind");

        let leaf_pid = match self.search_leaf(&key)? {
            Some(pid) => pid,
            None => {
                let leaf_pid = self.insert_leaf()?;
                self.desc.set_root(Some(leaf_pid));
                leaf_pid
            }
        };

        let mut leaf = self.borrow_mut_leaf(&leaf_pid)?;

        // si la feuille est pleine on va la diviser en deux.
        if leaf.is_full() {
            self.split(leaf.as_mut())?;
        }

        leaf.insert(key, value, self.pager)
    }

}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager + ?Sized,
    Page: AsMutPageSlice,
{
    /// Divise un noeud
    ///
    /// Cette opération est utilisée lors d'une insertion si le noeud est plein.
    fn split(&mut self, page: &mut MutPage<'_>) -> PagerResult<()> {
        let node: BPTreeNode<_> = page.into();

        match node.kind() {
            BPTreeNodeKind::Interior => {
                let mut left= BPTreeInterior::try_from(page)?;

                // on ne divise pas un noeud intérieur qui n'est pas plein.
                if !left.is_full() {
                    return Ok(());
                }

                // on ajoute un nouveau noeud intérieur
                let mut right = self
                    .insert_interior()
                    .and_then(|pid| self.borrow_mut_interior(&pid))?;
                
                // on divise le le noeud en deux au niveau [K/2]
                let key = left.split_into(&mut right)?.to_owned();

                // récupère le parent du noeud à gauche
                // si aucun parent n'existe alors on crée un noeud intérieur.
                let parent_id = match left.parent().clone() {
                    Some(parent_id) => parent_id,
                    None => {
                        let pid = self.insert_interior()?;
                        self.desc.set_root(Some(pid));
                        left.set_parent(Some(pid));
                        pid
                    }
                };

                right.set_parent(left.parent().clone());
                let mut parent = self.borrow_mut_interior(&parent_id)?;
                self.insert_in_interior(
                    &mut parent,
                    *left.id(),
                    &key,
                    *right.id(),
                )?;

                Ok(())
            }

            BPTreeNodeKind::Leaf => {
                let mut left = BPTreeLeaf::try_from(node.into_inner())?;

                // On ne divise pas un noeud qui n'est pas plein.
                if !left.is_full() {
                    return Ok(());
                }

                let mut right = self
                    .insert_leaf()
                    .and_then(|pid| self.borrow_mut_leaf(&pid))?;

                let key = left.split_into(&mut right)?.to_owned();

                right.set_prev(Some(*left.id()));
                left.set_next(Some(*right.id()));

                let parent_id = match left.get_parent() {
                    Some(parent_id) => parent_id,
                    None => {
                        let pid = self.insert_interior()?;
                        self.desc.set_root(Some(pid));
                        left.set_parent(Some(pid));
                        pid
                    }
                };

                right.set_parent(left.get_parent());

                let mut parent = self.borrow_mut_interior(&parent_id)?;
                self.insert_in_interior(
                    &mut parent,
                    *left.as_page().id(),
                    &key,
                    *right.as_page().id(),
                )?;

                Ok(())
            }
        }
    }

    /// Insère un triplet {gauche | clé | droit} dans le noeud intérieur.
    ///
    /// Split si le noeud est complet.
    fn insert_in_interior(
        &mut self,
        interior: &mut BPTreeInterior<MutPage<'pager>>,
        left: PageId,
        key: &Value,
        right: PageId,
    ) -> PagerResult<()> {

        if interior.is_full() {
            self.split(interior.as_mut())?;
        }

        interior.insert(left, key, right)?;

        interior
            .parent()
            .as_ref()
            .iter()
            .try_for_each(|parent_id| {
                let mut page = self.pager.borrow_mut_page(parent_id)?;
                self.split(&mut page)
            })?;

        Ok(())
    }

    fn borrow_mut_interior(&self, pid: &PageId) -> PagerResult<BPTreeInterior<MutPage<'pager>>> {
        self.pager
            .borrow_mut_page(pid)
            .and_then(BPTreeInterior::try_from)
    }

    fn borrow_mut_leaf(&self, pid: &PageId) -> PagerResult<BPTreeLeaf<MutPage<'pager>>> {
        self.pager
            .borrow_mut_page(pid)
            .and_then(BPTreeLeaf::try_from)
    }

    /// Insère une nouvelle feuille dans l'arbre.
    fn insert_leaf(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let page = self.pager.borrow_mut_page(&pid)?;

        BPTreeLeaf::new(
            page,
            self.desc.k(),
            self.desc.leaf_cell_size()
        )?;

        Ok(pid)
    }

    /// Insère un noeud intérieur dans l'arbre.
    fn insert_interior(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let page = self.pager.borrow_mut_page(&pid)?;

        BPTreeInterior::new(
            page,
            self.desc.k(),
            self.desc.interior_cell_size()
        )?;

        Ok(pid)
    }
}


#[derive(TryFromBytes, KnownLayout, Immutable, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
pub(super) enum BPTreeNodeKind {
    #[allow(dead_code)]
    Interior = PageKind::BPlusTreeInterior as u8,
    #[allow(dead_code)]
    Leaf = PageKind::BPlusTreeLeaf as u8,
}

pub struct BPTreeNode<Page>(Page)
where
    Page: AsRefPageSlice;

impl<Page> BPTreeNode<Page>
where
    Page: AsRefPageSlice,
{
    pub fn into_inner(self) -> Page {
        self.0
    }

    pub(super) fn kind(&self) -> &BPTreeNodeKind {
        &self.as_ref().kind
    }
}

impl<Page> From<Page> for BPTreeNode<Page>
where
    Page: AsRefPageSlice,
{
    fn from(value: Page) -> Self {
        Self(value)
    }
}

impl<Page> AsRef<BPTreeNodePageData> for BPTreeNode<Page>
where
    Page: AsRefPageSlice,
{
    fn as_ref(&self) -> &BPTreeNodePageData {
        BPTreeNodePageData::try_ref_from_bytes(self.0.as_ref()).unwrap()
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct BPTreeNodePageData {
    kind: BPTreeNodeKind,
    body: [u8],
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{
        pager::fixtures::fixture_new_pager,
        value::{IntoValueBuf, U64}
    };

    use super::{BPlusTree, IRefBPlusTree};

    #[test]
    pub fn test_insert() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();
        let mut tree = BPlusTree::new(pager.as_ref(), &U64, &U64)?;

        for i in 0..91u64 {
            tree.insert(
                &i.into_value_buf(),
                &1234u64.into_value_buf()
            )?;
        }

        let var = tree.search(&10_u64.into_value_buf())?.unwrap();
        let value = var.try_borrow()?.try_as_u64()?.to_owned();

        assert_eq!(value, 1234u64);

        Ok(())
    }
}
