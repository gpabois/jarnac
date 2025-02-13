//! Arbre B+
//! 
//! Le système permet d'indexer une valeur de taille variable ou fixe avec une clé signée/non-signée d'une taille d'au plus 64 bits (cf [crate::value::numeric]).
use std::{borrow::Cow, cmp::Ordering, marker::PhantomData, ops::{Deref, DerefMut}};

use zerocopy::{FromBytes, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, KnownLayout, TryFromBytes};

use crate::{
    pager::{
        cell::{Cell, CellHeader, CellId, CellPage, CellPageHeader}, 
        page::{MutPage, OptionalPageId, PageData, PageId, PageKind, PageSize, RefPageSlice, TryIntoRefFromBytes}, 
        spill::{read_dynamic_sized_data, DynamicSizedDataHeader}, 
        IPager, PagerResult
    },
    value::numeric::{IntoNumericSpec, Numeric, NumericSpec},
};

pub type BPTreeKey = Numeric;

/// Calcule les paramètres des arbres B+
fn compute_b_plus_tree_parameters(
    page_size: PageSize,
    key_spec: NumericSpec,
    data_size: Option<u16>,
) -> BPlusTreeHeader {
    let key_size = key_spec.size();
    let u32_key_size = u16::from(key_size);

    let u32_btree_interior_page_header_size = u16::try_from(size_of::<BPTreeInteriorPageHeader>()).unwrap();
    let u32_btree_leaf_page_header_size = u16::try_from(size_of::<BPTreeLeafPageHeader>()).unwrap();
    let u32_page_id_size = u16::try_from(size_of::<PageId>()).unwrap();
    let u32_dsd = u16::try_from(size_of::<DynamicSizedDataHeader>()).unwrap();

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
pub struct BPlusTree<'pager, Pager: IPager, Page: 'pager> {
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
        let key_spec = Key::into_numeric_spec();
        let header = compute_b_plus_tree_parameters(pager.page_size(), key_spec, data_size);
        
        let pid = pager.new_page()?;
        let mut page = pager.get_mut_page(&pid)?;
        page.fill(0);
        page[0] = PageKind::BPlusTree as u8;

        let bp_page = BPlusTreePage::try_mut_from_bytes(&mut page).unwrap();
        bp_page.header = header;

        Ok(Self { pager, page })
    }

}

impl<'pager, Pager, Page> AsRef<BPlusTreePage> for BPlusTree<'pager, Pager, Page> 
where
    Pager: IPager,
    Page: Deref<Target=[u8]> + 'pager {
    
    fn as_ref(&self) -> &BPlusTreePage {
        BPlusTreePage::try_ref_from_bytes(&self.page).unwrap()
    }
}

impl<'pager, Pager, Page> AsMut<BPlusTreePage> for BPlusTree<'pager, Pager, Page> 
where
    Pager: IPager,
    Page: DerefMut<Target=[u8]> + 'pager {
    
    fn as_mut(&mut self) -> &mut BPlusTreePage {
        BPlusTreePage::try_mut_from_bytes(&mut self.page).unwrap()
    }
}


impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager,
    Page: Deref<Target=[u8]> + 'pager
{

    pub fn node_kind(&self, pid: &PageId) -> PagerResult<BPTreeNodeKind> {
        let page = self.pager.get_page(pid)?;
        let node_page = BPTreeNodePage::try_ref_from_bytes(&page)?;
        Ok(node_page.kind)
    }

    pub fn search(&self, key: &BPTreeKey) -> PagerResult<Option<RefPageSlice<'pager>>> {
        let maybe_pid = self.search_leaf(key)?;

        Ok(match maybe_pid {
            Some(pid) => {
                let page = self.pager.get_page(&pid)?;
                let leaf  = BPTreeLeafPage::try_ref_from_bytes(&page).unwrap();
                
                BPTreeLeafPage::iter(page, &self.as_ref().header.key_spec)
                .filter(|cell| cell == key);

                None
            },
            None => None
        })
    }

    /// Recherche une feuille contenant potentiellement la clé
    fn search_leaf(&self, key: &BPTreeKey) -> PagerResult<Option<PageId>> {
        let bp_page = self.as_ref();

        let mut current: Option<PageId> = bp_page.header.root.into();
        
        // Le type de la clé passée en argument doit être celle supportée par l'arbre.
        assert_eq!(key.into_numeric_spec(), bp_page.header.key_spec, "invalid key type");

        while let Some(pid) = current.as_ref() {
            if self.node_kind(pid)? == BPTreeNodeKind::Leaf {
                return Ok(Some(*pid));
            } else {
                let page = self.pager.get_page(pid)?;
                current = Some(BPTreeInteriorPage::search_child(&page, &key))
            }
        }

        Ok(None)
    }

}

impl<'pager, Pager, Page> BPlusTree<'pager, Pager, Page>
where
    Pager: IPager,
    Page: DerefMut<Target=[u8]> + 'pager
{
    pub fn insert(&mut self, key: Numeric, value: &[u8]) -> PagerResult<()> {
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


        Ok(())
    }

    /// Divise un noeud
    /// 
    /// Cette opération est utilisée lors d'une insertion si le noeud est plein.
    fn split(&mut self, pid: &PageId) -> PagerResult<()> {
        match self.node_kind(pid)? {
            BPTreeNodeKind::Interior => {
                let mut page = self.pager.get_mut_page(&pid)?;
                let interior = BPTreeInteriorPage::try_mut_from_bytes(&mut page)?;
                
                // on ne divise pas un noeud qui n'est pas plein.
                if !interior.is_full() {
                    return Ok(())
                }

                Ok(())
            },
            BPTreeNodeKind::Leaf => {
                let mut page = self.pager.get_mut_page(&pid)?;
                let leaf = BPTreeInteriorPage::try_mut_from_bytes(&mut page)?;
                
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

        let mut page = self.pager.get_mut_page(&pid)?;
        page.fill(0);
        page[0] = PageKind::BPlusTreeLeaf as u8;
        
        let leaf = BPTreeLeafPage::try_mut_from_bytes(&mut page).unwrap();
        leaf.header.cell_spec = CellPageHeader::new(
            self.as_ref().header.interior_cell_size.into(), 
            self.as_ref().header.k.try_into().unwrap(),
            size_of::<BPTreeLeafPageHeader>().try_into().unwrap()
        );

        Ok(pid)
    }

    /// Insère un noeud intérieur dans l'arbre.
    fn insert_interior(&mut self) -> PagerResult<PageId> {
        let pid = self.pager.new_page()?;
        let mut page = self.pager.get_mut_page(&pid)?;
        page[0] = PageKind::BPlusTreeInterior as u8;

        let interior = BPTreeInteriorPage::try_mut_from_bytes(&mut page).unwrap();
        
        interior.header.cell_spec = CellPageHeader::new(
            self.as_ref().header.leaf_cell_size.into(),
            self.as_ref().header.k.try_into().unwrap(),
            size_of::<BPTreeInteriorPageHeader>().try_into().unwrap()
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
    key_spec: NumericSpec,
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

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// L'entête d'un noeud d'un arbre B+
pub struct BPTreeInteriorPageHeader {
    kind: BPTreeNodeKind,
    cell_spec: CellPageHeader
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Noeud intérieur
pub struct BPTreeInteriorPage {
    header: BPTreeInteriorPageHeader,
    /// Pointeur le plus à droite
    tail: OptionalPageId,
    /// Contient les cellules du noeud. (cf [crate::pager::cell])
    cells: [u8],
}

impl BPTreeInteriorPage {
    pub fn is_full(&self) -> bool {
        self.header.cell_spec.is_full()
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Cellule d'un noeud intérieur.
pub struct BPTreeInteriorCell {
    cell: CellHeader,
    left: OptionalPageId,
    parent: OptionalPageId,
    key: [u8]
}

impl BPTreeInteriorCell {
    /// Récupère la valeur de la clé à partir d'une tranche binaire.
    pub fn from_key_byte_slice(&self, spec: &NumericSpec) -> Numeric {
        spec.from_byte_slice(&self.key)
    }
}

impl BPTreeInteriorPage {
    /// Recherche le noeud enfant à partir de la clé passée en référence.
    pub fn search_child<'page, Page>(page: Page, key: &Numeric) -> PageId 
    where Page: PageData<'page> + Clone
    {
        let spec = &key.into_numeric_spec();
        let interior = BPTreeInteriorPage::try_ref_from_bytes(page).unwrap();

        let maybe_child: Option<PageId>  = CellPage::iter(page)
        .filter(|cell| {
            let interior: &BPTreeInteriorCell = cell.try_into_ref_from_bytes();
            interior.from_key_byte_slice(spec).partial_cmp(key).map(Ordering::is_le).unwrap_or_default()
        })
        .last()
        .map(|cell| {
            let interior: &BPTreeInteriorCell = cell.try_into_ref_from_bytes();
            interior.left
        })
        .unwrap_or_else(|| interior.tail)
        .into();

        maybe_child.expect("should have a child to perform the search")
    }
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// En-tête d'une [feuille](self::BPTreeLeafPage).
pub struct BPTreeLeafPageHeader {
    kind: BPTreeNodeKind,
    cell_spec: CellPageHeader,
    parent: OptionalPageId,
    prev: OptionalPageId,
    next: OptionalPageId,
}

#[derive(TryFromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Page d'une feuille d'un arbre B+
pub struct BPTreeLeafPage {
    header: BPTreeLeafPageHeader,
    cells: [u8],
}

impl BPTreeLeafPage {
    pub fn is_full(&self) -> bool {
        self.header.cell_spec.is_full()
    }

    pub fn iter<'a, Page>(page: Page, key_spec: &'a NumericSpec) -> impl Iterator<Item=BPlusTreeCell<'a, &'a BPlusTreeValue>> 
    where Page: PageData<'a> + Clone
    {
        CellPage::iter(page)
            .map(|cell| {
                BPlusTreeCell::ref_from_bytes(
                    key_spec, 
                    cell.cid, 
                    cell.cell_bytes
                )
            })
    }

}

/// Cellule d'une feuille contenant une paire clé/valeur.
pub struct BPlusTreeCell<'a, ValuePart: 'a> {
    _pht: PhantomData<&'a()>,
    cid: CellId,
    key: Numeric,
    value_part: ValuePart
}

impl<ValuePart> PartialEq<Numeric> for BPlusTreeCell<'_, ValuePart> {
    fn eq(&self, other: &Numeric) -> bool {
        self.key.eq(other)
    }
}

impl<'a> BPlusTreeCell<'a, &'a BPlusTreeValue> {
    
    /// Retourne une cellule clé/valeur
    pub fn ref_from_bytes<CellData>(key_spec: &NumericSpec, cid: CellId, cell: CellData) -> Self 
    where CellData: Deref<Target = [u8]>
    {
        let key_bytes = key_spec.get_byte_slice(&cell);
        let key = key_spec.from_byte_slice(key_bytes);
        let value_part_bytes = &cell[usize::from(key_spec.size())..];

        let value_part = BPlusTreeValue::ref_from_bytes(value_part_bytes).unwrap();

        Self { key, value_part, cid, _pht: PhantomData }
    }

    /// Récupère une référence sur les données stockées dans la cellule.
    pub fn get_value<Pager: IPager>(&self, pager: &Pager) -> PagerResult<Cow<'_, [u8]>> {
        self.value_part.get(pager)
    }
}


#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente la partie valeur d'une cellule d'une feuille.
pub struct BPlusTreeValue {
    data_header: DynamicSizedDataHeader,
    data: [u8],
}

impl BPlusTreeValue {

    /// Récupère une référence sur les données stockées dans la cellule.
    pub fn get<Pager: IPager>(&self, pager: &Pager) -> PagerResult<Cow<'_, [u8]>> {
        // Les données ont débordées ailleurs
        // on va devoir créer un tampon pour stocker tout ça.
        if self.data_header.has_spilled() {
            let mut buf = Vec::<u8>::default();
            read_dynamic_sized_data(&self.data_header, &mut buf, self.get_in_page_data(), pager)?;
            return Ok(Cow::Owned(buf));
        }

        Ok(Cow::Borrowed(self.get_in_page_data()))
    }

    /// Retourne une référence sur la portion stockée dans la cellule.
    fn get_in_page_data(&self) -> &[u8] {
        &self.data[..self.data_header.in_page_size.try_into().unwrap()]
    }
}
