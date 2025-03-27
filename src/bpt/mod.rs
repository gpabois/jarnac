use std::ops::Deref;

use descriptor::BPTreeDescriptor;
use interior::{BPlusTreeInterior, BPlusTreeInteriorMut, BPlusTreeInteriorRef};
use leaf::{BPlusTreeLeaf, BPlusTreeLeafMut, BPlusTreeLeafRef};

use crate::{
    pager::IPager, 
    error::{Error, ErrorKind}, 
    knack::{GetKnackKind, Knack, KnackKind, MaybeSizedValueKind, SizedKnackKind}, 
    cell::CellCapacity, 
    page::{AsRefPageSlice, PageKind, PageSize, RefPageSlice}, 
    var::{MaybeSpilled, VarMeta}, 
    result::Result, 
    tag::JarTag, 
    utils::{MaybeSized, Sized, Valid}};

pub mod descriptor;
pub mod leaf;
pub mod interior;

pub struct BPlusTree<'nodes, Arena> where Arena: IPager<'nodes> {
    arena: &'nodes Arena,
    tag: JarTag
}

impl<'nodes, Arena> BPlusTree<'nodes, Arena> where Arena: IPager<'nodes> {
    pub fn new(arena: &'nodes Arena, args: BPlusTreeArgs) -> Result<Self> {
        let node_size: PageSize = arena.size_of().try_into().unwrap();
        let valid_definition = args.define(node_size).validate()?;

        let page = arena.new_element()?;
        let tag =  *page.tag();
        BPTreeDescriptor::new(page, valid_definition)?;
        Ok(Self{arena, tag})
    }

    pub fn search(&self, key: &Knack) -> Result<Option<MaybeSpilled<RefPageSlice<'nodes>>>> {
        let maybe_tag = self.search_leaf(key)?;

        if let Some(tag) = maybe_tag {
            return self
                    .borrow_leaf(&tag)
                    .map(|leaf|
                        leaf.into_value(
                            key, 
                            &self.as_descriptor().key_kind(), 
                            &self.as_descriptor().value_kind()
                        )
                    )
                
        }
        
        Ok(None)
    }

    
    fn new_leaf(&self) -> Result<BPlusTreeLeafMut<'nodes>> {
        self.arena
            .new_element()
            .and_then(|page| BPlusTreeLeaf::new(page, self.as_descriptor().as_description()))
    }

    fn new_interior(&self) -> Result<BPlusTreeInteriorMut<'nodes>> {
        self.arena
            .new_element()
            .and_then(|page| BPlusTreeInterior::new(page, self.as_descriptor().as_description()))
    }

    fn borrow_leaf(&self, tag: &JarTag) -> Result<BPlusTreeLeafRef<'nodes>> {
        self.arena
            .borrow_element(tag)
            .and_then(TryFrom::try_from)
    }

    fn borrow_mut_leaf(&mut self, tag: &JarTag) -> Result<BPlusTreeLeafMut<'nodes>> {
        self.arena
            .borrow_mut_element(tag)
            .and_then(TryFrom::try_from)
    }

    fn borrow_interior(&self, tag: &JarTag) -> Result<BPlusTreeInteriorRef<'nodes>> {
        self.arena.borrow_element(tag).and_then(TryFrom::try_from)
    }

    fn borrow_mut_interior(&mut self, tag: &JarTag) -> Result<BPlusTreeInteriorMut<'nodes>> {
        self.arena.borrow_mut_element(tag).and_then(TryFrom::try_from)
    }

    /// Recherche une feuille contenant potentiellement la clé
    fn search_leaf(&self, key: &Knack) -> Result<Option<JarTag>> {
        let mut current = self.as_descriptor().root();

        // Le type de la clé passée en argument doit être celle supportée par l'arbre.
        assert_eq!(key.kind(), self.as_descriptor().key_kind().deref(), "wrong key type");

        while let Some(tag) = current.as_ref().map(|&pid| self.tag.in_page(pid)) {
            if self.node_kind(&tag)? == BPTreeNodeKind::Leaf {
                return Ok(Some(tag));
            } else {
                let interior = self.borrow_interior(&tag)?;
                current = Some(interior.search_child(key, &self.as_descriptor().key_kind()))
            }
        }

        Ok(None)
    }

    fn node_kind(&self, tag: &JarTag) -> Result<BPTreeNodeKind> {
        self.arena
            .borrow_element(tag)
            .and_then(|page| TryFrom::try_from(page.as_bytes()[0]))
    }

}


impl<'nodes, Arena> BPlusTree<'nodes, Arena> where Arena: IPager<'nodes> {
    fn as_descriptor(&self) -> BPTreeDescriptor<Arena::Ref> {
        self.arena
            .borrow_element(&self.tag)
            .and_then(|page| BPTreeDescriptor::try_from(page)).unwrap()
    }

    fn as_mut_descriptor(&self) -> BPTreeDescriptor<Arena::RefMut> {
        self.arena
            .borrow_mut_element(&self.tag)
            .and_then(|page| BPTreeDescriptor::try_from(page)).unwrap()
    }
}


#[derive(PartialEq, Eq)]
#[repr(u8)]
pub(super) enum BPTreeNodeKind {
    #[allow(dead_code)]
    Interior = PageKind::BPlusTreeInterior as u8,
    #[allow(dead_code)]
    Leaf = PageKind::BPlusTreeLeaf as u8,
}

impl TryFrom<u8> for BPTreeNodeKind {
    type Error = Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        let interior = PageKind::BPlusTreeInterior as u8;
        let leaf = PageKind::BPlusTreeLeaf as u8;

        if value == interior {
            Ok(BPTreeNodeKind::Interior)
        } else if value == leaf {
            Ok(BPTreeNodeKind::Leaf)
        } else {
            let kind = PageKind::try_from(value).expect("not a valid page kind");
            panic!("not a b+ tree node (kind={kind})")
        }
    }
}

/// Les arguments à passer pour instancier un nouvel arbre B
pub struct BPlusTreeArgs {
    k: Option<CellCapacity>,
    key: Sized<KnackKind>,
    value: MaybeSized<KnackKind>,
}

impl BPlusTreeArgs {
    pub fn new<K, V>(k: Option<CellCapacity>) -> Self where 
        K: GetKnackKind<Kind=SizedKnackKind>,
        V: GetKnackKind, 
        V::Kind: Into<MaybeSizedValueKind> {
        Self {
            k,
            key: K::KIND,
            value: V::KIND.into()
        }
    }

}
impl BPlusTreeArgs {
    /// Prend les exigences et transforme cela en une définition des paramètres de l'arbre B+.
    pub fn define(self, page_size: PageSize) -> BPlusTreeDefinition {
        let k = self.k.unwrap_or_else(|| self.find_best_k(page_size));
        let available_value_size = BPlusTreeLeaf::<()>::compute_available_value_space_size(page_size, self.key, k);

        let (flags, value_size) = match self.value {
            MaybeSized::Sized(sized) => {
                let value_size = u16::try_from(sized.outer_size()).unwrap();
                let will_spill = value_size > available_value_size;

                (will_spill.then_some(BPlusTreeDefinition::VAL_IS_VAR_SIZED).unwrap_or_default(), value_size)
            },
            MaybeSized::Var(_) => {                
                (BPlusTreeDefinition::VAL_IS_VAR_SIZED, 0)
            },
        };

        BPlusTreeDefinition {
            k,
            flags,
            key: *self.key.deref(),
            key_size: u16::try_from(self.key.outer_size()).unwrap(),
            value: self.value.into_inner(),
            value_size,
            page_size
        }
    }

    /// On trouve MAX(K) pour K e [1..255] tel qu'on puisse rentrer dans un noeud intérieur et un noeud feuille.
    pub fn find_best_k(&self, page_size: PageSize) -> CellCapacity {
        (1..CellCapacity::MAX)
            .into_iter()
            .filter(|&k| {
                let available_value_size = BPlusTreeLeaf::<()>::compute_available_value_space_size(page_size, self.key, k);

                let value_size = self.value
                    .outer_size()
                    .map(|size| u16::try_from(size).unwrap())
                    .unwrap_or_else(|| available_value_size)
                    .min(available_value_size);

                BPlusTreeLeaf::<()>::within_available_cell_space_size(page_size, self.key, value_size, k)
                && BPlusTreeInterior::<()>::within_available_cell_space_size(page_size, self.key, k)
            })
            .last()
            .expect("cannot find k")
    }

}

pub struct BPlusTreeDefinition {
    k: u8,
    flags: u8,
    key: KnackKind,
    key_size: u16,
    value: KnackKind,
    value_size: u16,
    page_size: PageSize
}
impl BPlusTreeDefinition {
    pub const VAL_WILL_SPILL: u8 = 0b1;
    pub const VAL_IS_VAR_SIZED: u8 = 0b10;

    pub fn validate(self) -> Result<Valid<BPlusTreeDefinition>> {
        let key_kind = Sized::new(self.key, self.key_size.into());
        let valid = BPlusTreeLeaf::<()>::within_available_cell_space_size(self.page_size, key_kind, self.value_size, self.k)
            && BPlusTreeInterior::<()>::within_available_cell_space_size(self.page_size, key_kind, self.k);

        let valid_value_requirements = if self.flags & BPlusTreeDefinition::VAL_IS_VAR_SIZED > 0 {
            self.value_size >= u16::try_from(size_of::<VarMeta>()).unwrap()
        } else {
            true
        };

        (valid && valid_value_requirements).then(|| Valid(self)).ok_or_else(|| Error::new(ErrorKind::InvalidBPlusTreeDefinition))
    }
}
