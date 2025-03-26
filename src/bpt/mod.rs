use std::ops::Deref;

use descriptor::BPTreeDescriptor;
use interior::{BPlusTreeInterior, BPlusTreeInteriorMut, BPlusTreeInteriorRef};
use leaf::{BPlusTreeLeaf, BPlusTreeLeafMut, BPlusTreeLeafRef};

use crate::{arena::IPageArena, error::{Error, ErrorKind}, pager::{cell::CellCapacity, page::PageSize, var::VarMeta}, result::Result, tag::JarTag, utils::{MaybeSized, Sized, Valid}, value::{GetValueKind, MaybeSizedValueKind, SizedValueKind, ValueKind}};

pub mod descriptor;
pub mod leaf;
pub mod interior;

pub struct BPlusTree<'nodes, Arena> where Arena: IPageArena<'nodes> {
    arena: &'nodes Arena,
    tag: JarTag
}

impl<'nodes, Arena> BPlusTree<'nodes, Arena> where Arena: IPageArena<'nodes> {
    pub fn new(arena: &'nodes Arena, args: BPlusTreeArgs) -> Result<Self> {
        let node_size: PageSize = arena.size_of().try_into().unwrap();
        let valid_definition = args.define(node_size).validate()?;

        let page = arena.new()?;
        let tag =  *page.tag();
        BPTreeDescriptor::new(page, valid_definition)?;
        Ok(Self{arena, tag})
    }

    pub fn new_leaf(&self) -> Result<BPlusTreeLeafMut<'nodes>> {
        self.arena
            .new()
            .and_then(|page| BPlusTreeLeaf::new(page, self.as_descriptor().as_description()))
    }

    pub fn new_interior(&self) -> Result<BPlusTreeInteriorMut<'nodes>> {
        self.arena
            .new()
            .and_then(|page| BPlusTreeInterior::new(page, self.as_descriptor().as_description()))
    }

    pub fn borrow_leaf(&self, tag: &JarTag) -> Result<BPlusTreeLeafRef<'nodes>> {
        self.arena.borrow_node(tag).and_then(TryFrom::try_from)
    }

    pub fn borrow_mut_leaf(&mut self, tag: &JarTag) -> Result<BPlusTreeLeafMut<'nodes>> {
        self.arena.borrow_mut_node(tag).and_then(TryFrom::try_from)
    }

    pub fn borrow_interior(&self, tag: &JarTag) -> Result<BPlusTreeInteriorRef<'nodes>> {
        self.arena.borrow_node(tag).and_then(TryFrom::try_from)
    }

    pub fn borrow_mut_interior(&mut self, tag: &JarTag) -> Result<BPlusTreeInteriorMut<'nodes>> {
        self.arena.borrow_mut_node(tag).and_then(TryFrom::try_from)
    }
}


impl<'nodes, Arena> BPlusTree<'nodes, Arena> where Arena: IPageArena<'nodes> {
    fn as_descriptor(&self) -> BPTreeDescriptor<Arena::Ref> {
        self.arena
            .borrow_node(&self.tag)
            .and_then(|page| BPTreeDescriptor::try_from(page)).unwrap()
    }

    fn as_mut_descriptor(&self) -> BPTreeDescriptor<Arena::RefMut> {
        self.arena
            .borrow_mut_node(&self.tag)
            .and_then(|page| BPTreeDescriptor::try_from(page)).unwrap()
    }
}


/// Les arguments à passer pour instancier un nouvel arbre B
pub struct BPlusTreeArgs {
    k: Option<CellCapacity>,
    key: Sized<ValueKind>,
    value: MaybeSized<ValueKind>,
}

impl BPlusTreeArgs {
    pub fn new<K, V>(k: Option<CellCapacity>) -> Self where 
        K: GetValueKind<Kind=SizedValueKind>,
        V: GetValueKind, 
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

                (will_spill.then_some(BPlusTreeDefinition::VAL_WILL_SPILL).unwrap_or_default(), value_size)
            },
            MaybeSized::Var(_) => {                
                (BPlusTreeDefinition::VAL_WILL_SPILL | BPlusTreeDefinition::VAL_IS_VAR_SIZED, 0)
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
    key: ValueKind,
    key_size: u16,
    value: ValueKind,
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
