use descriptor::BPTreeDescriptor;
use leaf::BPlusTreeLeaf;

use crate::{arena::IPageArena, pager::{cell::CellCapacity, page::PageSize}, result::Result, tag::JarTag, utils::{Sized, Valid}, value::ValueKind};

pub mod descriptor;
pub mod leaf;
pub mod interior;

pub struct VarSized<T>(T);

pub enum MaybeSized<T> {
    Sized(Sized<T>),
    Var(VarSized<T>)
}

impl MaybeSized<ValueKind> {
    pub fn max(&self, size: usize) -> usize {
        match self {
            MaybeSized::Sized(sized) => sized.outer_size().max(size),
            MaybeSized::Var(_) => size,
        }
    }
}

impl From<ValueKind> for MaybeSized<ValueKind> {
    fn from(value: ValueKind) -> Self {
        match value.outer_size() {
            None => MaybeSized::Var(VarSized(value)),
            Some(_) => MaybeSized::Sized(Sized(value))
        }
    }
}

pub struct BPlusTreeArgs {
    k: CellCapacity,
    key: Sized<ValueKind>,
    value: MaybeSized<ValueKind>,
}

impl BPlusTreeArgs {
    pub fn define(self, page_size: PageSize) -> BPlusTreeDefinition {
        BPlusTreeDefinition {
            k: self.k,
            key: self.key,
            value: self.value,
            page_size
        }
    }
}

pub struct BPlusTreeDefinition {
    k: u8,
    key: Sized<ValueKind>,
    value: MaybeSized<ValueKind>,
    page_size: PageSize
}

impl BPlusTreeDefinition {
    pub fn validate(self) -> Result<Valid<BPlusTreeDefinition>> {
        let leaf_compliance = BPlusTreeLeaf::<()>::within_available_cell_space_size(self.page_size, self.key, self.value, self.k);
        let interior_compliance =
        if BPlusTreeLeaf::<()>::within_available_cell_space_size(self.page_size, self.key, self.k) {

        }
    }


}

impl BPlusTreeDefinition {
    fn assert_ok(&self, page_size: PageSize) {
        
    }
}

pub struct BPlusTree<'nodes, Arena> where Arena: IPageArena<'nodes> {
    arena: &'nodes Arena,
    tag: JarTag
}

impl<'nodes, Arena> BPlusTree<'nodes, Arena> where Arena: IPageArena<'nodes> {
    pub fn new(arena: &'nodes Arena, args: BPlusTreeArgs) -> Result<Self> {
        let node_size: PageSize = arena.size_of().try_into().unwrap();
        let def = args.define(node_size).validate()?;

        let page = arena.new()?;
        BPTreeDescriptor::new(&mut page, args)
    }
}


impl<'nodes, Arena> BPlusTree<'nodes, Arena> where Arena: IPageArena<'nodes>
{
    fn as_desc(&self) -> BPTreeDescriptor<Arena::Ref> {
        self.arena
            .borrow_node(&self.tag)
            .and_then(|page| BPTreeDescriptor::try_from(page)).unwrap()
    }

    fn as_mut_desc(&self) -> BPTreeDescriptor<Arena::RefMut> {
        self.arena
            .borrow_mut_node(&self.tag)
            .and_then(|page| BPTreeDescriptor::try_from(page)).unwrap()
    }
}