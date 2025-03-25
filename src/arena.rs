use crate::{pager::page::{MutPage, RefPage}, result::Result, tag::JarTag};

pub trait IArena {
    type Ref;
    type RefMut;

    fn new(&self) -> Result<Self::RefMut>;
    fn borrow_node(&self, tag: &JarTag) -> Result<Self::Ref>;
    fn borrow_mut_node(&self, tag: &JarTag) -> Result<Self::RefMut>;
    
    fn size_of(&self) -> usize;
}

pub trait IPageArena<'pager>: IArena<Ref = RefPage<'pager>, RefMut = MutPage<'pager>> {}