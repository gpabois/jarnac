use crate::{result::Result, tag::JarTag};

pub trait IArena {
    type Ref;
    type RefMut;

    fn new_element(&self) -> Result<Self::RefMut>;
    fn delete_element(&self, tag: &JarTag) -> Result<()>;
    fn try_borrow_element(&self, tag: &JarTag) -> Result<Option<Self::Ref>>;
    fn try_borrow_mut_element(&self, tag: &JarTag) -> Result<Option<Self::RefMut>>;
    fn borrow_element(&self, tag: &JarTag) -> Result<Self::Ref> {
        Ok(self.try_borrow_element(tag)?.unwrap())
    }
    fn borrow_mut_element(&self, tag: &JarTag) -> Result<Self::RefMut> {
        Ok(self.try_borrow_mut_element(tag)?.unwrap())
    }
    fn size_of(&self) -> usize;
}
