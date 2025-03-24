use crate::pager::page::PageId;

#[derive(Clone, Copy, Hash, Debug, PartialEq, Eq)]
pub struct JarId(u64);

impl std::fmt::Display for JarId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Hash, Clone, Copy, PartialEq, Eq, Debug)]
pub struct JarTag {
    pub jar_id: JarId,
    pub page_id: PageId
}

impl std::fmt::Display for JarTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}", self.jar_id, self.page_id)
    }
}