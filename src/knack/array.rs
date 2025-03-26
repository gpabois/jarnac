use std::ops::Deref;

use super::{IntoKnackBuf, KnackBuilder};

pub struct Array(Vec<KnackBuilder>);

impl IntoKnackBuf for Array {
    fn into_value_buf(self) -> super::KnackBuf {
        todo!()
    }
}

impl Deref for Array {
    type Target = [KnackBuilder];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
