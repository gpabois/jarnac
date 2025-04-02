use std::ops::Deref;

use super::{buf::{IntoKnackBuf, KnackBuf}, KnackBuilder};

pub struct Array(Vec<KnackBuilder>);

impl IntoKnackBuf for Array {
    fn into_knack_buf(self) -> KnackBuf {
        todo!()
    }
}

impl Deref for Array {
    type Target = [KnackBuilder];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
