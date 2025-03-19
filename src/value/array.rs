use std::ops::Deref;

use super::{IntoValueBuf, ValueBuilder};

pub struct Array(Vec<ValueBuilder>);

impl IntoValueBuf for Array {
    fn into_value_buf(self) -> super::ValueBuf {
        todo!()
    }
}

impl Deref for Array {
    type Target = [ValueBuilder];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
