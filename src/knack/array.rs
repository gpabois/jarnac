use std::ops::Deref;

use crate::utils::Shift;

use super::{
    buf::{IntoKnackBuf, KnackBuf},
    kind::{GetKnackKind, KnackKind, ANY_TYPE_ID, ARRAY_FLAG},
    marker::sized::Sized,
    FromKnack, Knack, KnackBuilder,
};

pub struct ArrayRef([u8]);

impl ArrayRef {
    pub fn get<'array>(&'array self, index: &usize) -> Option<&'array Knack> {
        if let Sized::Fixed(desc) = self.element_kind().as_sized() {
            let offset = index * desc.outer_size();
            let base = 1usize;

            let range = (0..desc.outer_size()).shift(base + offset);
            Some(<&Knack>::from(&self.0[range]))
        } else {
            todo!("implement variable array")
        }
    }

    fn element_kind(&self) -> KnackKind {
        self.kind().element_kind()
    }

    fn kind(&self) -> &KnackKind {
        <&KnackKind>::from(&self.0[0])
    }
}

pub struct Array(Vec<KnackBuilder>);

impl IntoKnackBuf for Array {
    fn into_knack_buf(self) -> KnackBuf {
        todo!()
    }
}

impl FromKnack for Array {
    type Output = ArrayRef;

    fn try_ref_from_knack(value: &super::Knack) -> super::result::KnackResult<&Self::Output> {
        todo!()
    }

    fn try_mut_from_knack(
        value: &mut super::Knack,
    ) -> super::result::KnackResult<&mut Self::Output> {
        todo!()
    }
}

impl GetKnackKind for Array {
    type Kind = KnackKind;

    fn kind() -> Self::Kind {
        KnackKind::new(ANY_TYPE_ID | ARRAY_FLAG)
    }
}

impl Deref for Array {
    type Target = [KnackBuilder];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
