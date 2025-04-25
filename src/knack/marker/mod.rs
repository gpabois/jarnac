pub mod comparable;
pub mod kernel;
pub mod sized;
pub mod array;

pub use comparable::{AsComparable, Comparable};
pub use sized::{AsFixedSized, FixedSized};
pub use array::{Array, Element};

pub type ComparableAndFixedSized<T> = Comparable<FixedSized<T>>;

#[cfg(test)]
mod tests {
    use crate::prelude::IntoKnackBuf;

    use super::super::prelude::GetKnackKind;

    use super::{AsComparable, AsFixedSized};

    #[test]
    fn test_constraint_target() {
        let kind = u8::kind();

        let szd = kind.as_fixed_sized();
        let cmp = kind.as_comparable();

        let knack = 8u8.into_knack_buf();
    }
}
