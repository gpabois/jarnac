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

    use super::super::prelude::GetKnackKind;

    use super::{AsComparable, AsFixedSized};

    #[test]
    fn test_constraint_target() {
        let kind = u8::kind();

        kind.as_fixed_sized();
        kind.as_comparable();
    }
}
