pub mod sized;
pub mod kernel;
pub mod comparable;

pub use sized::{AsSized, Sized};
pub use comparable::{AsComparable, Comparable};

#[cfg(test)]
mod tests {
    use super::super::prelude::GetKnackKind;

    use super::{AsComparable, AsSized};

    #[test]
    fn test_constraint_target() {
        let kind = u8::kind();

        let szd = kind.as_sized();
        let cmp = kind.as_comparable();
    }
}
