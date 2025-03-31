use super::kind::KnackKind;

#[derive(Debug)]
pub struct KnackError {
    kind: KnackErrorKind
}

impl KnackError {
    pub fn kind(&self) -> &KnackErrorKind {
        &self.kind
    }
}

impl KnackError {
    pub fn new(kind: KnackErrorKind) -> Self {
        Self {kind}
    }
}

#[derive(Debug)]
pub enum KnackErrorKind {
    WrongKind {
        got: KnackKind,
        expected: KnackKind
    }
}