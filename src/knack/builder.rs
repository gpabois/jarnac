use std::ops::Deref;

use super::{array::Array, document::Document, path::IntoKnackPath, FromKnackBuilder, GetKnackKind, IntoKnackBuf, Knack, KnackBuf, KnackKind};

/// Type utilisé pour construire des valeurs stockables en base.
pub enum KnackBuilder {   
    Document(Document),
    Array(Array),
    Str(String),
    Other(KnackBuf)
}

impl KnackBuilder {
    /// Permet de rechercher une valeur de manière récursive.
    /// 
    /// # Exemple
    /// let val = {
    ///     "foo": [{"bar": "hello world !"}]
    /// }
    /// val.get("foo.0.bar") retourne "hello world !"
    pub fn get<P: IntoKnackPath>(&self, path: P) -> Option<&KnackBuilder> {
        let mut pth = path.into_value_path();

        match (pth.pop(), self) {
            (Some(attr_name), KnackBuilder::Document(doc)) => doc.get_field(&attr_name).and_then(|attr| attr.get(pth)),
            (Some(str_index), KnackBuilder::Array(array)) => {
                let index = str_index.parse::<usize>().ok()?;
                array.get(index).and_then(|element| element.get(pth))
            }
            (None, val) => Some(val),
            _ => None
        }
    }

    pub fn cast<T: GetKnackKind + FromKnackBuilder + ?Sized>(&self) -> &T::Output {
        self.kind().assert_eq(&T::KIND).expect("wrong types");
        T::borrow_value(self)
    }

    pub fn cast_mut<T: GetKnackKind + FromKnackBuilder + ?Sized>(&mut self) -> &mut T::Output {
        self.kind().assert_eq(&T::KIND).expect("wrong types");
        T::borrow_mut_value(self)
    }

    pub fn is<T: GetKnackKind + ?Sized>(&self) -> bool {
        self.kind().deref() == T::KIND.deref()
    }

    pub fn kind(&self) -> &KnackKind {
        match self {
            KnackBuilder::Document(_) => &Document::KIND,
            KnackBuilder::Array(_) => todo!(),
            KnackBuilder::Str(_) => &str::KIND,
            KnackBuilder::Other(value_buf) => value_buf.kind(),
        }
    }
}

impl From<Document> for KnackBuilder {
    fn from(value: Document) -> Self {
        Self::Document(value)
    }
}

impl From<&Knack> for KnackBuilder {
    fn from(value: &Knack) -> Self {
        if value.is::<Document>() {
            return Self::Document(value.cast::<Document>().to_owned())
        }
        if value.is::<str>() {
            return Self::Str(value.cast::<str>().to_owned())
        }
        Self::Other(value.to_owned())
    }
}

impl IntoKnackBuf for KnackBuilder {
    fn into_value_buf(self) -> KnackBuf {
        match self {
            KnackBuilder::Document(document) => document.into_value_buf(),
            KnackBuilder::Array(array) => array.into_value_buf(),
            KnackBuilder::Str(string) => string.into_value_buf(),
            KnackBuilder::Other(value) => value,
        }
    }
}
