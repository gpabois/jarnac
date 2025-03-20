use super::{array::Array, document::Document, path::IntoValuePath, FromValueBuilder, GetValueKind, IntoValueBuf, Value, ValueBuf, ValueKind};

/// Type utilisé pour construire des valeurs stockables en base.
pub enum ValueBuilder {   
    Document(Document),
    Array(Array),
    Str(String),
    Other(ValueBuf)
}

impl ValueBuilder {
    /// Permet de rechercher une valeur de manière récursive.
    /// 
    /// # Exemple
    /// let val = {
    ///     "foo": [{"bar": "hello world !"}]
    /// }
    /// val.get("foo.0.bar") retourne "hello world !"
    pub fn get<P: IntoValuePath>(&self, path: P) -> Option<&ValueBuilder> {
        let mut pth = path.into_value_path();

        match (pth.pop(), self) {
            (Some(attr_name), ValueBuilder::Document(doc)) => doc.get_field(&attr_name).and_then(|attr| attr.get(pth)),
            (Some(str_index), ValueBuilder::Array(array)) => {
                let index = str_index.parse::<usize>().ok()?;
                array.get(index).and_then(|element| element.get(pth))
            }
            (None, val) => Some(val),
            _ => None
        }
    }

    pub fn cast<T: GetValueKind + FromValueBuilder + ?Sized>(&self) -> &T::Output {
        self.kind().assert_eq(&T::KIND).expect("wrong types");
        T::borrow_value(self)
    }

    pub fn cast_mut<T: GetValueKind + FromValueBuilder + ?Sized>(&mut self) -> &mut T::Output {
        self.kind().assert_eq(&T::KIND).expect("wrong types");
        T::borrow_mut_value(self)
    }

    pub fn is<T: GetValueKind + ?Sized>(&self) -> bool {
        self.kind() == &T::KIND
    }

    pub fn kind(&self) -> &ValueKind {
        match self {
            ValueBuilder::Document(_) => &Document::KIND,
            ValueBuilder::Array(_) => todo!(),
            ValueBuilder::Str(_) => &str::KIND,
            ValueBuilder::Other(value_buf) => value_buf.kind(),
        }
    }
}

impl From<Document> for ValueBuilder {
    fn from(value: Document) -> Self {
        Self::Document(value)
    }
}

impl From<&Value> for ValueBuilder {
    fn from(value: &Value) -> Self {
        if value.is::<Document>() {
            return Self::Document(value.cast::<Document>().to_owned())
        }
        if value.is::<str>() {
            return Self::Str(value.cast::<str>().to_owned())
        }
        Self::Other(value.to_owned())
    }
}

impl IntoValueBuf for ValueBuilder {
    fn into_value_buf(self) -> ValueBuf {
        match self {
            ValueBuilder::Document(document) => document.into_value_buf(),
            ValueBuilder::Array(array) => array.into_value_buf(),
            ValueBuilder::Str(string) => string.into_value_buf(),
            ValueBuilder::Other(value) => value,
        }
    }
}
