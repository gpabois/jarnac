use super::{array::Array, document::Document, IntoValuePath};

/// Type utilisé pour construire des valeurs stockables en base.
pub enum ValueBuilder {   
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),

    I8(u8),
    I16(u16),
    I32(u32),
    I64(u64),
    I128(u128),

    F32(f32),
    F64(f64),

    Str(String),

    Document(Document),
    Array(Array),
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
        Self::Value(value.to_owned())
    }
}

impl IntoValueBuf for ValueBuilder {
    fn into_value_buf(self) -> ValueBuf {
        match self {
            ValueBuilder::Document(document) => document.into_value_buf(),
            ValueBuilder::Value(value) => value,
            ValueBuilder::Array(array) => array.into_value_buf(),
        }
    }
}
