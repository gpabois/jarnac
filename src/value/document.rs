use std::{collections::HashMap, io::Write, ops::{Deref, Index}};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::pager::{error::PagerError, PagerResult};

use super::{FromValue, GetValueKind, IntoValueBuf, IntoValueBuilder, path::IntoValuePath, Value, ValueBuf, ValueBuilder, ValueKind, DOCUMENT_KIND, KV_PAIR_KIND};

pub struct KeyValue([u8]);

impl GetValueKind for KeyValue {
    const KIND: ValueKind = KV_PAIR_KIND;
}
impl Deref for KeyValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl KeyValue {
    /// Lit une paire clé/valeur depuis la base de la tranche.
    pub fn read_from_slice(slice: &[u8]) -> &Self {
        let kind = ValueKind::from(slice[0]);
        KeyValue::KIND.assert_eq(&kind).expect("not a kv pair");

        let key_len = usize::try_from(Self::read_key_len(slice)).unwrap();
        let val_len = usize::try_from(Self::read_value_len(slice)).unwrap();
        let kv_slice = &slice[..1usize + 2 * 4usize + key_len + val_len];

        unsafe {
            std::mem::transmute(kv_slice)
        }
    }

    fn read_key_len(slice: &[u8]) -> u32 {
        (&slice[1..5]).read_u32::<LittleEndian>().unwrap()
    }

    fn read_value_len(slice: &[u8]) -> u32 {
        (&slice[5..9]).read_u32::<LittleEndian>().unwrap()
    }

    pub fn key(&self) -> &Value {
        Value::from_ref(self.key_slice())
    }

    pub fn value(&self) -> &Value {
        Value::from_ref(self.value_slice())
    }

    fn key_len(&self) -> u32 {
        Self::read_key_len(&self.0)
    }

    fn value_len(&self) -> u32 {
        Self::read_value_len(&self.0)
    }

    fn kv_space(&self) -> &[u8] {
        let base = 1usize + 4usize + 4usize;
        return &self.0[base..]
    }

    fn key_slice(&self) -> &[u8] {
        &self.kv_space()[..]
    }

    fn value_slice(&self) -> &[u8] {
        let key_len = usize::try_from(self.key_len()).unwrap();
        let value_len = usize::try_from(self.value_len()).unwrap();
        &self.kv_space()[key_len..(value_len + key_len)]
    }
}

pub struct DocumentRef([u8]);
impl DocumentRef {
    const KV_BASE: usize = 1;

    pub fn iter(&self) -> DocAttributesIter<'_> {
        DocAttributesIter { doc: self, base: Self::KV_BASE }
    }

    pub fn get<K: IntoValueBuf>(&self, key: K) -> &Value {
        let key = key.into_value_buf();
        self.iter().find(|kv| kv.key() == key.deref()).map(|kv| kv.value()).unwrap()
    }

    pub fn to_owned(&self) -> Document {
        self
            .iter()
            .map(|kv| (kv.key().cast::<str>().to_owned(), ValueBuilder::from(kv.value())))
            .collect()
    }
}
impl Deref for DocumentRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl TryFrom<&Value> for &DocumentRef {
    type Error = PagerError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        DOCUMENT_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Value> for &mut DocumentRef {
    type Error = PagerError;

    fn try_from(value: &mut Value) -> Result<Self, Self::Error> {
        DOCUMENT_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}

pub struct DocAttributesIter<'a> {
    doc: &'a DocumentRef,
    base: usize
}
impl<'a> Iterator for DocAttributesIter<'a> {
    type Item = &'a KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.base >= self.doc.len() {
            return None
        }

        let kv = KeyValue::read_from_slice(&self.doc[self.base..]);
        self.base += kv.len();

        Some(kv)
    }
}

#[derive(Default)]
pub struct Document(HashMap<String, ValueBuilder>);

impl GetValueKind for Document {
    const KIND: ValueKind = DOCUMENT_KIND;
}

impl FromValue for Document {
    type Output = DocumentRef;

    fn try_ref_from_value(value: &Value) -> PagerResult<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_value(value: &mut Value) -> PagerResult<&mut Self::Output> {
        value.try_into()
    }
}

impl FromIterator<(String, ValueBuilder)> for Document {
    fn from_iter<T: IntoIterator<Item = (String, ValueBuilder)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<Q> Index<Q> for Document where Q: IntoValuePath {
    type Output = ValueBuilder;

    fn index(&self, index: Q) -> &Self::Output {
        self.try_get(index).unwrap()
    }
}

impl IntoValueBuilder for Document {
    fn into_value_builder(self) -> ValueBuilder {
        ValueBuilder::Document(self)
    }
}

impl Document {
    pub fn insert<V: IntoValueBuilder>(&mut self, key: &str, value: V) {
        let key = key.into_value_buf();
        let value = value.into_value_builder();
        self.0.insert(key.to_string(), value);
    }

    pub fn try_get<P: IntoValuePath>(&self, k: P) -> Option<&ValueBuilder> {
        let mut pth = k.into_value_path();

        match pth.pop() {
            None => None,
            Some(attr_name) => self.get_field(&attr_name).and_then(|val| val.get(pth))
        }
    }

    pub fn get_field(&self, k: &str) -> Option<&ValueBuilder> {
        self.0.get(k)
    }
}

impl IntoValueBuf for Document {
    fn into_value_buf(self) -> ValueBuf {
        let mut buf: Vec<u8> = vec![Document::KIND.into()];

        for kv in self.0.into_iter().map(IntoValueBuf::into_value_buf) {
            buf.write_all(&kv).unwrap();
        }

        ValueBuf(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::value::document::Document;

    #[test]
    pub fn test_insert() {
        let mut doc = Document::default();
        
        let mut sub = Document::default();
        sub.insert("bar", "hello world !");
        sub.insert("barbar", 128u8);


        doc.insert("foo", sub);

        assert!(doc["foo"].is::<Document>());
        assert!(doc["foo.bar"].cast::<str>() == "hello world !");
        assert!(doc["foo.barbar"].cast::<u8>() == &128u8);
    }
}