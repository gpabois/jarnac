use std::{collections::HashMap, io::Write, ops::{Deref, Index}};

use byteorder::{LittleEndian, ReadBytesExt};

use crate::{error::Error, pager::var::Var, result::Result, utils::VarSized};

use super::{FromKnack, GetKnackKind, IntoKnackBuf, IntoKnackBuilder, path::IntoKnackPath, Knack, KnackBuf, KnackBuilder, KnackKind, DOCUMENT_KIND, KV_PAIR_KIND};

pub struct KeyValue([u8]);

impl GetKnackKind for KeyValue {
    const KIND: KnackKind = KV_PAIR_KIND;
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
        let kind = KnackKind::from(slice[0]);
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

    pub fn key(&self) -> &Knack {
        Knack::from_ref(self.key_slice())
    }

    pub fn value(&self) -> &Knack {
        Knack::from_ref(self.value_slice())
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

    pub fn get<K: IntoKnackBuf>(&self, key: K) -> &Knack {
        let key = key.into_value_buf();
        self.iter().find(|kv| kv.key() == key.deref()).map(|kv| kv.value()).unwrap()
    }

    pub fn to_owned(&self) -> Document {
        self
            .iter()
            .map(|kv| (kv.key().cast::<str>().to_owned(), KnackBuilder::from(kv.value())))
            .collect()
    }
}
impl Deref for DocumentRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl TryFrom<&Knack> for &DocumentRef {
    type Error = Error;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        DOCUMENT_KIND.assert_eq(value.kind())?;

        unsafe {
            Ok(std::mem::transmute(value))
        }
    }
}
impl TryFrom<&mut Knack> for &mut DocumentRef {
    type Error = Error;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
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
pub struct Document(HashMap<String, KnackBuilder>);

impl GetKnackKind for Document {
    type Kind = VarSized<KnackKind>;
    const KIND: Self::Kind = VarSized::new(DOCUMENT_KIND);
}

impl FromKnack for Document {
    type Output = DocumentRef;

    fn try_ref_from_knack(value: &Knack) -> Result<&Self::Output> {
        value.try_into()    
    }
    
    fn try_mut_from_knack(value: &mut Knack) -> Result<&mut Self::Output> {
        value.try_into()
    }
}

impl FromIterator<(String, KnackBuilder)> for Document {
    fn from_iter<T: IntoIterator<Item = (String, KnackBuilder)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<Q> Index<Q> for Document where Q: IntoKnackPath {
    type Output = KnackBuilder;

    fn index(&self, index: Q) -> &Self::Output {
        self.try_get(index).unwrap()
    }
}

impl IntoKnackBuilder for Document {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Document(self)
    }
}

impl Document {
    pub fn insert<V: IntoKnackBuilder>(&mut self, key: &str, value: V) {
        let key = key.into_value_buf();
        let value = value.into_value_builder();
        self.0.insert(key.to_string(), value);
    }

    pub fn try_get<P: IntoKnackPath>(&self, k: P) -> Option<&KnackBuilder> {
        let mut pth = k.into_value_path();

        match pth.pop() {
            None => None,
            Some(attr_name) => self.get_field(&attr_name).and_then(|val| val.get(pth))
        }
    }

    pub fn get_field(&self, k: &str) -> Option<&KnackBuilder> {
        self.0.get(k)
    }
}

impl IntoKnackBuf for Document {
    fn into_value_buf(self) -> KnackBuf {
        let mut buf: Vec<u8> = vec![Document::KIND.0.into()];

        for kv in self.0.into_iter().map(IntoKnackBuf::into_value_buf) {
            buf.write_all(&kv).unwrap();
        }

        KnackBuf(buf)
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