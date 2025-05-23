use std::{
    collections::HashMap,
    io::Write,
    ops::{Deref, Index},
};

use byteorder::{LittleEndian, ReadBytesExt};

use super::{error::KnackError, marker::kernel::AsKernelRef, result::KnackResult};

use super::{
    buf::{IntoKnackBuf, KnackBuf},
    builder::IntoKnackBuilder,
    path::IntoKnackPath,
    FromKnack, GetKnackKind, Knack, KnackBuilder,
};

pub enum DocCow<'a> {
    Owned(DocBuilder),
    Borrow(&'a Document)
}

impl From<DocBuilder> for DocCow<'_> {
    fn from(value: DocBuilder) -> Self {
        DocCow::Owned(value)
    }
}

impl<'a> From<&'a Document> for DocCow<'a> {
    fn from(value: &'a Document) -> Self {
        DocCow::Borrow(value)
    }
}

pub struct KeyValue([u8]);

impl Deref for KeyValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl KeyValue {
    /// Lit une paire clé/valeur depuis la base de la tranche.
    pub fn read_from_slice(_slice: &[u8]) -> &Self {
        todo!();
        /*
        let kind = KnackKind::from(slice[0]);
        KeyValue::KIND.assert_eq(&kind).expect("not a kv pair");

        let key_len = usize::try_from(Self::read_key_len(slice)).unwrap();
        let val_len = usize::try_from(Self::read_value_len(slice)).unwrap();
        let kv_slice = &slice[..1usize + 2 * 4usize + key_len + val_len];

        unsafe {
            std::mem::transmute(kv_slice)
        }
         */
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
        return &self.0[base..];
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

pub struct Document([u8]);

impl Document {
    const KV_BASE: usize = 1;

    pub fn iter(&self) -> DocAttributesIter<'_> {
        DocAttributesIter {
            doc: self,
            base: Self::KV_BASE,
        }
    }

    pub fn get_field(&self, name: &str) -> Option<&Knack> {
        self.iter()
            .filter(|kv| kv.key().cast::<str>() == name)
            .map(|kv| kv.value())
            .next()
    }

    pub fn get<Path: IntoKnackPath>(&self, _key: Path) -> &Knack {
        todo!()
    }

    pub fn to_owned(&self) -> DocBuilder {
        self.iter()
            .map(|kv| {
                (
                    kv.key().cast::<str>().to_owned(),
                    KnackBuilder::from(kv.value()),
                )
            })
            .collect()
    }
}

impl Deref for Document {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl TryFrom<&Knack> for &Document {
    type Error = KnackError;

    fn try_from(value: &Knack) -> std::result::Result<Self, Self::Error> {
        DocBuilder::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}
impl TryFrom<&mut Knack> for &mut Document {
    type Error = KnackError;

    fn try_from(value: &mut Knack) -> std::result::Result<Self, Self::Error> {
        DocBuilder::kind()
            .as_kernel_ref()
            .assert_same(value.kind().as_kernel_ref())?;

        unsafe { Ok(std::mem::transmute(value)) }
    }
}

pub struct DocAttributesIter<'a> {
    doc: &'a Document,
    base: usize,
}
impl<'a> Iterator for DocAttributesIter<'a> {
    type Item = &'a KeyValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.base >= self.doc.len() {
            return None;
        }

        let kv = KeyValue::read_from_slice(&self.doc[self.base..]);
        self.base += kv.len();

        Some(kv)
    }
}

#[derive(Default)]
pub struct DocBuilder(HashMap<String, KnackBuilder>);

impl FromKnack for DocBuilder {
    type Output = Document;

    fn try_ref_from_knack(value: &Knack) -> KnackResult<&Self::Output> {
        value.try_into()
    }

    fn try_mut_from_knack(value: &mut Knack) -> KnackResult<&mut Self::Output> {
        value.try_into()
    }
}

impl IntoKnackBuf for (String, KnackBuilder) {
    type Buf = KnackBuf;

    fn into_knack_buf(self) -> Self::Buf {
        KnackBuf::from(self)
    }
}

impl FromIterator<(String, KnackBuilder)> for DocBuilder {
    fn from_iter<T: IntoIterator<Item = (String, KnackBuilder)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl<Q> Index<Q> for DocBuilder
where
    Q: IntoKnackPath,
{
    type Output = KnackBuilder;

    fn index(&self, index: Q) -> &Self::Output {
        self.try_get(index).unwrap()
    }
}

impl DocBuilder {
    /// Insère une paire clé/valeur dans le document.
    pub fn insert<V: IntoKnackBuilder>(&mut self, key: &str, value: V) {
        let value = value.into_knack_builder();
        self.0.insert(key.to_string(), value);
    }

    pub fn try_get<P: IntoKnackPath>(&self, k: P) -> Option<&KnackBuilder> {
        let mut pth = k.into_value_path();

        match pth.pop() {
            None => None,
            Some(attr_name) => self.get_field(&attr_name).and_then(|val| val.get(pth)),
        }
    }

    pub fn get_field(&self, k: &str) -> Option<&KnackBuilder> {
        self.0.get(k)
    }
}

impl IntoKnackBuf for DocBuilder {
    type Buf = KnackBuf;
    
    fn into_knack_buf(self) -> KnackBuf {
        let mut buf: Vec<u8> = vec![];

        buf.write_all(&DocBuilder::kind().as_kernel_ref().as_bytes())
            .unwrap();

        for kv in self.0.into_iter().map(IntoKnackBuf::into_knack_buf) {
            buf.write_all(kv.as_bytes()).unwrap();
        }

        KnackBuf::from_bytes(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::knack::document::DocBuilder;

    #[test]
    pub fn test_insert() {
        let mut doc = DocBuilder::default();

        let mut sub = DocBuilder::default();
        sub.insert("bar", "hello world !");
        sub.insert("barbar", 128u8);

        doc.insert("foo", sub);

        assert!(doc["foo"].is::<DocBuilder>());
        assert!(doc["foo.bar"].cast::<str>() == "hello world !");
        assert!(doc["foo.barbar"].cast::<u8>() == &128u8);
    }
}

