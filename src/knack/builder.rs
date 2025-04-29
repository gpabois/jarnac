use super::array::Array;
use super::marker::kernel::{AsKernelRef, IntoKernel};

use super::{
    buf::{IntoKnackBuf, KnackBuf},
    document::DocBuilder,
    path::IntoKnackPath,
    GetKnackKind, Knack, KnackKind, Str, F32, F64, I128, I16, I32, I64, I8, U128, U16, U32, U64,
};

pub trait IntoKnackBuilder {
    fn into_value_builder(self) -> KnackBuilder;
}

pub trait FromKnackBuilder {
    type Output: ?std::marker::Sized;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output;
    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output;
}

/// Type utilisé pour construire des valeurs stockables en base.
pub enum KnackBuilder {
    Document(DocBuilder),
    Array(Array),
    Str(String),
    Other(KnackBuf),
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
            (Some(attr_name), KnackBuilder::Document(doc)) => {
                doc.get_field(&attr_name).and_then(|attr| attr.get(pth))
            }
            (Some(str_index), KnackBuilder::Array(array)) => {
                let index = str_index.parse::<usize>().ok()?;
                array.get(index).and_then(|element| element.get(pth))
            }
            (None, val) => Some(val),
            _ => None,
        }
    }

    pub fn cast<T: GetKnackKind + FromKnackBuilder + ?Sized>(&self) -> &T::Output {
        self.kind().assert_same(T::kind()).expect("wrong types");
        T::borrow_value(self)
    }

    pub fn cast_mut<T: GetKnackKind + FromKnackBuilder + ?Sized>(&mut self) -> &mut T::Output {
        self.kind().assert_same(T::kind()).expect("wrong types");
        T::borrow_mut_value(self)
    }

    pub fn is<T: GetKnackKind + ?Sized>(&self) -> bool {
        self.kind() == T::kind().as_kernel_ref()
    }

    pub fn kind(&self) -> &KnackKind {
        match self {
            KnackBuilder::Document(_) => DocBuilder::kind().as_kernel_ref(),
            KnackBuilder::Array(_) => todo!(),
            KnackBuilder::Str(_) => str::kind().as_kernel_ref(),
            KnackBuilder::Other(value_buf) => value_buf.kind(),
        }
    }
}

impl From<DocBuilder> for KnackBuilder {
    fn from(value: DocBuilder) -> Self {
        Self::Document(value)
    }
}

impl From<&Knack> for KnackBuilder {
    fn from(value: &Knack) -> Self {
        if value.is::<DocBuilder>() {
            return Self::Document(value.cast::<DocBuilder>().to_owned());
        }
        if value.is::<str>() {
            return Self::Str(value.cast::<str>().to_owned());
        }
        Self::Other(value.to_owned())
    }
}

impl IntoKnackBuf for KnackBuilder {
    type Buf = KnackBuf;
    
    fn into_knack_buf(self) -> KnackBuf {
        match self {
            KnackBuilder::Document(document) => document.into_knack_buf(),
            KnackBuilder::Array(array) => array.into_knack_buf(),
            KnackBuilder::Str(string) => string.into_knack_buf(),
            KnackBuilder::Other(value) => value,
        }
    }
}

impl FromKnackBuilder for u8 {
    type Output = Self;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for u8 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for u16 {
    type Output = U16;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for u16 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for u32 {
    type Output = U32;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for u32 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for u64 {
    type Output = U64;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for u64 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for u128 {
    type Output = U128;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for u128 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for i8 {
    type Output = I8;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for i8 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for i16 {
    type Output = I16;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for i16 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl IntoKnackBuilder for i32 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}
impl FromKnackBuilder for i32 {
    type Output = I32;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}

impl FromKnackBuilder for i64 {
    type Output = I64;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for i64 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl IntoKnackBuilder for i128 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}
impl FromKnackBuilder for i128 {
    type Output = I128;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}

impl FromKnackBuilder for f32 {
    type Output = F32;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}
impl IntoKnackBuilder for f32 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for f64 {
    type Output = F64;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}

impl IntoKnackBuilder for f64 {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf().into_kernel())
    }
}

impl FromKnackBuilder for str {
    type Output = Str;

    fn borrow_value(value: &KnackBuilder) -> &Self::Output {
        if let KnackBuilder::Other(val) = value {
            return val.cast::<Self>();
        }
        panic!("not an unsigned byte")
    }

    fn borrow_mut_value(value: &mut KnackBuilder) -> &mut Self::Output {
        if let KnackBuilder::Other(buf) = value {
            return buf.cast_mut::<Self>();
        }
        panic!("not an unsigned byte")
    }
}

impl IntoKnackBuilder for &str {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Other(self.into_knack_buf())
    }
}

impl IntoKnackBuilder for DocBuilder {
    fn into_value_builder(self) -> KnackBuilder {
        KnackBuilder::Document(self)
    }
}

