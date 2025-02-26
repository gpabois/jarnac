pub mod numeric;

use std::{collections::HashMap, fmt::Display};

use serde::{de::Visitor, Deserialize};

pub enum Value {
    Array(Vec<Value>),
    Document(Document),
    Primary(Primary),
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVisitor)
    }
}

pub struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Bool(v)))
    }

    fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::UnsignedInt8(v)))
    }

    fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::UnsignedInt16(v)))
    }

    fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::UnsignedInt32(v)))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::UnsignedInt64(v)))
    }

    fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Int8(v)))
    }

    fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Int16(v)))
    }

    fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Int32(v)))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Int64(v)))
    }

    fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Float(v)))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::Double(v)))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::String(v.to_owned())))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Primary(Primary::String(v)))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut arr = Vec::<Value>::new();

        while let Some(v) = seq.next_element()? {
            arr.push(v);
        }

        Ok(Value::Array(arr))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a value")
    }
}

#[derive(PartialEq, PartialOrd)]
pub enum Primary {
    UnsignedInt8(u8),
    UnsignedInt16(u16),
    UnsignedInt32(u32),
    UnsignedInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Bool(bool),
    String(String),
}

impl Display for Primary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Primary::UnsignedInt8(v) => write!(f, "{v}"),
            Primary::UnsignedInt16(v) => write!(f, "{v}"),
            Primary::UnsignedInt32(v) => write!(f, "{v}"),
            Primary::UnsignedInt64(v) => write!(f, "{v}"),
            Primary::Int8(_) => todo!(),
            Primary::Int16(_) => todo!(),
            Primary::Int32(_) => todo!(),
            Primary::Int64(_) => todo!(),
            Primary::Float(_) => todo!(),
            Primary::Double(_) => todo!(),
            Primary::Bool(_) => todo!(),
            Primary::String(_) => todo!(),
        }
    }
}

#[allow(dead_code)]
pub struct Document(HashMap<Primary, Value>);
