use std::collections::VecDeque;


pub trait IntoValuePath{
    fn into_value_path(self) -> ValuePath;
}

impl<V> IntoValuePath for V where ValuePath: From<V> {
    fn into_value_path(self) -> ValuePath {
        self.into()
    }
}

pub struct ValuePath(VecDeque<String>);

impl ValuePath {
    pub fn pop(&mut self) -> Option<String> {
        self.0.pop_front()
    }
}

impl From<&str> for ValuePath {
    fn from(value: &str) -> Self {
        Self(value.split(".").into_iter().map(|seg| seg.to_owned()).collect())
    }
}

