use std::collections::VecDeque;


pub trait IntoKnackPath {
    fn into_value_path(self) -> KnackPath;
}

impl<V> IntoKnackPath for V where KnackPath: From<V> {
    fn into_value_path(self) -> KnackPath {
        self.into()
    }
}

pub struct KnackPath(VecDeque<String>);

impl KnackPath {
    pub fn pop(&mut self) -> Option<String> {
        self.0.pop_front()
    }
}

impl From<&str> for KnackPath {
    fn from(value: &str) -> Self {
        Self(value.split(".").into_iter().map(|seg| seg.to_owned()).collect())
    }
}

