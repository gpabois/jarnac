use std::{marker::PhantomData, ptr::NonNull};

pub struct JarDescriptor<'buf> {
    ptr: NonNull<JarDescriptorInner>,
    _pht: PhantomData<&'buf ()>,
}
pub struct JarDescriptorInner {}
