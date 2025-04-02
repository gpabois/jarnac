use super::{kernel::{AsKernelMut, AsKernelRef}, Comparable};

pub struct FixedSized<T>(pub(crate) T) where T: ?std::marker::Sized;

pub trait AsFixedSized: AsKernelRef  {
    fn as_fixed_sized(&self) -> &FixedSized<Self::Kernel>;
}

impl<T> AsFixedSized for FixedSized<T> where T: AsKernelRef {
    fn as_fixed_sized(&self) -> &FixedSized<Self::Kernel> {
        unsafe {
            std::mem::transmute(self.as_kernel_ref())
        }
    }
}

impl<T> AsFixedSized for Comparable<T> where T: AsFixedSized + ?std::marker::Sized {
    fn as_fixed_sized(&self) -> &FixedSized<Self::Kernel> {
        unsafe {
            std::mem::transmute(self.as_kernel_ref())
        }
    }
}

impl<L> AsKernelRef for FixedSized<L> where L: AsKernelRef + ?std::marker::Sized {
    type Kernel = L::Kernel;

    fn as_kernel_ref(&self) -> &Self::Kernel {
        self.0.as_kernel_ref()
    }
}

impl<L> AsKernelMut for FixedSized<L> where L: AsKernelMut + ?std::marker::Sized {
    fn as_kernel_mut(&mut self) -> &mut Self::Kernel {
        self.0.as_kernel_mut()
    }
}


pub struct VarSized<T>(pub(crate) T) where T: ?std::marker::Sized;

impl<T> VarSized<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

pub trait AsVarSized: AsKernelRef {
    fn as_var_sized(&self) -> &VarSized<Self::Kernel>;
}

impl<T> AsVarSized for Comparable<T> where T: AsVarSized
{
    fn as_var_sized(&self) -> &VarSized<Self::Kernel> {
        unsafe {
            std::mem::transmute(self.as_kernel_ref())
        }
    }
}


impl<L> AsKernelRef for VarSized<L> where L: AsKernelRef + ?std::marker::Sized {
    type Kernel = L::Kernel;

    fn as_kernel_ref(&self) -> &Self::Kernel {
        self.0.as_kernel_ref()
    }
}

impl<L> AsKernelMut for VarSized<L> where L: AsKernelMut + ?std::marker::Sized{
    fn as_kernel_mut(&mut self) -> &mut Self::Kernel {
        self.0.as_kernel_mut()
    }
}

pub enum Sized<'a, K> {
    Fixed(&'a FixedSized<K>),
    Var(&'a VarSized<K>)
}
