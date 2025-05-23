use crate::knack::kind::KnackKind;

use super::{kernel::{AsKernelMut, AsKernelRef, IntoKernel}, Comparable};

pub struct FixedSized<T>(pub(crate) T) where T: ?std::marker::Sized;

impl<L> IntoKernel for FixedSized<L> where L: IntoKernel {
    type Kernel = L::Kernel;

    fn into_kernel(self) -> Self::Kernel {
        self.0.into_kernel()
    }
}

pub trait AsFixedSized: AsKernelRef  {
    fn as_fixed_sized(&self) -> &FixedSized<Self::Kernel>;
}

impl<T> AsFixedSized for FixedSized<T> where T: AsKernelRef + ?std::marker::Sized {
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

impl<L> IntoKernel for VarSized<L> where L: IntoKernel {
    type Kernel = L::Kernel;

    fn into_kernel(self) -> Self::Kernel {
        self.0.into_kernel()
    }
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

pub enum Sized<'a, K> where K: ?std::marker::Sized {
    Fixed(&'a FixedSized<K>),
    Var(&'a VarSized<K>)
}

impl<K> Sized<'_, K> where K: AsKernelRef<Kernel=KnackKind> + ?std::marker::Sized {
    pub fn is_variable(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    pub fn is_fixed(&self) -> bool {
        matches!(self, Self::Fixed(_))
    }

    pub fn inner_size(&self) -> Option<usize> {
        match self {
            Sized::Fixed(fixed_sized) => Some(fixed_sized.as_fixed_sized().inner_size()),
            Sized::Var(_) => None,
        }
    }

    pub fn outer_size(&self) -> Option<usize> {
        match self {
            Sized::Fixed(fixed_sized) => Some(fixed_sized.as_fixed_sized().outer_size()),
            Sized::Var(_) => None,
        }
    }
}