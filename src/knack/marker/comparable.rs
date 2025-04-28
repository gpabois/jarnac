
use super::{kernel::{AsKernelMut, AsKernelRef, IntoKernel}, sized::VarSized, FixedSized};

pub struct Comparable<T>(pub(crate) T) where T: ?std::marker::Sized;

pub trait AsComparable: AsKernelRef {
    fn as_comparable(&self) -> &Comparable<Self::Kernel>;
}

impl<T> AsComparable for Comparable<T> where T: AsKernelRef + ?std::marker::Sized {
    fn as_comparable(&self) -> &Comparable<Self::Kernel> {
        unsafe {
            std::mem::transmute(self.as_kernel_ref())
        }
    }
}

impl<T> AsComparable for FixedSized<T> where T: AsComparable {
    fn as_comparable(&self) -> &super::Comparable<Self::Kernel> {
        unsafe {
            std::mem::transmute(self.as_kernel_ref())
        }
    }
}

impl<T> AsComparable for VarSized<T> where T: AsComparable {
    fn as_comparable(&self) -> &super::Comparable<Self::Kernel> {
        unsafe {
            std::mem::transmute(self.as_kernel_ref())
        }
    }
}

impl<L> IntoKernel for Comparable<L> where L: IntoKernel {
    type Kernel = L::Kernel;

    fn into_kernel(self) -> Self::Kernel {
        self.0.into_kernel()
    }
}

impl<L> AsKernelRef for Comparable<L> where L: AsKernelRef + ?std::marker::Sized {
    type Kernel = L::Kernel;

    fn as_kernel_ref(&self) -> &Self::Kernel {
        self.0.as_kernel_ref()
    }
}

impl<L> AsKernelMut for Comparable<L> where L: AsKernelMut + ?std::marker::Sized {
    fn as_kernel_mut(&mut self) -> &mut Self::Kernel {
        self.0.as_kernel_mut()
    }
}