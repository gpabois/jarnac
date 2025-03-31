use super::{kernel::{AsKernelMut, AsKernelRef}, AsComparable};

pub struct Sized<T>(pub(crate) T);

pub trait AsSized<Kernel> {
    fn as_sized(&self) -> &Sized<Kernel>;
}

impl<Kernel, T> AsSized<Kernel> for Sized<T> where T: AsKernelRef<Kernel> {
    fn as_sized(&self) -> &Sized<Kernel> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl<Kernel, L> AsKernelRef<Kernel> for Sized<L> where L: AsKernelRef<Kernel> {
    fn as_ref(&self) -> &Kernel {
        self.0.as_ref()
    }
}

impl<Kernel, L> AsKernelMut<Kernel> for Sized<L> where L: AsKernelMut<Kernel> {
    fn as_mut(&mut self) -> &mut Kernel {
        self.0.as_mut()
    }
}

impl<Kernel, L> AsComparable<Kernel> for Sized<L> where L: AsComparable<Kernel> {
    fn as_comparable(&self) -> &super::Comparable<Kernel> {
        self.0.as_comparable()
    }
}

pub struct VarSized<T>(pub(crate) T);

impl<T> VarSized<T> {
    pub fn new(value: T) -> Self {
        Self(value)
    }
}

impl<Kernel, L> AsKernelRef<Kernel> for VarSized<L> where L: AsKernelRef<Kernel> {
    fn as_ref(&self) -> &Kernel {
        self.0.as_ref()
    }
}

impl<Kernel, L> AsKernelMut<Kernel> for VarSized<L> where L: AsKernelMut<Kernel> {
    fn as_mut(&mut self) -> &mut Kernel {
        self.0.as_mut()
    }
}