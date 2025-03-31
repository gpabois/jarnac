use super::{kernel::{AsKernelMut, AsKernelRef}, AsSized};

pub struct Comparable<T>(pub(crate) T);

pub trait AsComparable<Kernel> {
    fn as_comparable(&self) -> &Comparable<Kernel>;
}

impl<Kernel, T> AsComparable<Kernel> for Comparable<T> where T: AsKernelRef<Kernel> {
    fn as_comparable(&self) -> &Comparable<Kernel> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl<Kernel, L> AsKernelRef<Kernel> for Comparable<L> where L: AsKernelRef<Kernel> {
    fn as_ref(&self) -> &Kernel {
        self.0.as_ref()
    }
}

impl<Kernel, L> AsKernelMut<Kernel> for Comparable<L> where L: AsKernelMut<Kernel> {
    fn as_mut(&mut self) -> &mut Kernel {
        self.0.as_mut()
    }
}

impl<Kernel, L> AsSized<Kernel> for Comparable<L> where L: AsSized<Kernel> {
    fn as_sized(&self) -> &super::Sized<Kernel> {
        self.0.as_sized()
    }
}

pub struct MaybeComparable<T>(pub(crate) T);