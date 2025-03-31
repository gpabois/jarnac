use super::super::kind::KnackKind;
use super::super::Knack;

pub trait AsKernelRef<Kernel: ?std::marker::Sized> {
    fn as_ref(&self) -> &Kernel;

    fn as_kernel_ref(&self) -> &Kernel {
        self.as_ref()
    }
}

pub trait AsKernelMut<Kernel: ?std::marker::Sized> {
    fn as_mut(&mut self) -> &mut Kernel;

    fn as_kernel_mut(&mut self) -> &mut Kernel {
        self.as_mut()
    }
}

macro_rules! kernel {
    ($kernel:ident) => {
        impl AsKernelRef<Self> for $kernel {
            fn as_ref(&self) -> &Self {
                self
            }
        }

        impl AsKernelMut<Self> for $kernel {
            fn as_mut(&mut self) -> &mut Self {
                self
            }
        }
    }
}

kernel!(Knack);
kernel!(KnackKind);