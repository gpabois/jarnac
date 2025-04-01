use super::super::kind::KnackKind;
use super::super::Knack;

pub trait AsKernelRef { 
    type Kernel: ?std::marker::Sized;

    fn as_kernel_ref(&self) -> &Self::Kernel;
}

pub trait AsKernelMut: AsKernelRef {
    fn as_kernel_mut(&mut self) -> &mut Self::Kernel;
}

macro_rules! kernel {
    ($kernel:ident) => {
        impl AsKernelRef for $kernel {
            type Kernel = Self;

            fn as_kernel_ref(&self) -> &Self {
                self
            }
        }

        impl AsKernelMut for $kernel {
            fn as_kernel_mut(&mut self) -> &mut Self {
                self
            }
        }
    }
}

kernel!(Knack);
kernel!(KnackKind);