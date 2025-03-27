use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::arena::IArena;
use crate::page::{RefPage, MutPage, PageSize, OptionalPageId};
use crate::tag::JarTag;

pub trait IPager<'pager>: IArena<Ref = RefPage<'pager>, RefMut = MutPage<'pager>> {
    fn tag(&self) -> JarTag;
    fn len(&self) -> u64;
}

pub(crate) trait IPagerInternals<'pager>: IPager<'pager> {
    fn get_free_head(&self) -> Option<JarTag>;
    fn set_free_head(&self, tag: Option<JarTag>);
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct PagerDescriptor {
    /// Nombre magique
    magic_number: u16,
    /// Taille d'une page
    pub page_size: PageSize,
    /// Nombre de pages stockées dans le pager
    pub page_count: u64,
    /// Début de la liste chaînée des pages libres
    pub free_head: OptionalPageId,
    ///
    pub reserved: [u8; 100]
}

impl PagerDescriptor {
    pub fn new(page_size: PageSize) -> Self {
        Self {
            magic_number: 0xD00D,
            page_size,
            page_count: 0,
            free_head: None.into(),
            reserved: [0; 100]
        }
    }
}

pub mod stub {
    use std::{cell::{RefCell, UnsafeCell}, collections::HashMap, marker::PhantomData, pin::Pin, ptr::NonNull};

    use crate::{arena::IArena, page::{descriptor::{PageDescriptor, PageDescriptorInner}, MutPage, PageSlice, RefPage}, result::Result, tag::JarTag, utils::Flip};

    use super::{IPager, PagerDescriptor};

    pub fn new_stub_pager<'buf, const PAGE_SIZE: usize>() -> StubPager<'buf, PAGE_SIZE> {
        StubPager::new()
    }


    /// Paginateur bouchonné.
    pub struct StubPager<'buf, const PAGE_SIZE: usize> {
        descriptor: RefCell<PagerDescriptor>,
        pages: RefCell<Vec<Pin<Box<UnsafeCell<[u8; PAGE_SIZE]>>>>>,
        descriptors: RefCell<HashMap<JarTag, Pin<Box<UnsafeCell<PageDescriptorInner>>>>>,
        _pht: PhantomData<&'buf ()>
    }

    impl<'buf, const PAGE_SIZE: usize> StubPager<'buf, PAGE_SIZE> {
        pub fn new() -> Self {
            Self {
                descriptor: RefCell::new(PagerDescriptor::new(u16::try_from(PAGE_SIZE).unwrap())),
                pages: Default::default(),
                descriptors: Default::default(),
                _pht: PhantomData
            }
        }
    }

    impl<'buf, const PAGE_SIZE: usize> StubPager<'buf, PAGE_SIZE> {
        fn get_page_descriptor(&self, tag: &JarTag) -> Option<PageDescriptor<'buf>> {
            self.descriptors.borrow().get(tag).map(|desc| unsafe {
                let ptr = NonNull::new(desc.get()).unwrap();
                PageDescriptor::new(ptr)
            })
        }
    }

    impl<'buf, const PAGE_SIZE: usize> IPager<'buf> for StubPager<'buf, PAGE_SIZE> {
        fn tag(&self) -> JarTag {
            JarTag::in_jar(0)
        }
        
        fn len(&self) -> u64 {
            self.descriptor.borrow().page_count
        }
    }

    impl<'buf, const PAGE_SIZE: usize> IArena for StubPager<'buf, PAGE_SIZE> {
        type Ref = RefPage<'buf>;
        type RefMut = MutPage<'buf>;

        fn new_element(&self) -> Result<Self::RefMut> {
            unsafe {
                let buf_id = self.pages.borrow().len();
                let content_box = Box::pin(UnsafeCell::new([0; PAGE_SIZE]));
                let content_ptr = content_box.get().cast::<u8>();
                self.pages.borrow_mut().push(content_box);
    
                let content: NonNull<PageSlice> = std::mem::transmute(
                        NonNull::slice_from_raw_parts(NonNull::new(content_ptr).unwrap(), PAGE_SIZE)
                    ) 
                ;
    
                let tag = JarTag::in_jar(0).in_page(u64::try_from(buf_id).unwrap());
    
                let desc = PageDescriptorInner::new(
                    buf_id, 
                    tag,
                    content
                );
    
                let desc_box = Box::pin(UnsafeCell::new(desc));
                desc_box.get().as_mut().unwrap().set_new();
    
                let desc_ptr = NonNull::new(desc_box.get()).unwrap();
                self.descriptors.borrow_mut().insert(tag, desc_box);
                self.descriptor.borrow_mut().page_count += 1;

                MutPage::try_new(PageDescriptor::new(desc_ptr))
            }

        }

        fn try_borrow_element(&self, tag: &JarTag) -> Result<Option<Self::Ref>> {
            self.get_page_descriptor(tag).map(RefPage::try_new).flip()
        }
        fn try_borrow_mut_element(&self, tag: &JarTag) -> Result<Option<Self::RefMut>> {
            self.get_page_descriptor(tag).map(MutPage::try_new).flip()
        }
        
        fn size_of(&self) -> usize {
            PAGE_SIZE
        }
        
        fn delete_element(&self, tag: &JarTag) -> Result<()> {
            let desc = self.get_page_descriptor(tag).unwrap();
            let can_be_deleted = desc.get_ref_counter() == 0;

            if can_be_deleted {
                let buf_id = desc.buf_id();
                self.descriptors.borrow_mut().remove(tag);
                self.pages.borrow_mut().remove(buf_id);
            }

            Ok(())
        }
        
  
    }
}