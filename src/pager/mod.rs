use std::mem::MaybeUninit;

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::arena::IArena;
use crate::buffer::{BufferPool, IBufferPool};
use crate::free::{pop_free_page, push_free_page};
use crate::page::{AsMutPageSlice, AsRefPageSlice, MutPage, OptionalPageId, PageId, PageSize, RefPage};
use crate::result::Result;
use crate::tag::{JarId, JarTag};

pub trait IPager<'pager>: IArena<Ref = RefPage<'pager>, RefMut = MutPage<'pager>> {
    /// Le tag (page_id: 0, cell_id: 0)
    fn tag(&self) -> JarTag;
    
    /// Nombre de pages stockées 
    fn len(&self) -> u64;
    
    /// Aucune page n'est stockée
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Interface permettant de manipuler un pager
pub struct Pager<'buf> {
    pool: &'buf BufferPool,
    id: JarId
}

impl<'buf> Pager<'buf> {
    /// Créé un nouveau pager
    pub fn new(id: JarId, pool: &'buf BufferPool) -> Result<Self> {
        let pager = Self {id, pool};
        
        let mut desc = pool.alloc(&JarTag::in_jar(id).in_page(0)).map(PagerDescriptor)?;
        desc.new(pool.page_size());

        Ok(pager)
    }

    fn get_descriptor(&self) -> PagerDescriptor<RefPage<'buf>> {
        self.borrow_element(&self.tag().in_page(0))
        .map(PagerDescriptor)
        .unwrap()
    }

    fn get_mut_descriptor(&self) -> PagerDescriptor<MutPage<'buf>> {
        self.borrow_mut_element(&self.tag().in_page(0))
        .map(PagerDescriptor)
        .unwrap()
    }

    fn load_page(&self, _tag: &JarTag) -> Result<()> {
        todo!("implement page loading")
    }
}

impl<'buf> IPager<'buf> for Pager<'buf> {
    fn tag(&self) -> JarTag {
        JarTag::in_jar(self.id)
    }

    fn len(&self) -> u64 {
        self.get_descriptor().as_description().len()
    }
}

impl<'buf> IArena for Pager<'buf> {
    type Ref = RefPage<'buf>;
    type RefMut = MutPage<'buf>;

    fn new_element(&self) -> Result<Self::RefMut> {
        if let Some(tag) = pop_free_page(self, self.get_mut_descriptor().as_mut_description())? {
            self.borrow_mut_element(&tag)
        } else {
            let pid = self.get_descriptor().as_description().len();
            self.get_mut_descriptor().as_mut_description().inc_len();
            let tag = self.tag().in_page(pid);
            self.pool.alloc(&tag)
        }
    }

    fn delete_element(&self, tag: &JarTag) -> Result<()> {
        push_free_page(
            self, 
            self.get_mut_descriptor().as_mut_description(), 
            tag
        )
    }

    fn try_borrow_element(&self, tag: &JarTag) -> Result<Option<Self::Ref>> {
        if !self.pool.contains(tag) {
            self.load_page(tag)?;
        }

        self.pool.try_get_ref(tag)
    }

    fn try_borrow_mut_element(&self, tag: &JarTag) -> crate::result::Result<Option<Self::RefMut>> {
        if !self.pool.contains(tag) {
            self.load_page(tag)?;
        }

        self.pool.try_get_mut(tag)
    }

    fn size_of(&self) -> usize {
        usize::from(self.get_descriptor().as_description().page_size)
    }
}

pub struct PagerDescriptor<Page>(Page) where Page: AsRefPageSlice;

impl<Page> PagerDescriptor<Page> where Page: AsRefPageSlice {
    pub fn as_description(&self) -> &PagerDescription {
        PagerDescription::ref_from_bytes(&self.0.as_bytes()[0..size_of::<PagerDescription>()]).unwrap()
    }
}

impl<Page> PagerDescriptor<Page> where Page: AsMutPageSlice {
    pub fn new(&mut self, page_size: PageSize) {
        self.as_uninit_description().write(PagerDescription::new(page_size));
    }

    pub fn as_mut_description(&mut self) -> &mut PagerDescription {
        PagerDescription::mut_from_bytes(&mut self.0.as_mut_bytes()[0..size_of::<PagerDescription>()]).unwrap()
    }

    pub fn as_uninit_description(&mut self) -> &mut MaybeUninit<PagerDescription> {
        unsafe {
            std::mem::transmute(self.as_mut_description())
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
pub struct PagerDescription {
    /// Taille d'une page
    pub page_size: PageSize,
    /// Nombre de pages stockées dans le pager
    pub page_count: u64,
    /// Début de la liste chaînée des pages libres
    pub free_head: OptionalPageId,
    /// Données réservées
    pub reserved: [u8; 100],
}

impl PagerDescription {
    pub fn new(page_size: PageSize) -> Self {
        Self {
            page_size,
            page_count: 0,
            free_head: None.into(),
            reserved: [0; 100],
        }
    }

    pub fn get_free_head(&self) -> Option<PageId> {
        self.free_head.into()
    }

    pub fn set_free_head(&mut self, head: Option<PageId>) {
        self.free_head = head.into();
    }

    pub fn inc_len(&mut self) {
        self.page_count += 1
    }

    pub fn len(&self) -> u64 {
        self.page_count
    }
}

pub mod stub {
    use std::{
        cell::{RefCell, UnsafeCell},
        collections::HashMap,
        marker::PhantomData,
        pin::Pin,
        ptr::NonNull,
    };

    use crate::{
        arena::IArena,
        page::{
            descriptor::{PageDescriptor, PageDescriptorInner},
            MutPage, PageSlice, RefPage,
        },
        result::Result,
        tag::JarTag,
        utils::Flip,
    };

    use super::{IPager, PagerDescription};

    pub fn new_stub_pager<'buf, const PAGE_SIZE: usize>() -> StubPager<'buf, PAGE_SIZE> {
        StubPager::new()
    }

    /// Paginateur bouchonné.
    pub struct StubPager<'buf, const PAGE_SIZE: usize = 4096> {
        descriptor: RefCell<PagerDescription>,
        pages: RefCell<Vec<Pin<Box<UnsafeCell<[u8; PAGE_SIZE]>>>>>,
        descriptors: RefCell<HashMap<JarTag, Pin<Box<UnsafeCell<PageDescriptorInner>>>>>,
        _pht: PhantomData<&'buf ()>,
    }

    impl<'buf, const PAGE_SIZE: usize> StubPager<'buf, PAGE_SIZE> {
        pub fn new() -> Self {
            Self {
                descriptor: RefCell::new(PagerDescription::new(u16::try_from(PAGE_SIZE).unwrap())),
                pages: Default::default(),
                descriptors: Default::default(),
                _pht: PhantomData,
            }
        }
    }

    impl<'buf, const PAGE_SIZE: usize> StubPager<'buf, PAGE_SIZE> {
        fn get_page_descriptor(&self, tag: &JarTag) -> Option<PageDescriptor<'buf>> {
            self.descriptors.borrow().get(tag).map(|desc| unsafe {
                let ptr = NonNull::new(desc.get()).unwrap();
                PageDescriptor::from_raw_ptr(ptr)
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
                    NonNull::slice_from_raw_parts(NonNull::new(content_ptr).unwrap(), PAGE_SIZE),
                );

                let tag = JarTag::in_jar(0).in_page(u64::try_from(buf_id).unwrap());

                let desc = PageDescriptorInner::new(buf_id, tag, content);

                let desc_box = Box::pin(UnsafeCell::new(desc));
                desc_box.get().as_mut().unwrap().set_new();

                let desc_ptr = NonNull::new(desc_box.get()).unwrap();
                self.descriptors.borrow_mut().insert(tag, desc_box);
                self.descriptor.borrow_mut().page_count += 1;

                MutPage::try_new(PageDescriptor::from_raw_ptr(desc_ptr))
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

#[cfg(test)]
mod tests {
    use crate::{arena::IArena, buffer::{stress::stubs::StressStub, BufferPool}};

    use super::Pager;

    #[test]
    fn test_new_element() {
        let buf_pool = BufferPool::new(4_000_000, 4096, StressStub::default().into_boxed());
        let pager = Pager::new(0, &buf_pool).unwrap();
        let page = pager.new_element().unwrap();

        assert!(buf_pool.contains(page.tag()));
    }
}