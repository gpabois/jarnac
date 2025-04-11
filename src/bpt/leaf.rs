
use std::{mem::transmute, ops::{Div, Index, IndexMut, Range, RangeFrom}};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    cell::{Cell, CellCapacity, CellId, CellPage, Cells, WithCells}, error::Error, knack::{kind::KnackKind, marker::{kernel::{AsKernelMut, AsKernelRef}, sized::Sized, AsComparable, AsFixedSized, Comparable, FixedSized}, Knack, KnackCell}, page::{AsMutPageSlice, AsRefPage, AsRefPageSlice, IntoRefPageSlice, MutPage, OptionalPageId, PageId, PageKind, PageSize, PageSlice, RefPage, RefPageSlice}, pager::IPager, result::Result, tag::{DataArea, JarTag}, utils::Shift, var::{MaybeSpilled, Var}
};

use super::descriptor::BPlusTreeDescription;

pub struct BPlusTreeLeaf<Page>(CellPage<Page>);

pub type BPlusTreeLeafMut<'page> = BPlusTreeLeaf<MutPage<'page>>;
pub type BPlusTreeLeafRef<'page> = BPlusTreeLeaf<RefPage<'page>>;

impl<Page> Index<&CellId> for BPlusTreeLeaf<Page> where Page: AsRefPageSlice {
    type Output = BPlusTreeLeafCell<PageSlice>;

    fn index(&self, index: &CellId) -> &Self::Output {
        self.borrow_cell(index).unwrap()
    }
}

impl<Page> IndexMut<&CellId> for BPlusTreeLeaf<Page> where Page: AsMutPageSlice {
    fn index_mut(&mut self, index: &CellId) -> &mut Self::Output {
        self.borrow_mut_cell(index).unwrap()
    }
}

impl<Page> BPlusTreeLeaf<Page> where Page: AsRefPage {
    pub fn tag(&self) -> &JarTag {
        self.0.tag()
    }
}

impl<'buf> TryFrom<RefPage<'buf>> for BPlusTreeLeaf<RefPage<'buf>> {
    type Error = Error;

    fn try_from(page: RefPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}

impl<'buf> TryFrom<MutPage<'buf>> for BPlusTreeLeaf<MutPage<'buf>> {
    type Error = Error;

    fn try_from(page: MutPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}

impl<'a, 'buf> TryFrom<&'a mut MutPage<'buf>> for BPlusTreeLeaf<&'a mut MutPage<'buf>> {
    type Error = Error;

    fn try_from(page: &'a mut MutPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf.assert(kind).map(move |_| Self(CellPage::from(page)))
    }
}


impl<Page> BPlusTreeLeaf<Page> where Page: AsRefPageSlice {
    pub fn len(&self) -> u8 {
        self.0.len()
    }

    pub fn is_full(&self) -> bool {
        self.0.is_full()
    }

    pub fn get_parent(&self) -> Option<PageId> {
        self.as_meta().get_parent()
    }

    fn borrow_cell(&self, cid: &CellId) -> Option<&BPlusTreeLeafCell<PageSlice>> {
        self.0
        .borrow_cell(cid)
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }

    fn as_meta(&self) -> &BPTreeLeafMeta {
        BPTreeLeafMeta::ref_from_bytes(&self.0.as_bytes()[BPTreeLeafMeta::AREA]).unwrap()
    }
}

impl<Page> BPlusTreeLeaf<Page> where Page: AsMutPageSlice {
    pub fn new(mut page: Page, desc: &BPlusTreeDescription) -> Result<Self> {
        page.as_mut_bytes()[0] = PageKind::BPlusTreeLeaf as u8;
        
        CellPage::new(
            page, 
            desc.leaf_content_size(),
            desc.k(),
            BPlusTreeLeaf::<()>::reserved_space()
        ).map(Self)
    }

    pub fn insert<'a, Pager: IPager<'a> + ?std::marker::Sized>(&mut self, key: &Comparable<Knack>, value: &Knack, pager: &Pager) -> Result<()> {
        let before = self.iter()
            .filter(|&cell| cell.borrow_key().as_comparable() >= &key)
            .map(|cell| cell.cid())
            .last();

        match before {
            Some(before) => self.insert_before(&before, &key, &value, pager)?,
            None => self.push(&key, &value, pager)?,
        };

        Ok(())
    }
    pub fn split_into<'a, P>(&mut self, dest: &mut BPlusTreeLeaf<P>) -> Result<&Knack> where P: AsMutPageSlice {
        let at = self.len().div(2) + 1;
        self.0.split_at_into(&mut dest.0, at)?;       
        let key = self.iter().last().map(|cell| cell.borrow_key()).unwrap();
        Ok(key.as_kernel_ref())
    }

    pub fn set_next(&mut self, next: Option<PageId>) {
        self.as_mut_meta().set_next(next);
    }

    pub fn set_prev(&mut self, prev: Option<PageId>) {
        self.as_mut_meta().set_prev(prev);
    }

    pub fn set_parent(&mut self, parent: Option<PageId>) {
        self.as_mut_meta().set_parent(parent);
    }

    pub fn as_mut_page(&mut self) -> &mut Page {
        self.0.as_mut()
    }

    fn borrow_mut_cell(&mut self, cid: &CellId) -> Option<&mut BPlusTreeLeafCell<PageSlice>> {
        self.0
        .borrow_mut_cell(cid)
        .map(|cell| unsafe {
            std::mem::transmute(cell)
        })
    }

    fn insert_before<'a, Pager: IPager<'a> + ?std::marker::Sized>(&mut self, before: &CellId, key: &Knack, value: &Knack, pager: &Pager) -> Result<CellId> {
        let cid = self.0.insert_before(before)?;
        BPlusTreeLeafCell::initialise(&mut self[&cid], key, value, pager)?;
        Ok(cid)    
    }

    fn push<'a, Pager: IPager<'a> + ?std::marker::Sized>(&mut self, key: &Knack, value: &Knack, pager: &Pager) -> Result<CellId> {
        let cid = self.0.push()?;
        BPlusTreeLeafCell::initialise(&mut self[&cid], key, value, pager)?;
        Ok(cid)
    }

    fn as_mut_meta(&mut self) -> &mut BPTreeLeafMeta {
        BPTreeLeafMeta::mut_from_bytes(&mut self.0.as_mut_bytes()[BPTreeLeafMeta::AREA]).unwrap()
    }
}

impl<'buf> BPlusTreeLeaf<RefPage<'buf>> {
    pub fn into_iter(self) -> impl Iterator<Item=BPlusTreeLeafCell<RefPageSlice<'buf>>> {
        self.0
        .into_iter()
        .map(BPlusTreeLeafCell)
    }

    pub fn into_cell(self, cid: &CellId) -> Option<BPlusTreeLeafCell<RefPageSlice<'buf>>> {
        Some(BPlusTreeLeafCell(self.0.into_cell(cid)?))
    }

    pub fn into_value(self, key: &Knack, key_kind: &FixedSized<KnackKind>, value_kind: &KnackKind) -> Option<MaybeSpilled<RefPageSlice<'buf>>> {
        self
        .into_iter()
        .filter(|cell| {
            cell
                .borrow_key()
                .as_comparable() == key
        })
        .map(|cell| cell.into_value(key_kind, value_kind))
        .last()
    }
}

impl<Page> BPlusTreeLeaf<Page> where Page: AsRefPageSlice {
    pub fn iter(&self) -> impl Iterator<Item=&BPlusTreeLeafCell<PageSlice>> {
        self.0.iter().map(|cell| {
            <&BPlusTreeLeafCell<PageSlice>>::from(cell)
        })
    }
}

impl BPlusTreeLeaf<()> {
    pub fn compute_cell_content_size(key: &FixedSized<KnackKind>, value_size: u16) -> u16 {
        u16::try_from(key.outer_size()).unwrap() + value_size
    }
    /// Calcule la taille disponible dans une cellule pour stocker une valeur.
    pub fn compute_available_value_space_size(page_size: PageSize, key: &FixedSized<KnackKind>, k: CellCapacity) -> u16 {
        let key_size = u16::try_from(key.outer_size()).unwrap();
        let max_cell_size = Cells::compute_available_cell_content_size(page_size, Self::reserved_space(), k);
        max_cell_size - key_size
    }

    pub fn reserved_space() -> u16 {
        u16::try_from(size_of::<BPTreeLeafMeta>()).unwrap()
    }

    pub fn within_available_cell_space_size(page_size: PageSize, key: &FixedSized<KnackKind>, value_size: u16, k: CellCapacity) -> bool {
        let content_size = Self::compute_cell_content_size(key, value_size);
        Cells::within_available_cell_space_size(page_size, Self::reserved_space(), content_size, k)
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête d'une [feuille](self::BPTreeLeafPage).
pub struct BPTreeLeafMeta {
    pub(super) parent: OptionalPageId,
    pub(super) prev: OptionalPageId,
    pub(super) next: OptionalPageId,
}

impl BPTreeLeafMeta {
    pub fn get_parent(&self) -> Option<PageId> {
        self.parent.into()
    }
    pub fn set_parent(&mut self, parent: Option<PageId>) {
        self.parent = parent.into();
    }
    pub fn set_prev(&mut self, prev: Option<PageId>) {
        self.prev = prev.into()
    }
    pub fn set_next(&mut self, next: Option<PageId>) {
        self.next = next.into()
    }
}

impl DataArea for BPTreeLeafMeta {
    const AREA: std::ops::Range<usize> = WithCells::<Self>::AREA;
}

/// Cellule d'une feuille contenant une paire clé/valeur.
pub struct BPlusTreeLeafCell<Slice>(Cell<Slice>) where Slice: AsRefPageSlice + ?std::marker::Sized;

impl<Slice> BPlusTreeLeafCell<Slice> where Slice: AsMutPageSlice + ?std::marker::Sized {
    /// Initialise la cellule
    pub fn initialise<'buf, Pager: IPager<'buf> + ?std::marker::Sized>(cell: &mut Self, key: &Knack, value: &Knack, pager: &Pager) -> Result<()> {
        let area = KnackKind::AREA;
        
        cell.0
            .as_mut_content_slice()
            .as_mut_bytes()[area]
            .clone_from_slice(key.kind().as_bytes());

        cell.borrow_mut_key().as_kernel_mut().set(key);
        cell.borrow_mut_value().set(value, pager)?;

        Ok(())
    }

    pub(crate) fn borrow_mut_key(&mut self) -> &mut Comparable<FixedSized<Knack>> {
        let range = self.key_area();
        let bytes = &mut self.0.as_mut_content_slice()[range];
        unsafe {
            transmute(Knack::from_mut(bytes))
        }
    }
    
    pub fn borrow_mut_value(&mut self) -> &mut Var<PageSlice> {
        let range = self.value_area();
        let bytes = &mut self.0.as_mut_content_slice()[range];
        Var::from_mut_slice(bytes)   
    }
}


impl<'buf> BPlusTreeLeafCell<RefPageSlice<'buf>> {
    /// Transforme la cellule en une valeur possédant une référence vers une tranche de la page.
    pub fn into_value(self, key_kind: &FixedSized<KnackKind>, value_kind: &KnackKind) -> MaybeSpilled<RefPageSlice<'buf>> {
        match value_kind.as_sized() {
            Sized::Fixed(sized) => {
                let value_range = sized.as_area().shift(key_kind.outer_size());
                let value_bytes = self.0.into_content_slice().into_page_slice(value_range);
                KnackCell::from(value_bytes).into()
            },
            Sized::Var(_) => {
                let value_range = key_kind.outer_size()..;
                let value_bytes = self.0.into_content_slice().into_page_slice(value_range);
                Var::from_owned_slice(value_bytes).into()
            },
        }
    }
}

impl<Slice> From<&Cell<Slice>> for &BPlusTreeLeafCell<Slice> where Slice: AsRefPageSlice + ?std::marker::Sized {
    fn from(value: &Cell<Slice>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl<Slice> From<&mut Cell<Slice>> for &BPlusTreeLeafCell<Slice> where Slice: AsMutPageSlice + ?std::marker::Sized {
    fn from(value: &mut Cell<Slice>) -> Self {
        unsafe {
            std::mem::transmute(value)
        }
    }
}

impl<Slice> BPlusTreeLeafCell<Slice> 
where Slice: AsRefPageSlice + ?std::marker::Sized
{
    pub fn cid(&self) -> CellId {
        self.as_cell().id()
    }

    pub fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }

    pub fn borrow_key_kind(&self) -> &Comparable<FixedSized<KnackKind>> {
        let kernel: &KnackKind = self.as_cell()
            .as_content_slice()[..size_of::<KnackKind>()]
            .as_bytes()
            .try_into()
            .unwrap();
        
        unsafe {
            std::mem::transmute(kernel)
        }
    }

    pub fn borrow_key(&self) -> &Comparable<FixedSized<Knack>> {
        let slice = &self.as_cell().as_content_slice()[self.key_area()];
        unsafe {
            std::mem::transmute(Knack::from_ref(slice))
        }
    }

    pub fn key_area(&self) -> Range<usize> {
        0..self.borrow_key_kind().as_fixed_sized().outer_size()
    }

    fn value_area(&self) -> RangeFrom<usize> {
        return self.key_area().end..    
    }
}

