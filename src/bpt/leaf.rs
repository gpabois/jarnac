use std::{
    io::Read, mem::transmute, ops::{Div, Index, IndexMut, Range}
};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    cell::{Cell, CellCapacity, CellId, CellPage, Cells, WithCells}, error::Error, knack::{
        kind::KnackKind,
        marker::{
            kernel::AsKernelRef, sized::Sized, AsComparable, AsFixedSized, Comparable, ComparableAndFixedSized, FixedSized
        },
        Knack, KnackCell,
    }, page::{
        AsMutPageSlice, AsRefPage, AsRefPageSlice, IntoRefPageSlice, MutPage, OptionalPageId,
        PageId, PageKind, PageSize, PageSlice, RefPage, RefPageSlice,
    }, pager::IPager, result::Result, tag::{DataArea, JarTag}, utils::Shift, var::{MaybeSpilled, MaybeSpilledRef, Var}
};

use super::descriptor::BPlusTreeDescription;

pub struct BPlusTreeLeaf<Page>(CellPage<Page>);

pub type BPlusTreeLeafMut<'page> = BPlusTreeLeaf<MutPage<'page>>;
pub type BPlusTreeLeafRef<'page> = BPlusTreeLeaf<RefPage<'page>>;

impl<Page> Index<&CellId> for BPlusTreeLeaf<Page>
where
    Page: AsRefPageSlice,
{
    type Output = BPlusTreeLeafCell<PageSlice>;

    fn index(&self, index: &CellId) -> &Self::Output {
        self.borrow_cell(index).unwrap()
    }
}

impl<Page> IndexMut<&CellId> for BPlusTreeLeaf<Page>
where
    Page: AsMutPageSlice,
{
    fn index_mut(&mut self, index: &CellId) -> &mut Self::Output {
        self.borrow_mut_cell(index).unwrap()
    }
}

impl<Page> BPlusTreeLeaf<Page>
where
    Page: AsRefPage,
{
    pub fn tag(&self) -> &JarTag {
        self.0.tag()
    }
}

impl<'buf> TryFrom<RefPage<'buf>> for BPlusTreeLeaf<RefPage<'buf>> {
    type Error = Error;

    fn try_from(page: RefPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf
            .assert(kind)
            .map(move |_| Self(CellPage::from(page)))
    }
}

impl<'buf> TryFrom<MutPage<'buf>> for BPlusTreeLeaf<MutPage<'buf>> {
    type Error = Error;

    fn try_from(page: MutPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf
            .assert(kind)
            .map(move |_| Self(CellPage::from(page)))
    }
}

impl<'a, 'buf> TryFrom<&'a mut MutPage<'buf>> for BPlusTreeLeaf<&'a mut MutPage<'buf>> {
    type Error = Error;

    fn try_from(page: &'a mut MutPage<'buf>) -> Result<Self> {
        let kind: PageKind = page.as_ref().as_bytes()[0].try_into()?;
        PageKind::BPlusTreeLeaf
            .assert(kind)
            .map(move |_| Self(CellPage::from(page)))
    }
}

impl<Page> BPlusTreeLeaf<Page>
where
    Page: AsRefPageSlice,
{
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> u8 {
        self.0.len()
    }

    pub fn is_full(&self) -> bool {
        self.0.is_full()
    }

    pub fn get_parent(&self) -> Option<PageId> {
        self.as_meta().get_parent()
    }

    pub fn search_cell<Key>(&self, key: &Key) -> Option<&BPlusTreeLeafCell<PageSlice>> 
        where Key: AsComparable<Kernel=Knack>
    {
        self.iter().find(|cell| cell.borrow_key().as_comparable() == key.as_comparable())
    }

    fn borrow_cell(&self, cid: &CellId) -> Option<&BPlusTreeLeafCell<PageSlice>> {
        self.0
            .borrow_cell(cid)
            .map(|cell| unsafe { std::mem::transmute(cell) })
    }

    fn as_meta(&self) -> &BPTreeLeafMeta {
        BPTreeLeafMeta::ref_from_bytes(&self.0.as_bytes()[BPTreeLeafMeta::AREA]).unwrap()
    }
}

impl<Page> BPlusTreeLeaf<Page>
where
    Page: AsMutPageSlice,
{
    pub fn new(mut page: Page, desc: &BPlusTreeDescription) -> Result<Self> {
        page.as_mut_bytes()[0] = PageKind::BPlusTreeLeaf as u8;

        CellPage::new(
            page,
            desc.leaf_content_size(),
            desc.k(),
            BPlusTreeLeaf::<()>::reserved_space(),
        )
        .map(Self)
    }

    pub fn insert<'a, Pager: IPager<'a> + ?std::marker::Sized>(
        &mut self,
        key: &ComparableAndFixedSized<Knack>,
        value: &Knack,
        desc: &BPlusTreeDescription,
        pager: &Pager,
    ) -> Result<()> {
        let before = self
            .iter()
            .filter(|&cell| cell.borrow_key().as_comparable() >= key.as_comparable())
            .map(|cell| cell.cid())
            .last();

        match before {
            Some(before) => self.insert_before(&before, key, value, desc, pager)?,
            None => self.push(key, value, desc, pager)?,
        };

        Ok(())
    }
    pub fn split_into<'a, P>(&'a mut self, dest: &mut BPlusTreeLeaf<P>) -> Result<&'a Knack>
    where
        P: AsMutPageSlice,
    {
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
            .map(|cell| unsafe { std::mem::transmute(cell) })
    }

    fn insert_before<'a, Pager: IPager<'a> + ?std::marker::Sized>(
        &mut self,
        before: &CellId,
        key: &ComparableAndFixedSized<Knack>,
        value: &Knack,
        desc: &BPlusTreeDescription,
        pager: &Pager,
    ) -> Result<CellId> {
        let cid = self.0.insert_before(before)?;
        BPlusTreeLeafCell::initialise(&mut self[&cid], key, value, desc, pager)?;
        Ok(cid)
    }

    fn push<'a, Pager: IPager<'a> + ?std::marker::Sized>(
        &mut self,
        key: &ComparableAndFixedSized<Knack>,
        value: &Knack,
        desc: &BPlusTreeDescription,
        pager: &Pager,
    ) -> Result<CellId> {
        let cid = self.0.push()?;
        BPlusTreeLeafCell::initialise(&mut self[&cid], key, value, desc, pager)?;
        Ok(cid)
    }

    fn as_mut_meta(&mut self) -> &mut BPTreeLeafMeta {
        BPTreeLeafMeta::mut_from_bytes(&mut self.0.as_mut_bytes()[BPTreeLeafMeta::AREA]).unwrap()
    }
}

impl<'buf> IntoIterator for BPlusTreeLeaf<RefPage<'buf>> {
    type Item = BPlusTreeLeafCell<RefPageSlice<'buf>>;

    type IntoIter = std::iter::Map<
        crate::cell::OwnedRefPageCellCursor<RefPage<'buf>>,
        fn(Cell<RefPageSlice<'buf>>) -> BPlusTreeLeafCell<RefPageSlice<'buf>>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter().map(BPlusTreeLeafCell)
    }
}

impl<'buf> BPlusTreeLeaf<RefPage<'buf>> {
    pub fn into_cell(self, cid: &CellId) -> Option<BPlusTreeLeafCell<RefPageSlice<'buf>>> {
        Some(BPlusTreeLeafCell(self.0.into_cell(cid)?))
    }

    pub fn into_value(
        self,
        key: &Knack,
        key_kind: &FixedSized<KnackKind>,
        value_kind: &KnackKind,
    ) -> Option<MaybeSpilled<RefPageSlice<'buf>>> {
        self.into_iter()
            .filter(|cell| cell.borrow_key().as_comparable() == key)
            .map(|cell| cell.into_value(key_kind, value_kind))
            .last()
    }
}

impl<Page> BPlusTreeLeaf<Page>
where
    Page: AsRefPageSlice,
{
    pub fn iter(&self) -> impl Iterator<Item = &BPlusTreeLeafCell<PageSlice>> {
        self.0.iter().map(<&BPlusTreeLeafCell<PageSlice>>::from)
    }
}

impl BPlusTreeLeaf<()> {
    /// Calcule la taille du contenu d'une cellule.
    pub fn compute_cell_content_size(key: &FixedSized<KnackKind>, value_size: u16) -> u16 {
        u16::try_from(key.outer_size()).unwrap() + value_size
    }
    /// Calcule la taille disponible dans une cellule pour stocker une valeur.
    pub fn compute_available_value_space_size(
        page_size: PageSize,
        key: &FixedSized<KnackKind>,
        k: CellCapacity,
    ) -> u16 {
        let key_size = u16::try_from(key.outer_size()).unwrap();

        let max_cell_size =
            Cells::compute_available_cell_content_size(page_size, Self::reserved_space(), k);
        
        max_cell_size.saturating_sub(key_size)
    }

    /// Espace réservée dans l'entête de la page.
    pub fn reserved_space() -> u16 {
        u16::try_from(size_of::<BPTreeLeafMeta>()).unwrap()
    }

    pub fn is_compliant(
        page_size: PageSize,    
        key: &FixedSized<KnackKind>,     
        value_size: u16,
        k: CellCapacity,) -> bool {
        
        let cell_size_is_gt_zero = Self::compute_cell_content_size(key, value_size) > 0;
        let within = Self::within_available_cell_space_size(page_size, key, value_size, k);

        cell_size_is_gt_zero && within
    }

    pub fn within_available_cell_space_size(
        page_size: PageSize,
        key: &FixedSized<KnackKind>,
        value_size: u16,
        k: CellCapacity,
    ) -> bool {
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
pub struct BPlusTreeLeafCell<Slice>(Cell<Slice>)
where
    Slice: AsRefPageSlice + ?std::marker::Sized;

impl<Slice> BPlusTreeLeafCell<Slice>
where
    Slice: AsMutPageSlice + ?std::marker::Sized,
{
    /// Initialise la cellule
    pub fn initialise<'buf, Pager: IPager<'buf> + ?std::marker::Sized>(
        cell: &mut Self,
        key: &ComparableAndFixedSized<Knack>,
        value: &Knack,
        desc: &BPlusTreeDescription,
        pager: &Pager,
    ) -> Result<()> {
        let area = key.as_fixed_sized().range();

        cell.0.as_mut_content_slice().as_mut_bytes()[area].clone_from_slice(key.as_kernel_ref().as_bytes());
        cell.set_value(value, desc, pager)?;

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn borrow_mut_key(&mut self) -> &mut Comparable<FixedSized<Knack>> {
        let range: Range<usize> = self.key_area();
        let bytes = &mut self.0.as_mut_content_slice()[range];
        unsafe { transmute(Knack::from_mut(bytes)) }
    }

    /// Ecris une valeur dans la cellule de la feuille.
    pub fn set_value<'a, Pager>(&mut self, value: &Knack, desc: &BPlusTreeDescription, pager: &Pager)  -> Result<()> where Pager: IPager<'a> + ?std::marker::Sized {
        let range = self.value_area();

        let bytes = &mut self.0.as_mut_content_slice()[range];
        
        if desc.value_will_spill() {
            Var::from_mut_slice(bytes).set(value, pager)?;
        } else {
            value.as_bytes().read(bytes)?;
        }

        Ok(())
    }
}

impl<'buf> BPlusTreeLeafCell<RefPageSlice<'buf>> {
    /// Transforme la cellule en une valeur possédant une référence vers une tranche de la page.
    pub fn into_value(
        self,
        key_kind: &FixedSized<KnackKind>,
        value_kind: &KnackKind,
    ) -> MaybeSpilled<RefPageSlice<'buf>> {
        match value_kind.as_sized() {
            Sized::Fixed(sized) => {
                let value_range = sized.range().shift(key_kind.outer_size());
                let value_bytes = self.0.into_content_slice().into_page_slice(value_range);
                KnackCell::from(value_bytes).into()
            }
            Sized::Var(_) => {
                let value_range = key_kind.outer_size()..;
                let value_bytes = self.0.into_content_slice().into_page_slice(value_range);
                Var::from_owned_slice(value_bytes).into()
            }
        }
    }
}

impl<Slice> From<&Cell<Slice>> for &BPlusTreeLeafCell<Slice>
where
    Slice: AsRefPageSlice + ?std::marker::Sized,
{
    fn from(value: &Cell<Slice>) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl<Slice> From<&mut Cell<Slice>> for &BPlusTreeLeafCell<Slice>
where
    Slice: AsMutPageSlice + ?std::marker::Sized,
{
    fn from(value: &mut Cell<Slice>) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

impl<Slice> BPlusTreeLeafCell<Slice>
where
    Slice: AsRefPageSlice + ?std::marker::Sized,
{
    pub fn cid(&self) -> CellId {
        self.as_cell().id()
    }

    pub fn as_cell(&self) -> &Cell<Slice> {
        &self.0
    }

    pub fn key_kind(&self) -> &ComparableAndFixedSized<KnackKind> {
        <&ComparableAndFixedSized::<KnackKind>>::try_from(self.as_cell().as_content_slice().as_bytes()).unwrap()
    }

    pub fn borrow_key(&self) -> &ComparableAndFixedSized<Knack> {
        let slice = &self.as_cell().as_content_slice()[self.key_area()];
        unsafe { std::mem::transmute(Knack::from_ref(slice)) }
    }

    pub fn borrow_value<'leaf>(&'leaf self, desc: &BPlusTreeDescription) -> MaybeSpilledRef<'leaf> {
        let range = self.value_area();
        let bytes = &self.0.as_content_slice()[range];
        if desc.value_will_spill() {
            MaybeSpilledRef::Spilled(Var::from_ref_slice(bytes))
        } else {
            MaybeSpilledRef::Unspilled(<&Knack>::from(bytes))
        }
        
    }

    pub fn key_area(&self) -> Range<usize> {
        0..self.key_kind().as_fixed_sized().outer_size()
    }

    fn value_area(&self) -> Range<usize> {
        return self.key_area().end..usize::from(self.0.as_content_slice().len());
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use crate::{arena::IArena, bpt::{descriptor::BPlusTreeDescription, BPlusTreeArgs}, knack::marker::AsComparable, pager::stub::StubPager, prelude::IntoKnackBuf};

    use super::BPlusTreeLeaf;

    #[test]
    fn test_insert() {
        let pager = StubPager::<4096>::new();
        let desc = BPlusTreeArgs::new::<u64, str>(None).define(4096).validate().map(BPlusTreeDescription::new).unwrap();

        let mut leaf = pager.new_element().and_then(|page| BPlusTreeLeaf::new(page, &desc)).unwrap();

        let key = 18u128.into_knack_buf();
        let value = "test".into_knack_buf();

        leaf.insert(key.borrow(), &value, &desc, &pager).unwrap();
        let cell = leaf.iter().find(|&cell| cell.borrow_key().as_comparable() == key.as_comparable()).unwrap();

        let value = cell.borrow_value(&desc).assert_loaded(&pager).unwrap();
        assert_eq!(value.cast::<str>(), "test");

    }
}