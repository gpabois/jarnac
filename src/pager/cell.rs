//! Système de répartition par cellules de taille constante du contenu d'une page.
//!
//! Permet de :
//! - découper l'espace en liste chaînée réordonnable sans réaliser de déplacements de blocs de données
//! - allouer/libérer des cellules 
//! 
//! # Exigence
//! Pour que cela marche :
//! - l'entête de la page doit contenir, après le nombre magique ([crate::pager::page::PageKind]), [CellPageHeader]
//! - l'entête de la cellule doit contenir en premier lieu [CellHeader].
//! 
//! [CellPageHeader] est utilisé pour piloter les cellules, notamment via :
//! - [CellPageHeader::push], et ses variantes [CellPageHeader::push_after] ou [CellPageHeader::push_before]
//! - [CellPageHeader::iter_cells_bytes]


use std::{marker::PhantomData, num::NonZeroU8, ops::{Add, Deref, DerefMut, Mul, Range}};

use zerocopy::{Immutable, KnownLayout, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{error::PagerError, page::{IntoPageSlice, MutPage, PageData, RefPage, RefPageSlice, TryIntoRefFromBytes, WeakPage}, PagerResult};

/// Insère une nouvelle cellule avant une autre.
fn insert_page_cell_before<Page: DerefMut<Target=[u8]>>(page: &mut Page, before: &CellId) -> PagerResult<CellId> {
    let cid = alloc_page_cell(page)?;

    let maybe_prev: Option<CellId> = {
        let cell = get_mut_cell(page, before);
        let prev = cell.header.prev;
        cell.header.prev = Some(cid).into();
        prev.into()
    };

    match maybe_prev {
        None => {
            set_free_head(page, Some(cid));
            get_mut_cell(page, &cid).header.next = Some(*before).into();
        },
        Some(prev) => {
            get_mut_cell(page, &prev).header.next = Some(cid).into();
            let cell = get_mut_cell(page, &cid);
            cell.header.prev = Some(prev).into();
            cell.header.next = Some(*before).into();
        }
    }

    Ok(cid)
}

/// Alloue une nouvelle cellule au sein de la page, si on en a assez.
fn alloc_page_cell<Page: DerefMut<Target=[u8]>>(page: &mut Page) -> PagerResult<CellId> {
    if is_full(page) {
        return Err(PagerError::new(super::error::PagerErrorKind::CellPageFull));
    }

    let cid = pop_free_cell(page).unwrap_or_else(|| {
        let cid_u8 = inc_len(page);
        CellId(NonZeroU8::new(cid_u8).unwrap())
    });

    let cp: &mut CellPage = CellPage::try_mut_from_bytes(page).unwrap();
    let cell_range = cp.header.get_cell_range(&cid);
    let cell_bytes = &mut page.deref_mut()[cell_range];
    
    let cell = Cell::try_mut_from_bytes(cell_bytes).unwrap();
    cell.header = CellHeader::default();
    cell.data.fill(0);

    Ok(cid)
}

fn is_head<Page: Deref<Target=[u8]>>(page: &Page, cid: &CellId) -> bool {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();
    cp.header.head_cell == Some(*cid).into()
}

fn is_full<Page: Deref<Target=[u8]>>(page: &Page) -> bool {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();
    cp.header.len >= cp.header.capacity
}

fn len<Page: Deref<Target=[u8]>>(page: &Page) -> u8 {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();
    cp.header.len
}

fn inc_len<Page: DerefMut<Target=[u8]>>(page: &mut Page) -> u8 {
    let cp = CellPage::try_mut_from_bytes(page).unwrap();
    cp.header.len += 1;
    cp.header.len
}

fn dec_len<Page: DerefMut<Target=[u8]>>(page: &mut Page) -> u8 {
    let cp = CellPage::try_mut_from_bytes(page).unwrap();
    cp.header.len -= 1;
    cp.header.len
}

fn dec_free_len<Page: DerefMut<Target=[u8]>>(page: &mut Page) {
    let cp = CellPage::try_mut_from_bytes(page).unwrap();
    cp.header.free_len -= 1;
}

fn inc_free_len<Page: DerefMut<Target=[u8]>>(page: &mut Page) {
    let cp = CellPage::try_mut_from_bytes(page).unwrap();
    cp.header.free_len += 1;
}

fn set_free_head<Page: DerefMut<Target=[u8]>>(page: &mut Page, head: Option<CellId>) {
    let cp = CellPage::try_mut_from_bytes(page).unwrap();
    cp.header.free_head_cell = head.into();
}

fn get_free_head<Page: Deref<Target = [u8]>>(page: &Page) -> Option<CellId> {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();
    cp.header.free_head_cell.into()
}

fn get_mut_cell<'a, Page: DerefMut<Target=[u8]>>(page: &'a mut Page, cid: &CellId) -> &'a mut Cell {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();
    let cell_range = cp.get_cell_range(cid).unwrap();
    let cell_bytes = &mut page.deref_mut()[cell_range];
    Cell::try_mut_from_bytes(cell_bytes).unwrap()
}

fn set_previous_sibling<Page: DerefMut<Target=[u8]>>(page: &mut Page, cid: &CellId, previous: Option<CellId>) {
    let cell = get_mut_cell(page, cid);
    cell.header.prev = previous.into()
}

fn previous_sibling<Page: Deref<Target=[u8]>>(page: &Page, cid: &CellId) -> Option<CellId> {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();

    let cell_range = cp.get_cell_range(&cid).unwrap();
    let cell_bytes = &page.deref()[cell_range];
    let cell = Cell::try_ref_from_bytes(cell_bytes).unwrap();

    cell.header.prev.into()
}

fn set_next_sibling<Page: DerefMut<Target=[u8]>>(page: &mut Page, cid: &CellId, next: Option<CellId>) {
    let cell = get_mut_cell(page, cid);
    cell.header.next = next.into()
}

fn next_sibling<Page: Deref<Target=[u8]>>(page: &Page, cid: &CellId) -> Option<CellId> {
    let cp = CellPage::try_ref_from_bytes(page).unwrap();

    let cell_range = cp.get_cell_range(&cid).unwrap();
    let cell_bytes = &page.deref()[cell_range];
    let cell = Cell::try_ref_from_bytes(cell_bytes).unwrap();

    cell.header.next.into()
}


/// Retire la cellule de la liste chaînée
fn detach_cell<Page: DerefMut<Target=[u8]>>(page: &mut Page, cid: &CellId) {
    let prev = previous_sibling(page, cid);
    let next = next_sibling(page, cid);

    set_previous_sibling(page, cid, None);
    set_next_sibling(page, cid, None);

    prev.inspect(|prev| set_next_sibling(page, prev, next));
    next.inspect(|next| set_previous_sibling(page, next, prev));
} 

/// Insère une nouvelle cellule dans la liste des cellules libres.
fn push_free_cell<Page: DerefMut<Target=[u8]>>(page: &mut Page, cid: &CellId) {
    let head = get_free_head(page);
    head.inspect(|head| set_previous_sibling(page, head, Some(*cid)));

    set_free_head(page, Some(*cid));
    inc_free_len(page);
}

fn pop_free_cell<Page: DerefMut<Target=[u8]>>(page: &mut Page) -> Option<CellId> {
    if let Some(head) = get_free_head(page) {
        let maybe_next = next_sibling(page, &head);
        
        maybe_next.inspect(|next| {
            set_previous_sibling(page, &next, None);
        });
        
        set_free_head(page, maybe_next);
        dec_free_len(page);

        return Some(head)
    }

    return None
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente une cellule 
pub struct Cell {
    header: CellHeader,
    data: [u8]
}

/// Une référence vers une cellule de page.
pub struct RefPageCell<Slice> 
where Slice: Deref<Target = [u8]>
{
    pub(crate) cid: CellId,
    pub(crate) cell_bytes: Slice
}

impl<Slice, Output> TryIntoRefFromBytes<Output> for RefPageCell<Slice> 
where Slice: Deref<Target = [u8]>,
      Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_ref_from_bytes(&self) -> &Output {
        Output::try_ref_from_bytes(self.cell_bytes.deref()).unwrap()
    }
}

/// Un curseur sur les cellules d'une page.
pub struct RefPageCellCursor<'page, Page> 
where Page: PageData<'page>
{
    _pht: PhantomData<&'page ()>,
    page: Page,
    current: Option<CellId>
}

impl<'page, Page> Iterator for RefPageCellCursor<'page, Page> 
where Page: PageData<'page> + Clone 
{
    type Item = RefPageCell<<Page as IntoPageSlice<std::ops::Range<usize>>>::Output>;

    fn next(&mut self) -> Option<Self::Item> {
        let cell_page = CellPage::try_ref_from_bytes(&self.page).unwrap();

        match self.current {
            Some(cid) => {
                let idx = cell_page.header.get_cell_range(&cid);
                let cell_bytes = self.page.clone().into_page_slice(idx);
                self.current = next_sibling(&self.page, &cid);
                Some(RefPageCell { cid, cell_bytes })
            },
            None => None
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
/// En-tête d'une cellule.
pub struct CellHeader {
    prev: OptionalCellId,
    next: OptionalCellId,
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct CellPage {
    kind: u8,
    header: CellPageHeader,
    body: [u8]
}

impl CellPage {
    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        let loc = (*cid) * self.header.cell_size + self.header.cell_base;
        Some(loc.into_range(&self.header.cell_size))
    }

    pub fn iter<'page, Page>(page: Page) -> RefPageCellCursor<'page, Page> 
    where Page: PageData<'page> + Clone
    {
        let cp = Self::try_ref_from_bytes(&page).unwrap();
        let current = cp.header.head_cell.into();
        RefPageCellCursor { _pht: PhantomData, page, current }
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// En-tête de la page contenant les informations relatives aux cellules qui y sont stockées.
pub struct CellPageHeader {
    /// Taille d'une cellule
    cell_size: CellSize,
    /// Nombre de cellules alloués
    len: u8,
    /// Nombre maximal de cellules stockables.
    capacity: u8,
    /// Nombre de cellules libres
    free_len: u8,
    /// Tête de la liste des cellules libérées
    free_head_cell: OptionalCellId,
    /// Tête de a liste des cellules allouées
    head_cell: OptionalCellId,
    /// Localisation de la base des cellules
    cell_base: u16
}

impl CellPageHeader {
    pub fn new(cell_size: CellSize, capacity: u8, base: u16) -> Self {
        return Self {
            cell_size,
            capacity,
            len: 0,
            free_len: 0,
            free_head_cell: None.into(),
            head_cell: None.into(),
            cell_base: base
        }
    }

    pub fn is_full(&self) -> bool {
        self.len - self.free_len >= self.capacity
    }

    /// Récupère le range pour cibler la tranche relative à une cellule
    fn get_cell_range(&self, cid: &CellId) -> Range<usize> {
        let start = (*cid) * self.cell_size;
        let end = start + self.cell_size;
        usize::try_from(start.0).unwrap()..usize::try_from(end.0).unwrap()
    }

}

#[derive(Clone, Copy)]
pub struct CellId(NonZeroU8);

impl Mul<CellSize> for CellId {
    type Output = CellLocation;

    fn mul(self, rhs: CellSize) -> Self::Output {
        CellLocation(u16::from(self.0.get()) * u16::from(rhs.0))
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(transparent)]
pub struct CellSize(u16);

impl From<u16> for CellSize {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy)]
pub struct CellLocation(u16);

impl CellLocation {
    pub fn into_range(self, size: &CellSize) -> Range<usize> {
        let start = usize::from(self.0);
        let end = start + usize::from(size.0);
        start..end
    }
}

impl Add<CellSize> for CellLocation {
    type Output = CellLocation;

    fn add(mut self, rhs: CellSize) -> Self::Output {
       self.0 += u16::from(rhs.0);
       self
    }
}

impl Add<u16> for CellLocation {
    type Output = CellLocation;

    fn add(self, rhs: u16) -> Self::Output {
        Self(self.0 + rhs)
    }
}

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptionalCellId(Option<NonZeroU8>);

impl Into<Option<CellId>> for OptionalCellId {
    fn into(self) -> Option<CellId> {
        self.0.map(CellId)
    }
}

impl From<Option<CellId>> for OptionalCellId {
    fn from(value: Option<CellId>) -> Self {
        Self(value.map(|n| n.0))
    }
}

impl AsRef<Option<CellId>> for OptionalCellId {
    fn as_ref(&self) -> &Option<CellId> {
        unsafe {
            std::mem::transmute(self)
        }
    }
}

impl Mul<u32> for CellId {
    type Output = u32;

    fn mul(self, rhs: u32) -> Self::Output {
        u32::from(self.0.get()) * rhs
    }
}

