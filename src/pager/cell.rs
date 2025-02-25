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
//! [CellPage] est utilisé pour piloter les cellules, notamment via :
//! - [CellPageHeader::push], et ses variantes [CellPageHeader::insert_after] ou [CellPageHeader::insert_before]
//! - [CellPageHeader::iter]

use std::{num::NonZeroU8, ops::{Add, Deref, Mul, Range}};

use zerocopy::{Immutable, KnownLayout, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{error::PagerError, page::{IntoMutPageSlice, IntoRefPageSlice, MutPageData, MutPageSliceData, PageSlice, PageSliceData, RefPageData, TryIntoMutFromBytes, TryIntoRefFromBytes}, PagerResult};

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct CellPageData {
    kind: u8,
    header: CellPageHeader,
    body: [u8]
}

impl CellPageData {
    /// Initialise une page cellulaire
    pub fn new<Page>(page: &mut Page, header: CellPageHeader) 
        where Page: MutPageData 
    {
        let cp = Self::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header = header;
    }

    /// Récupère un intervalle permettant de cibler une cellule.
    pub fn get_cell_range<Page>(page: &Page, cid: &CellId) -> Option<Range<usize>> 
        where Page: RefPageData
    {
        let cp = Self::try_ref_from_bytes(page.as_ref()).unwrap();
        let loc = (*cid) * cp.header.cell_size + cp.header.cell_base;
        Some(loc.into_range(&cp.header.cell_size))
    }

    pub fn borrow_cell_slice<'a, Page>(page: &'a Page, cid: &CellId) -> Option<CellSlice<&'a PageSlice>> 
        where Page: RefPageData
    {
        let idx = Self::get_cell_range(page, cid)?;
        let cell_bytes = page.borrow_page_slice(idx);
        Some(CellSlice {cid: *cid, bytes: cell_bytes})
    }

    pub fn borrow_mut_cell_slice<'a, Page>(page: &'a mut Page, cid: &CellId) -> Option<CellSlice<&'a mut PageSlice>> 
        where Page: MutPageData
    {
        let idx = Self::get_cell_range(page, cid)?;
        let cell_bytes = page.borrow_mut_page_slice(idx);
        Some(CellSlice {cid: *cid, bytes: cell_bytes})
    }

    /// Itère sur les cellules de la page.
    pub fn iter<Page>(page: &Page) -> PageCellCursor<Page> 
    where Page: RefPageData
    {
        let cp = Self::try_ref_from_bytes(page.as_ref()).unwrap();
        let current = cp.header.head_cell.into();
        PageCellCursor { page, current }
    }

    /// Insère une nouvelle cellule à la fin de la liste chaînée.
    pub fn push<Page>(page: &mut Page) -> PagerResult<CellId> 
        where Page: MutPageData
    {
        let cid = Self::alloc_page_cell(page)?;
        let maybe_tail_cid = Self::iter(page).last();

        if let Some(tail_cid) = maybe_tail_cid {
            Self::get_mut_cell(page, &tail_cid).unwrap().header.next = Some(cid).into();
            Self::get_mut_cell(page, &cid).unwrap().header.prev = Some(cid).into();
        } else {
            Self::set_head(page, Some(cid));
        }

        Ok(cid)
    }

    /// Insère une nouvelle cellule avant une autre.
    pub fn insert_before<Page>(page: &mut Page, before: &CellId) -> PagerResult<CellId> 
        where Page: MutPageData
    {
        let cid = Self::alloc_page_cell(page)?;

        let maybe_prev: Option<CellId> = {
            let cell = Self::get_mut_cell(page, before).unwrap();
            let prev = cell.header.prev;
            cell.header.prev = Some(cid).into();
            prev.into()
        };

        match maybe_prev {
            None => {
                Self::set_head(page, Some(cid));
                Self::get_mut_cell(page, &cid).unwrap().header.next = Some(*before).into();
            },
            Some(prev) => {
                Self::get_mut_cell(page, &prev).unwrap().header.next = Some(cid).into();
                let cell = Self::get_mut_cell(page, &cid).unwrap();
                cell.header.prev = Some(prev).into();
                cell.header.next = Some(*before).into();
            }
        }

        Ok(cid)
    }

    pub fn previous_sibling<Page>(page: &Page, cid: &CellId) -> Option<CellId> 
    where Page: for<'page> RefPageData
    {
        let idx = CellPageData::get_cell_range(page, cid)?;
        let cell_bytes = page.borrow_page_slice(idx);
        let cell = Cell::try_ref_from_bytes(cell_bytes.as_ref()).unwrap();

        cell.header.prev.into()
    }

    pub fn next_sibling<Page>(page: &Page, cid: &CellId) -> Option<CellId> where Page: RefPageData {
        let cell = CellPageData::get_cell(page, cid).unwrap();
        cell.header.next.into()
    }
}

impl CellPageData {
    /// Alloue une nouvelle cellule au sein de la page, si on en a assez.
    fn alloc_page_cell<Page>(page: &mut Page) -> PagerResult<CellId> where Page: MutPageData {
        if Self::is_full(page) {
            return Err(PagerError::new(super::error::PagerErrorKind::CellPageFull));
        }

        let cid = Self::pop_free_cell(page).unwrap_or_else(|| {
            let cid_u8 = Self::inc_len(page);
            CellId(NonZeroU8::new(cid_u8).unwrap())
        });
        
        let cell = CellPageData::get_mut_cell(page, &cid).unwrap();
        cell.header = CellHeader::default();
        cell.data.fill(0);

        Ok(cid)
    }

    #[allow(dead_code)]
    fn is_head<Page: Deref<Target=[u8]>>(page: &Page, cid: &CellId) -> bool {
        let cp = CellPageData::try_ref_from_bytes(page).unwrap();
        cp.header.head_cell == Some(*cid).into()
    }

    fn is_full<Page>(page: &Page) -> bool 
        where Page: for<'page> RefPageData
    {
        let cp = CellPageData::try_ref_from_bytes(page.as_ref()).unwrap();
        cp.header.len >= cp.header.capacity
    }

    #[allow(dead_code)]
    fn len<Page: Deref<Target=[u8]>>(page: &Page) -> u8 {
        let cp = CellPageData::try_ref_from_bytes(page).unwrap();
        cp.header.len
    }

    fn inc_len<Page>(page: &mut Page) -> u8 
    where Page: for<'a> MutPageData
    {
        let cp = CellPageData::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header.len += 1;
        cp.header.len
    }

    #[allow(dead_code)]
    fn dec_len<Page>(page: &mut Page) -> u8 
    where Page: for<'a> MutPageData
    {
        let cp = CellPageData::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header.len -= 1;
        cp.header.len
    }

    fn dec_free_len<Page>(page: &mut Page) 
        where Page: for<'a> MutPageData
    {
        let cp = CellPageData::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header.free_len -= 1;
    }

    fn inc_free_len<Page>(page: &mut Page) 
        where Page: for<'a> MutPageData
    {
        let cp = CellPageData::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header.free_len += 1;
    }

    fn set_head<Page>(page: &mut Page, head: Option<CellId>) 
        where Page: for<'a> MutPageData
    {
        let cp = CellPageData::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header.head_cell = head.into();
    }

    fn set_free_head<Page>(page: &mut Page, head: Option<CellId>) 
        where Page: for<'a> MutPageData
    {
        let cp = CellPageData::try_mut_from_bytes(page.as_mut()).unwrap();
        cp.header.free_head_cell = head.into();
    }

    fn get_free_head<Page>(page: &Page) -> Option<CellId> 
        where Page: for<'a> RefPageData
    {
        let cp = CellPageData::try_ref_from_bytes(page.as_ref()).unwrap();
        cp.header.free_head_cell.into()
    }

    fn get_cell<'a, Page>(page: &'a Page, cid: &CellId) -> Option<&'a Cell>
        where Page: RefPageData 
    {
        let idx = Self::get_cell_range(page, cid)?;
        let cell_bytes = page.borrow_page_slice(idx);
        Some(Cell::try_ref_from_bytes(cell_bytes.as_ref()).unwrap())
    }

    fn get_mut_cell<'a, Page>(page: &'a mut Page, cid: &CellId) -> Option<&'a mut Cell>
        where for<'page> Page: MutPageData
    {
        let idx = Self::get_cell_range(page, cid)?;
        let cell_bytes = page.borrow_mut_page_slice(idx);
        Some(Cell::try_mut_from_bytes(cell_bytes).unwrap())
    }

    fn set_previous_sibling<Page>(page: &mut Page, cid: &CellId, previous: Option<CellId>) 
    where Page: for<'page> MutPageData
    {
        let cell = Self::get_mut_cell(page, cid).unwrap();
        cell.header.prev = previous.into()
    }   



    #[allow(dead_code)]
    fn set_next_sibling<Page>(page: &mut Page, cid: &CellId, next: Option<CellId>) 
        where Page: for<'a> MutPageData
    {
        let cell = Self::get_mut_cell(page, cid).unwrap();
        cell.header.next = next.into()
    }



    #[allow(dead_code)]
    /// Retire la cellule de la liste chaînée
    fn detach_cell<Page>(page: &mut Page, cid: &CellId) 
        where Page: for<'a> MutPageData
    {
        let prev = Self::previous_sibling(page, cid);
        let next = Self::next_sibling(page, cid);

        Self::set_previous_sibling(page, cid, None);
        Self::set_next_sibling(page, cid, None);

        prev.inspect(|prev| Self::set_next_sibling(page, prev, next));
        next.inspect(|next| Self::set_previous_sibling(page, next, prev));
    } 

    #[allow(dead_code)]
    /// Insère une nouvelle cellule dans la liste des cellules libres.
    fn push_free_cell<Page>(page: &mut Page, cid: &CellId) 
        where Page: for<'a> MutPageData
    {
        let head = Self::get_free_head(page);
        head.inspect(|head| Self::set_previous_sibling(page, head, Some(*cid)));

        Self::set_free_head(page, Some(*cid));
        Self::inc_free_len(page);
    }

    /// Retire une cellule de la liste des cellules libres.
    fn pop_free_cell<Page>(page: &mut Page) -> Option<CellId> 
        where Page: for<'a> MutPageData
    {
        if let Some(head) = Self::get_free_head(page) {
            let maybe_next = Self::next_sibling(page, &head);
            
            maybe_next.inspect(|next| {
                Self::set_previous_sibling(page, &next, None);
            });
            
            Self::set_free_head(page, maybe_next);
            Self::dec_free_len(page);

            return Some(head)
        }

        return None
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
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente une cellule 
pub struct Cell {
    header: CellHeader,
    data: [u8]
}

/// Une référence vers une cellule de page.
pub struct CellSlice<Slice> 
{
    #[allow(dead_code)]
    pub(crate) cid: CellId,
    pub(crate) bytes: Slice
}

impl<Slice> AsRef<[u8]> for CellSlice<Slice> where Slice: AsRef<[u8]> {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

impl<Slice, Output> TryIntoRefFromBytes<Output> for CellSlice<Slice> 
where Slice: PageSliceData,
      Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_ref_from_bytes(&self) -> &Output {
        Output::try_ref_from_bytes(self.bytes.as_ref()).unwrap()
    }
}


impl<Slice, Output> TryIntoMutFromBytes<Output> for CellSlice<Slice> 
where Slice: MutPageSliceData,
      Output: TryFromBytes + KnownLayout + Immutable + ?Sized
{
    fn try_into_mut_from_bytes(&mut self) -> &mut Output {
        Output::try_mut_from_bytes(self.bytes.as_mut()).unwrap()
    }
}

/// Un curseur sur les cellules d'une page.
pub struct PageCellCursor<'a, Page> 
where Page: RefPageData
{
    page: &'a Page,
    current: Option<CellId>
}

impl<Page> Iterator for PageCellCursor<'_, Page> 
where Page: RefPageData
{
    type Item = CellId;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = CellPageData::next_sibling(self.page, &cid);
                Some(cid)
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

#[cfg(test)]
mod tests {
    use std::{error::Error, rc::Rc};

    use crate::{fs::in_memory::InMemoryFs, pager::{page::PageSize, Pager, PagerOptions}};

    use super::{CellHeader, CellPageData, CellPageHeader, CellSize};


    #[test]
    fn test_cells() -> Result<(), Box<dyn Error>> {
        let fs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(fs, "memory", PageSize::new(4_096), PagerOptions::default())?.into_boxed();

        let header = CellPageHeader::new(CellSize::from(500), 4, u16::try_from(size_of::<CellHeader>()).unwrap());

        let pid = pager.new_page()?;
        let mut page =  pager.get_mut_page(&pid)?;
        
        CellPageData::new(&mut page, header);
        assert_eq!(CellPageData::len(&page), 0);

        let c1 = CellPageData::push(&mut page)?;
        assert_eq!(CellPageData::len(&page), 1);

        let c2 = CellPageData::insert_before(&mut page, &c1)?;
        assert_eq!(CellPageData::len(&page), 2);

        assert_eq!(CellPageData::previous_sibling(&page, &c1), Some(c2));

        assert_eq!(CellPageData::iter(&page).collect::<Vec<_>>(), vec![c2, c1]);
        Ok(())
    }
}