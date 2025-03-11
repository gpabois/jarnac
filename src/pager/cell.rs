//! Système de répartition par cellules de taille constante du contenu d'une page.
//!
//! Permet de :
//! - découper l'espace en liste chaînée réordonnable sans réaliser de déplacements de blocs de données
//! - allouer/libérer des cellules 
//! 
//! # Exigence
//! Pour que cela marche :(*cid) * self.cell_size
//! - l'entête de la page doit contenir, après le nombre magique ([crate::pager::page::PageKind]), [CellPageHeader]
//! - l'entête de la cellule doit contenir en premier lieu [CellHeader].
//! 
//! [CellPage] est utilisé pour piloter les cellules, notamment via :
//! - [CellPageHeader::push], et ses variantes [CellPageHeader::insert_after] ou [CellPageHeader::insert_before]
//! - [CellPageHeader::iter]
//! 
//! # Layout d'une page à cellules
//! 
//! | PageKind          | 1 byte    |
//! | CellPageHeader    | 9 bytes   |
//! |           ............        | - Espace réservée pour les systèmes employant le découpag en cellules
//! |-------------------|-----------| < base ^
//! | CellHeader        | 2 bytes   |        |- cell size  (x capacity)
//! |...............................|        v
//! |-------------------|-----------|
use std::{num::NonZeroU8, ops::{AddAssign, Div, Mul, Range, SubAssign}};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{error::{PagerError, PagerErrorKind}, page::{AsMutPageSlice, AsRefPageSlice, PageId, PageSize, PageSlice}, PagerResult};
use crate::prelude::*;

pub struct CellPage<Page>(Page) where Page: AsRefPageSlice;

pub const HEADER_SLICE_RANGE: Range<usize> = 1..(size_of::<CellPageHeader>() + 1);

impl<Page> From<Page> for CellPage<Page> where Page: AsRefPageSlice {
    fn from(value: Page) -> Self {
        Self(value)
    }
}
impl<Page> AsRef<Page> for CellPage<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &Page {
        &self.0
    }
}
impl<Page> AsMut<Page> for CellPage<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut Page {
        &mut self.0
    }
}
impl<Page> AsRef<CellPageHeader> for CellPage<Page> where Page: AsRefPageSlice {
    fn as_ref(&self) -> &CellPageHeader {
        CellPageHeader::ref_from_bytes(&self.0.as_ref()[HEADER_SLICE_RANGE]).unwrap()
    }
}
impl<Page> AsMut<CellPageHeader> for CellPage<Page> where Page: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut CellPageHeader {
        CellPageHeader::mut_from_bytes(&mut self.0.as_mut()[HEADER_SLICE_RANGE]).unwrap()
    }
}
impl<Page> CellPage<Page> where Page: AsRefPageSlice {
    /// Récupère la page après coups.
    pub fn into_inner(self) -> Page {
        self.0
    }

    /// Itère sur les références des cellules de la page
    pub fn iter(&self) -> RefPageCellCursor<'_, Page> 
    {
        let current = self.as_header().head_cell.into();
        RefPageCellCursor { page: self, current }        
    }

    /// Emprunte une cellule en lecture seule
    pub fn borrow_cell<'a>(&'a self, cid: &CellId) -> Option<&'a Cell<PageSlice>>
    {
        let idx = self.get_cell_range(cid)?;
        let slice = self.0.borrow_page_slice(idx);

        unsafe {
            Some(std::mem::transmute(slice))
        }
    }

    /// Récupère un intervalle permettant de cibler une cellule.
    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> 
    {
        self.as_header().get_cell_range(cid)
    }

    /// Retourne la prochaine cellule
    pub fn next_sibling(&self, cid: &CellId) -> Option<CellId> {
        let cell = self.borrow_cell(cid)?;
        cell.next_sibling().clone()
    }

    /// Retourne la cellule précédente
    pub fn previous_sibling(&self, cid: &CellId) -> Option<CellId> {
        let cell = self.borrow_cell(cid)?;
        cell.prev_sibling().clone()
    }

    pub fn len(&self) -> CellCapacity {
        self.as_header().len()
    }

    pub fn is_full(&self) -> bool {
        self.as_header().is_full()
    }

    fn as_header(&self) -> &CellPageHeader {
        self.as_ref()
    }
}
impl<Page> CellPage<Page> where Page: AsMutPageSlice {
    /// Vérifie que la taille allouée aux cellules est contenue au sein de la page.
    fn assert_no_overflow(page_size: PageSize, cell_size: PageSize, base: PageSize, capacity: CellCapacity) -> PagerResult<()> {
        let space_size = page_size - base;
        
        if space_size < cell_size * capacity {
            return Err(PagerError::new(PagerErrorKind::CellPageOverflow))
        }

        Ok(())
    }

    /// Initialise les éléments nécessaires pour découper la page en cellules.
    pub fn new(page: Page, cell_size: PageSize, capacity: CellCapacity, base: PageSize) -> PagerResult<Self>
    {
        Self::assert_no_overflow(page.as_ref().len(), cell_size, base, capacity)?;

        let mut cells = Self::from(page);
        *cells.as_mut_header() = CellPageHeader::new(
            cell_size, 
            capacity,
            base
        );

        Ok(cells)
    }

    /// Itère sur les références des cellules de la page
    pub fn iter_mut(&mut self) -> MutPageCellCursor<'_, Page> 
    {
        let current = self.as_header().get_head();
        MutPageCellCursor { page: self, current }        
    }

    /// Emprunte une cellule en écriture
    pub fn borrow_mut_cell<'a>(&'a mut self, cid: &CellId) -> Option<&'a mut Cell<PageSlice>> {
        let idx = self.get_cell_range(cid)?;

        unsafe {
            std::mem::transmute(self.0.borrow_mut_page_slice(idx))
        }
    }

    /// Divise les cellules à l'endroit choisi. 
    pub fn split_at_into<P2>(&mut self, dest: &mut CellPage<P2>, at: u8) -> PagerResult<()> where P2: AsMutPageSlice {
        let mut to_free: Vec<CellId> = vec![];

        self
        .iter()
        .skip(at.into())
        .try_for_each::<_, PagerResult<()>>(|src_cell| {
            let cid = dest.push()?;
            let mut dest_cell = dest.borrow_mut_cell(&cid).unwrap();

            src_cell.copy_into(&mut dest_cell);

            to_free.push(cid);
            
            Ok(())            
        })?;

        to_free
        .into_iter()
        .for_each(|cid| {
            self.free_cell(&cid);
        });

        Ok(())
    }   

    /// Insère une nouvelle cellule à la fin de la liste chaînée.
    pub fn push(&mut self) -> PagerResult<CellId> 
    {
        let cid = self.alloc_cell()?;
        let maybe_tail_cid = self.iter().map(|cell| cell.id()).last().copied();

        if let Some(tail_cid) = maybe_tail_cid {
            self.borrow_mut_cell(&tail_cid).unwrap().as_mut_header().next = Some(cid).into();
            self.borrow_mut_cell(&cid).unwrap().as_mut_header().prev = Some(cid).into();
        } else {
            self.as_mut_header().set_head(Some(cid));
        }

        Ok(cid)
    }

    /// Insère une nouvelle cellule après une autre.
    pub fn insert_after(&mut self, after: &CellId) -> PagerResult<CellId> {
        let cid = self.alloc_cell()?;

        // La prochaine cellule après la cellule à insérer
        let maybe_next: Option<CellId> = {
            let cell = self.borrow_mut_cell(&after).unwrap();
            let next = cell.as_header().next;
            cell.as_mut_header().next = Some(cid).into();
            next.into()      
        };

        match maybe_next {
            Some(next) => {
                let next_next =self.borrow_mut_cell( &next).unwrap();
                next_next.as_mut_header().prev = Some(cid).into();
                
                let cell = self.borrow_mut_cell(&cid).unwrap();
                cell.as_mut_header().prev = Some(*after).into();
                cell.as_mut_header().next = Some(next).into();
            },
            None => {
                self.as_mut_header().set_head(Some(cid));
                self.borrow_mut_cell(&cid).unwrap().as_mut_header().prev = Some(*after).into();
            },
        };

        Ok(cid)
    }

    /// Insère une nouvelle cellule avant une autre.
    pub fn insert_before(&mut self, before: &CellId) -> PagerResult<CellId> 
    {
        let cid = self.alloc_cell()?;

        let maybe_prev: Option<CellId> = {
            let cell = self.borrow_mut_cell(&before).unwrap();
            let prev = cell.as_header().prev;
            cell.as_mut_header().prev = Some(cid).into();
            prev.into()
        };

        match maybe_prev {
            None => {
                self.as_mut_header().set_head(Some(cid));
                self.borrow_mut_cell(&cid).unwrap().as_mut_header().next = Some(*before).into();
            },
            Some(prev) => {
                self.borrow_mut_cell( &prev).unwrap().as_mut_header().next = Some(cid).into();
                let cell = self.borrow_mut_cell(&cid).unwrap();
                cell.as_mut_header().prev = Some(prev).into();
                cell.as_mut_header().next = Some(*before).into();
            }
        }

        Ok(cid)
    }

    /// Alloue une nouvelle cellule au sein de la page, si on en a assez.
    fn alloc_cell(&mut self) -> PagerResult<CellId> where Page: AsMutPageSlice {
        if self.as_header().is_full() {
            return Err(PagerError::new(PagerErrorKind::CellPageFull));
        }

        let cid = self.pop_free_cell().unwrap_or_else(|| {
            self.as_mut_header().inc_len().into()
        });
        
        let cell = self.borrow_mut_cell(&cid).unwrap();
        *cell.as_mut_header() = CellHeader {
            id: cid,
            next: None.into(),
            prev: None.into()
        };

        Ok(cid)
    }

    fn set_previous_sibling(&mut self, cid: &CellId, previous: Option<CellId>) 
    {
        let cell = self.borrow_mut_cell(cid).unwrap();
        cell.as_mut_header().prev = previous.into();
    }  

    #[allow(dead_code)]
    fn set_next_sibling(&mut self, cid: &CellId, previous: Option<CellId>) 
    {
        let cell = self.borrow_mut_cell(cid).unwrap();
        cell.as_mut_header().prev = previous.into();
    }  

    fn free_cell(&mut self, cid: &CellId) {
        self.detach_cell(cid);
        self.push_free_cell(cid);
    }

    /// Insère une nouvelle cellule dans la liste des cellules libres.
    fn push_free_cell(&mut self, cid: &CellId) 
        where Page: AsMutPageSlice
    {
        self.as_header().get_free_head().inspect(|head| {
            self.borrow_mut_cell(&head).unwrap().set_prev_sibling(Some(*cid));
        });
        
        self.as_mut_header().set_free_head(Some(*cid));
        self.as_mut_header().inc_free_len();
    }

    /// Retire une cellule de la liste des cellules libres.
    fn pop_free_cell(&mut self) -> Option<CellId> 
    {
        if let Some(head) = self.as_header().get_free_head() {
            let maybe_next = self.next_sibling(&head);
            
            maybe_next.inspect(|next| {
                self.set_previous_sibling(&next, None);
            });
            
            self.as_mut_header().set_free_head(maybe_next);
            self.as_mut_header().dec_free_len();

            return Some(head)
        }

        return None
    }

    #[allow(dead_code)]
    /// Retire la cellule de la liste chaînée
    fn detach_cell(&mut self, cid: &CellId) 
        where Page: AsMutPageSlice
    {
        let prev = self.previous_sibling(cid);
        let next = self.next_sibling(cid);

        self.borrow_mut_cell(cid).unwrap().detach();

        prev.inspect(|cid| {
            self.borrow_mut_cell(cid).unwrap().set_next_sibling(next)
        });

        next.inspect(|cid| {
            self.borrow_mut_cell(cid).unwrap().set_prev_sibling(prev)
        });
    }  

    fn as_mut_header(&mut self) -> &mut CellPageHeader {
        self.as_mut()
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête de la page contenant les informations relatives aux cellules qui y sont stockées.
pub struct CellPageHeader {
    /// Taille d'une cellule
    cell_size: PageSize,
    /// Nombre de cellules alloués
    len: CellCapacity,
    /// Nombre maximal de cellules stockables.
    capacity: CellCapacity,
    /// Nombre de cellules libres
    free_len: CellCapacity,
    /// Tête de la liste des cellules libérées
    free_head_cell: OptionalCellId,
    /// Tête de a liste des cellules allouées
    head_cell: OptionalCellId,
    /// Localisation de la base des cellules
    cell_base: PageSize
}

impl CellPageHeader {
    pub fn new(cell_size: PageSize, capacity: CellCapacity, base: PageSize) -> Self {
        return Self {
            cell_size,
            capacity,
            len: 0.into(),
            free_len: 0.into(),
            free_head_cell: None.into(),
            head_cell: None.into(),
            cell_base: base
        }
    }

    pub fn get_cell_location(&self, cid: &CellId) -> PageSize {
       self.cell_size * (*cid) + self.cell_base
    }

    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        let loc: usize = self.get_cell_location(cid).into();
        let size: usize = self.cell_size.into();
        Some(loc..(loc + size))
    }

    pub fn get_head(&self) -> Option<CellId> {
        self.head_cell.into()
    }

    pub fn set_head(&mut self, head: Option<CellId>) {
        self.head_cell = head.into();
    }

    fn inc_len(&mut self) -> CellCapacity
    {
        self.len += 1;
        self.len
    }

    #[allow(dead_code)]
    fn dec_len<Page>(&mut self) -> CellCapacity
    {
        self.len -= 1;
        self.len
    }

    fn dec_free_len(&mut self) 
    {
        self.free_len -= 1;
    }

    fn inc_free_len(&mut self) 
    {
        self.free_len += 1;
    }

    fn set_free_head(&mut self, head: Option<CellId>) 
    {
        self.free_head_cell = head.into();
    }

    fn get_free_head(&self) -> Option<CellId> 
    {
        self.free_head_cell.into()
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.capacity && self.free_len == 0.into()
    }

    pub fn len(&self) -> CellCapacity {
        self.len
    }
}


/// Une référence vers une cellule de page.
pub struct Cell<Slice>(Slice) where Slice: AsRefPageSlice + ?Sized;

impl<Slice> AsRef<CellHeader> for Cell<Slice> where Slice: AsRefPageSlice + ?Sized {
    fn as_ref(&self) -> &CellHeader {
        CellHeader::ref_from_bytes(&self.0.as_ref()[0..size_of::<CellHeader>()]).unwrap()
    }
}

impl<Slice> AsMut<CellHeader> for Cell<Slice> where Slice: AsMutPageSlice + ?Sized {
    fn as_mut(&mut self) -> &mut CellHeader {
        CellHeader::mut_from_bytes(&mut self.0.as_mut()[0..size_of::<CellHeader>()]).unwrap()
    }
}

impl<Slice> Cell<Slice> where Slice: AsRefPageSlice + ?Sized {
    pub fn as_slice(&self) -> &PageSlice {
        self.0.as_ref()
    }
}

impl<Slice> Cell<Slice> where Slice: AsMutPageSlice + ?Sized {
    pub fn as_mut_slice(&mut self) -> &mut PageSlice {
        self.0.as_mut()
    }
}

impl<Slice> Cell<Slice> where Slice: AsRefPageSlice + ?Sized {
    /// Retourne l'identifiant de la cellule.
    pub fn id(&self) -> &CellId {
        &self.as_header().id
    }

    pub fn borrow_content(&self) -> &PageSlice {
        &self.as_slice()[size_of::<CellHeader>()..]
    }

    /// Copie le contenu de la cellule dans une autre cellule.
    pub fn copy_into<S2>(&self, dest: &mut Cell<S2>) where S2: AsMutPageSlice + ?Sized {
        dest.borrow_mut_content().copy_from_slice(self.borrow_content());
    }

    pub fn next_sibling(&self) -> &Option<CellId> {
        self.as_header().next.as_ref()
    }

    pub fn prev_sibling(&self) -> &Option<CellId> {
        self.as_header().prev.as_ref()
    }

    fn as_header(&self) -> &CellHeader {
        self.as_ref()
    }
}

impl<Slice> Cell<Slice> where Slice: AsMutPageSlice + ?Sized {
    /// Détache la cellule de sa liste chaînée.
    fn detach(&mut self) {
        let header = self.as_mut_header();
        header.next = None.into();
        header.prev = None.into();
    }

    /// Définit le prochain voisin de la cellule.
    fn set_next_sibling(&mut self, next: Option<CellId>) {
        self.as_mut_header().next = next.into();
    }

    /// Définit le voisin précédent de la cellule.
    fn set_prev_sibling(&mut self, prev: Option<CellId>) {
        self.as_mut_header().prev = prev.into();      
    }

    pub fn borrow_mut_content(&mut self) -> &mut PageSlice {
        &mut self.as_mut_slice()[size_of::<CellHeader>()..]
    }

    fn as_mut_header(&mut self) -> &mut CellHeader {
        self.as_mut()
    }
}

pub struct RefPageCellCursor<'a, Page>
where Page: AsRefPageSlice
{
    page: &'a CellPage<Page>,
    current: Option<CellId>
}

impl<'a, Page> Iterator for RefPageCellCursor<'a, Page> 
where Page: AsRefPageSlice
{
    type Item = &'a Cell<PageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.page.next_sibling(&cid);
                self.page.borrow_cell(&cid)
            },
            None => None
        }
    }
}

pub struct MutPageCellCursor<'a, Page>
where Page: AsMutPageSlice
{
    page: &'a mut CellPage<Page>,
    current: Option<CellId>
}

impl<'a, Page> Iterator for MutPageCellCursor<'a, Page> 
where Page: AsMutPageSlice
{
    type Item = Cell<&'a mut PageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.page.next_sibling(&cid);
                unsafe {
                    std::mem::transmute(self.page.borrow_cell(&cid))
                }
            },
            None => None
        }
    }
}

/// Un curseur sur les cellules d'une page.
pub struct PageCellIdCursor<'a, Page> 
where Page: AsRefPageSlice
{
    page: &'a CellPage<Page>,
    current: Option<CellId>
}

impl<Page> Iterator for PageCellIdCursor<'_, Page> 
where Page: AsRefPageSlice
{
    type Item = CellId;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.page.next_sibling(&cid);
                Some(cid)
            },
            None => None
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête d'une cellule.
pub struct CellHeader {
    id:   CellId,
    prev: OptionalCellId,
    next: OptionalCellId,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct GlobalCellId(PageId, CellId);

impl GlobalCellId {
    pub(crate) fn new(pid: PageId, cid: CellId) -> Self {
        Self(pid, cid)
    }

    pub fn pid(&self) -> &PageId {
        &self.0
    }

    pub fn cid(&self) -> &CellId {
        &self.1
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, FromBytes, IntoBytes, Immutable, KnownLayout)]
pub struct CellId(u8);

impl From<CellCapacity> for CellId {
    fn from(value: CellCapacity) -> Self {
        Self(value.0.try_into().unwrap())
    }
}

impl Into<u16> for CellId {
    fn into(self) -> u16 {
        u16::from(self.0)
    }
}


#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Debug)]
pub struct CellCapacity(u8);

impl Div<u8> for CellCapacity {
    type Output = u8;

    fn div(self, rhs: u8) -> Self::Output {
        self.0.div(rhs)
    }
}

impl AddAssign<u8> for CellCapacity {
    fn add_assign(&mut self, rhs: u8) {
        self.0 += rhs
    }
}

impl SubAssign<u8> for CellCapacity {
    fn sub_assign(&mut self, rhs: u8) {
        self.0 -= rhs
    }
}

impl Into<u16> for CellCapacity {
    fn into(self) -> u16 {
        u16::from(self.0)
    }
}

impl From<u8> for CellCapacity {
    fn from(value: u8) -> Self {
        Self(value)
    }
}


#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptionalCellId(Option<NonZeroU8>);

impl Into<Option<CellId>> for OptionalCellId {
    fn into(self) -> Option<CellId> {
        self.0.map(|v| CellId(v.get()))
    }
}

impl From<Option<CellId>> for OptionalCellId {
    fn from(value: Option<CellId>) -> Self {
        Self(value.map(|n| n.0.try_into().unwrap()))
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
        u32::from(self.0) * rhs
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::pager::{cell::{CellCapacity, CellPage}, fixtures::fixture_new_pager, page::PageSize};
    use super::CellHeader;

    #[test]
    fn basic_cells_tests() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();
        let page = pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?;

        let mut cells = CellPage::new(
            page, 
            PageSize::from(10u16), 
            4.into(), 
            size_of::<CellHeader>().try_into().unwrap()
        )?;

        assert_eq!(cells.len(), CellCapacity::from(0));

        let c1 = cells.push()?;
        assert_eq!(cells.len(), CellCapacity::from(1));

        let c2 = cells.insert_before(&c1)?;
        assert_eq!(cells.len(), CellCapacity::from(2));

        
        assert_eq!(cells.previous_sibling(&c1), Some(c2));
        assert_eq!(cells.iter().map(|cell| cell.id()).copied().collect::<Vec<_>>(), vec![c2, c1]);

        Ok(())
    }

    #[test]
    fn test_fails_when_overflow() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();
        let page = pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?;

        let page_size = pager.page_size();
        let base: PageSize = size_of::<CellHeader>().into();
        let available_cells_space_size = page_size - base;

        let result = CellPage::new(page,             
            available_cells_space_size, 
            4.into(), 
            size_of::<CellHeader>().into()
        );

        assert!(result.is_err(), "on ne devrait pas pouvoir créer une page cellulaire dont l'espace requis excède l'espace disponible");

        Ok(())
    }

    #[test]
    fn test_fails_when_full() -> Result<(), Box<dyn Error>>  {
        let pager = fixture_new_pager();
        let page = pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?;

        let mut cells = CellPage::new(
            page, 
            PageSize::from(10u16), 
            4.into(), 
            size_of::<CellHeader>().into()
        )?;

        for _ in 0..4 {
            cells.push().expect("on devrait pouvoir encore ajouter une cellule");
        }

        let result = cells.push();
        assert!(result.is_err(), "on ne devrait pas pouvoir ajouter une nouvelle cellule s'il n'y a plus d'espace.");

        Ok(())
    }
}