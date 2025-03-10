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
use std::{num::NonZeroU8, ops::{Add, Deref, DerefMut, Mul, Range}};

use zerocopy::{FromBytes, TryFromBytes};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{error::PagerError, page::{AsMutPageSlice, AsRefPageSlice, PageId, PageSlice}, PagerResult};
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

    /// Itère sur les identifiants des cellules de la page
    pub fn iter_ids(&self) -> PageCellIdCursor<'_, Page> 
    {
        let current = self.as_header().head_cell.into();
        PageCellIdCursor { page: self, current }
    }

    /// Emprunte une cellule en lecture seule
    pub fn borrow_cell<'a>(&'a self, cid: &CellId) -> Option<Cell<&'a PageSlice>>
    where Page: AsRefPageSlice
    {
        let idx = self.get_cell_range(cid)?;

        Some(Cell {
            cid: *cid,
            bytes: self.0.borrow_page_slice(idx)
        })
    }

    /// Récupère un intervalle permettant de cibler une cellule.
    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> 
    {
        self.as_header().get_cell_range(cid)
    }

    /// Retourne la prochaine cellule
    pub fn next_sibling(&self, cid: &CellId) -> Option<CellId> {
        let cell = self.borrow_cell(cid)?;
        cell.header.next.into()
    }

    /// Retourne la cellule précédente
    pub fn previous_sibling(&self, cid: &CellId) -> Option<CellId> {
        let cell = self.borrow_cell(cid)?;
        cell.header.prev.into()
    }

    pub fn len(&self) -> u8 {
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
    /// Initialise les éléments nécessaires pour découper la page en cellules.
    pub fn new(page: Page, cell_size: CellSize, capacity: u8, base: u16) -> Self
    {
        let mut cp = Self::from(page);
        *cp.as_mut_header() = CellPageHeader::new(
            cell_size, 
            capacity,
            base
        );
        cp
    }

    /// Itère sur les références des cellules de la page
    pub fn iter_mut(&mut self) -> MutPageCellCursor<'_, Page> 
    {
        let current = self.as_header().get_head();
        MutPageCellCursor { page: self, current }        
    }

    /// Emprunte une cellule en écriture
    pub fn borrow_mut_cell<'a>(&'a mut self, cid: &CellId) -> Option<Cell<&'a mut PageSlice>> {
        let idx = self.get_cell_range(cid)?;

        Some(Cell {
            cid: *cid,
            bytes: self.0.borrow_mut_page_slice(idx)
        })
    }

    /// Divise les cellules à l'endroit choisi. 
    pub fn split_at_into<P2>(&mut self, dest: &mut CellPage<P2>, at: u8) -> PagerResult<()> where P2: AsMutPageSlice {
        self.iter()
        .skip(at.into())
        .try_for_each::<_, PagerResult<()>>(|src_cell| {
            let cid = dest.push()?;
            let mut dest_cell = dest.borrow_mut_cell(&cid).unwrap();

            src_cell.copy_into(&mut dest_cell);
            
            Ok(())            
        })?;

        let to_free = self.iter_ids().collect::<Vec<_>>();

        to_free.into_iter().for_each(|cid| {
            self.free_cell(&cid);
        });

        Ok(())
    }   

    /// Insère une nouvelle cellule à la fin de la liste chaînée.
    pub fn push(&mut self) -> PagerResult<CellId> 
    {
        let cid = self.alloc_page_cell()?;
        let maybe_tail_cid = self.iter_ids().last();

        if let Some(tail_cid) = maybe_tail_cid {
            self.borrow_mut_cell(&tail_cid).unwrap().header.next = Some(cid).into();
            self.borrow_mut_cell(&cid).unwrap().header.prev = Some(cid).into();
        } else {
            self.as_mut_header().set_head(Some(cid));
        }

        Ok(cid)
    }

    /// Insère une nouvelle cellule après une autre.
    pub fn insert_after(&mut self, after: &CellId) -> PagerResult<CellId> {
        let cid = self.alloc_page_cell()?;

        // La prochaine cellule après la cellule à insérer
        let maybe_next: Option<CellId> = {
            let mut cell = self.borrow_mut_cell(&after).unwrap();
            let next = cell.header.next;
            cell.header.next = Some(cid).into();
            next.into()      
        };

        match maybe_next {
            Some(next) => {
                let mut next_next =self.borrow_mut_cell( &next).unwrap();
                next_next.header.prev = Some(cid).into();
                
                let mut cell = self.borrow_mut_cell(&cid).unwrap();
                cell.header.prev = Some(*after).into();
                cell.header.next = Some(next).into();
            },
            None => {
                self.as_mut_header().set_head(Some(cid));
                self.borrow_mut_cell(&cid).unwrap().header.prev = Some(*after).into();
            },
        };

        Ok(cid)
    }

    /// Insère une nouvelle cellule avant une autre.
    pub fn insert_before(&mut self, before: &CellId) -> PagerResult<CellId> 
    {
        let cid = self.alloc_page_cell()?;

        let maybe_prev: Option<CellId> = {
            let mut cell = self.borrow_mut_cell(&before).unwrap();
            let prev = cell.header.prev;
            cell.header.prev = Some(cid).into();
            prev.into()
        };

        match maybe_prev {
            None => {
                self.as_mut_header().set_head(Some(cid));
                self.borrow_mut_cell(&cid).unwrap().header.next = Some(*before).into();
            },
            Some(prev) => {
                self.borrow_mut_cell( &prev).unwrap().header.next = Some(cid).into();
                let mut cell = self.borrow_mut_cell(&cid).unwrap();
                cell.header.prev = Some(prev).into();
                cell.header.next = Some(*before).into();
            }
        }

        Ok(cid)
    }

    /// Alloue une nouvelle cellule au sein de la page, si on en a assez.
    fn alloc_page_cell(&mut self) -> PagerResult<CellId> where Page: AsMutPageSlice {
        if self.as_header().is_full() {
            return Err(PagerError::new(super::error::PagerErrorKind::CellPageFull));
        }

        let cid = self.pop_free_cell().unwrap_or_else(|| {
            let cid_u8 = self.as_mut_header().inc_len();
            CellId(NonZeroU8::new(cid_u8).unwrap())
        });
        
        let mut cell = self.borrow_mut_cell(&cid).unwrap();
        cell.header = CellHeader::default();
        cell.data.fill(0);

        Ok(cid)
    }

    fn set_previous_sibling(&mut self, cid: &CellId, previous: Option<CellId>) 
    {
        let mut cell = self.borrow_mut_cell(cid).unwrap();
        cell.header.prev = previous.into();
    }  

    #[allow(dead_code)]
    fn set_next_sibling(&mut self, cid: &CellId, previous: Option<CellId>) 
    {
        let mut cell = self.borrow_mut_cell(cid).unwrap();
        cell.header.prev = previous.into();
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

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
pub struct CellPageData {
    kind: u8,
    header: CellPageHeader,
    body: [u8]
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
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

    pub fn get_cell_location(&self, cid: &CellId) -> CellLocation {
        (*cid) * self.cell_size + self.cell_base
    }

    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        Some(self.get_cell_location(&cid).into_range(self.cell_size))
    }

    pub fn get_head(&self) -> Option<CellId> {
        self.head_cell.into()
    }

    pub fn set_head(&mut self, head: Option<CellId>) {
        self.head_cell = head.into();
    }

    fn inc_len(&mut self) -> u8 
    {
        self.len += 1;
        self.len
    }

    #[allow(dead_code)]
    fn dec_len<Page>(&mut self) -> u8 
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
        self.len - self.free_len >= self.capacity
    }

    pub fn len(&self) -> u8 {
        self.len
    }
}

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
/// Représente une cellule 
pub struct CellData {
    header: CellHeader,
    data: [u8]
}

/// Une référence vers une cellule de page.
pub struct Cell<Slice> where Slice: AsRefPageSlice
{
    #[allow(dead_code)]
    pub(crate) cid: CellId,
    pub(crate) bytes: Slice
}

impl<Slice> Cell<Slice> where Slice: AsRefPageSlice {
    pub fn as_slice(&self) -> &PageSlice {
        self.as_ref()
    }
}

impl<Slice> Cell<Slice> where Slice: AsMutPageSlice {
    pub fn as_mut_slice(&mut self) -> &mut PageSlice {
        self.as_mut()
    }
}

impl<Slice> Cell<Slice> where Slice: AsRefPageSlice {
    /// Retourne l'identifiant de la cellule.
    pub fn id(&self) -> &CellId {
        &self.cid
    }

    pub fn borrow_content(&self) -> &PageSlice {
        &self.as_slice()[size_of::<CellHeader>()..]
    }

    /// Copie le contenu de la cellule dans une autre cellule.
    pub fn copy_into<S2>(&self, dest: &mut Cell<S2>) where S2: AsMutPageSlice {
        let src_data: &CellData = self.as_ref();
        let dest_data: &mut CellData = dest.as_mut();

        dest_data.data.copy_from_slice(&src_data.data);
    }

    pub fn next_sibling(&self) -> &Option<CellId> {
        let data: &CellData = self.as_ref();
        data.header.next.as_ref()
    }

    pub fn prev_sibling(&self) -> &Option<CellId> {
        let data: &CellData = self.as_ref();
        data.header.prev.as_ref()
    }
}

impl<Slice> Cell<Slice> where Slice: AsMutPageSlice {
    /// Détache la cellule de sa liste chaînée.
    fn detach(&mut self) {
        let data: &mut CellData = self.as_mut();
        data.header.next = None.into();
        data.header.prev = None.into();
    }

    /// Définit le prochain voisin de la cellule.
    fn set_next_sibling(&mut self, next: Option<CellId>) {
        let data: &mut CellData = self.as_mut();
        data.header.next = next.into();
    }

    /// Définit le voisin précédent de la cellule.
    fn set_prev_sibling(&mut self, prev: Option<CellId>) {
        let data: &mut CellData = self.as_mut();
        data.header.prev = prev.into();      
    }

    pub fn borrow_mut_content(&mut self) -> &mut PageSlice {
        &mut self.as_mut_slice()[size_of::<CellHeader>()..]
    }
}

impl<Slice> Deref for Cell<Slice> where Slice: AsRefPageSlice {
    type Target = CellData;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<Slice> DerefMut for Cell<Slice> where Slice: AsMutPageSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}

impl<Slice> AsRef<PageSlice> for Cell<Slice> where Slice: AsRefPageSlice {
    fn as_ref(&self) -> &PageSlice {
        self.bytes.as_ref()
    }
}

impl<Slice> AsRef<CellData> for Cell<Slice> where Slice: AsRefPageSlice {
    fn as_ref(&self) -> &CellData {
        CellData::try_ref_from_bytes(AsRef::<PageSlice>::as_ref(self)).unwrap()
    }
}

impl<Slice> AsMut<PageSlice> for Cell<Slice> where Slice: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut PageSlice {
        self.bytes.as_mut()
    }
}

impl<Slice> AsMut<CellData> for Cell<Slice> where Slice: AsMutPageSlice {
    fn as_mut(&mut self) -> &mut CellData {
        CellData::try_mut_from_bytes(AsMut::<PageSlice>::as_mut(self)).unwrap()
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
    type Item = Cell<&'a PageSlice>;

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

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Default)]
#[repr(C, packed)]
/// En-tête d'une cellule.
pub struct CellHeader {
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
    pub fn into_range(self, size: CellSize) -> Range<usize> {
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

    use zerocopy::FromBytes;
    use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

    use crate::{fs::in_memory::InMemoryFs, pager::{cell::CellPage, page::PageSize, Pager, PagerOptions}};

    use super::{CellHeader, CellPageHeader, CellSize};


    #[derive(FromBytes, KnownLayout, Immutable, IntoBytes)]
    #[repr(packed)]
    struct Foo {
        header: CellHeader,
        value: u64
    }

    #[test]
    fn test_cells() -> Result<(), Box<dyn Error>> {
        let fs = Rc::new(InMemoryFs::default());
        let pager = Pager::new(fs, "memory", PageSize::new(4_096), PagerOptions::default())?.into_boxed();


        let pid = pager.new_page()?;
        let mut page =  pager.borrow_mut_page(&pid)?;
        
        let mut cpage = CellPage::new(
            &mut page, 
            CellSize::from(10), 
            4, 
            u16::try_from(size_of::<CellHeader>()).unwrap()
        );

        assert_eq!(cpage.len(), 0);

        let c1 = cpage.push()?;
        assert_eq!(cpage.len(), 1);

        let c2 = cpage.insert_before(&c1)?;
        assert_eq!(cpage.len(), 2);

        
        assert_eq!(cpage.previous_sibling(&c1), Some(c2));
        assert_eq!(cpage.iter_ids().collect::<Vec<_>>(), vec![c2, c1]);

        let mut page = cpage.borrow_mut_cell(&c1).unwrap();
        let foo = Foo::mut_from_bytes(page.as_mut_slice()).unwrap();
        

        Ok(())
    }
}