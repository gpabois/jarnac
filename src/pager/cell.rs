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
//! |           ............        | - Espace réservée pour les systèmes employant le découpage en cellules
//! |-------------------|-----------| < base ^
//! | CellHeader        | 2 bytes   |        |- cell size  (x capacity)
//! |...............................|        v
//! |-------------------|-----------|
use std::{fmt::Display, mem::MaybeUninit, num::NonZeroU8, ops::{AddAssign, Div, Index, IndexMut, Mul, Range, Sub, SubAssign}};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{error::{PagerError, PagerErrorKind}, page::{AsMutPageSlice, AsRefPageSlice, IntoRefPageSlice, MutPage, PageId, PageSize, PageSlice, RefPage}, PagerResult};
use crate::prelude::*;

/// Sous-sytème permettant de découper une page en cellules de tailles égales
pub struct CellPage<Page>(Page) where Page: AsRefPageSlice + ?Sized;

pub const HEADER_SLICE_RANGE: Range<usize> = 1..(size_of::<CellPageHeader>() + 1);

impl<'pager> CellPage<MutPage<'pager>> {
    pub fn into_ref(self) -> CellPage<RefPage<'pager>> {
        CellPage(self.0.into_ref())
    }
}

impl<Page> Clone for CellPage<Page> where Page: AsRefPageSlice + Clone {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

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

impl<Page> CellPage<Page> where Page: IntoRefPageSlice + Clone + AsRefPageSlice {
    pub fn into_iter(self) -> OwnedRefPageCellCursor<Page> {
        let current = self.head().clone();
        let cells = self;
        OwnedRefPageCellCursor { cells, current }
    }
}

impl<Page> CellPage<Page> where Page: AsRefPageSlice {
    /// Récupère la page après coups.
    pub fn into_inner(self) -> Page {
        self.0
    }

    /// Itère sur les références des cellules de la page
    pub fn iter(&self) -> RefPageCellCursor<'_, Page> {
        RefPageCellCursor { cells: self, current: self.head().clone() }        
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
    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        self
            .as_header()
            .get_cell_range(cid)
    }

    /// Retourne la prochaine cellule
    pub fn next_sibling(&self, cid: &CellId) -> Option<CellId> {
        self[cid].next_sibling()
    }

    /// Retourne la cellule précédente
    pub fn previous_sibling(&self, cid: &CellId) -> Option<CellId> {
        self[cid].prev_sibling()
    }

    pub fn len(&self) -> CellCapacity {
        self.as_header().len()
    }

    pub fn capacity(&self) -> CellCapacity {
        self.as_header().capacity()
    }

    pub fn is_full(&self) -> bool {
        self.as_header().is_full()
    }

    fn head(&self) -> Option<CellId> {
        self.as_header().get_head()
    }

    fn tail(&self) -> Option<CellId> {
        self.as_header().get_tail()
    }

    fn as_header(&self) -> &CellPageHeader {
        self.as_ref()
    }
}

impl<Page> Index<&CellId> for CellPage<Page> where Page: AsRefPageSlice {
    type Output = Cell<PageSlice>;

    fn index(&self, index: &CellId) -> &Self::Output {
        self.borrow_cell(index).expect(&format!("the cell {index} does not exist"))
    }
}

impl<Page> IndexMut<&CellId> for CellPage<Page> where Page: AsMutPageSlice {
    fn index_mut(&mut self, index: &CellId) -> &mut Self::Output {
        self.borrow_mut_cell(index).unwrap()
    }
}

impl<Page> CellPage<Page> where Page: AsMutPageSlice {
    /// Initialise les éléments nécessaires pour découper la page en cellules.
    /// 
    /// La fonction échoue si l'espace nécessaire pour stocker toutes les cellules excèdent la taille de l'espace libre
    /// allouée aux cellules.
    /// 
    /// - content_size: The size of the cell content, it is used to compute the cell size (= size_of::<CellHeader> + content_size)
    /// - capacity: The maximum number of cells the page can hold
    /// - reserved: The number of reserved bytes in the header, it is used to compute the cell space base (= reserved + size_of::<CellPageHeader>())
    pub fn new(page: Page, content_size: PageSize, capacity: CellCapacity, reserved: PageSize) -> PagerResult<Self>
    {
        let cell_size = PageSize::from(content_size + size_of::<CellHeader>());
        let base = PageSize::from(reserved + size_of::<CellPageHeader>() + 1);

        Self::assert_no_overflow(page.as_ref().len(), cell_size, base, capacity)?;

        let mut cells = Self::from(page);
        
        cells
            .as_mut_uninit_header()
            .write(CellPageHeader::new(
                cell_size, 
                capacity,
                base
            ));

        Ok(cells)
    }

    /// Itère sur les références des cellules de la page
    pub fn iter_mut(&mut self) -> MutPageCellCursor<'_, Page> {
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
            src_cell.copy_into(&mut dest[&cid]); 
            to_free.push(src_cell.id());
            Ok(())            
        })?;

        to_free.into_iter()
        .for_each(|cid| {
            self.free_cell(&cid);
        });

        Ok(())
    }   

    /// Insère une nouvelle cellule à la fin de la liste chaînée.
    pub fn push(&mut self) -> PagerResult<CellId> {
        let cid = self.alloc_cell()?;

        if let Some(tail) = &self.tail() {
            self.set_next_sibling(&tail, &cid);
        } else {
            self.set_head(Some(cid));
            self.set_tail(Some(cid));
        }

        Ok(cid)
    }

    /// Insère une nouvelle cellule après une autre.
    pub fn insert_after(&mut self, after: &CellId) -> PagerResult<CellId> {
        let cid = self.alloc_cell()?;
        self.set_next_sibling(after, &cid);
        Ok(cid)
    }

    /// Insère une nouvelle cellule avant une autre.
    pub fn insert_before(&mut self, before: &CellId) -> PagerResult<CellId> {
        let cid = self.alloc_cell()?;
        self.set_previous_sibling(before, &cid);
        Ok(cid)
    }

    /// Alloue une nouvelle cellule au sein de la page, si on en a assez.
    fn alloc_cell(&mut self) -> PagerResult<CellId> where Page: AsMutPageSlice {
        if self.is_full() {
            return Err(PagerError::new(PagerErrorKind::CellPageFull));
        }

        let cid = self.pop_free_cell()
        .unwrap_or_else(|| {
            self.as_mut_header()
                .inc_len()
                .into()
        });
        
        let cell = self.borrow_mut_cell(&cid).unwrap();
        
        cell
            .as_mut_uninit_header()
            .write(CellHeader {
            id: cid,
            next: None.into(),
            prev: None.into()
        });

        Ok(cid)
    }
    
    /// Vérifie que la taille allouée aux cellules est contenue au sein de la page.
    fn assert_no_overflow(page_size: PageSize, cell_size: PageSize, base: PageSize, capacity: CellCapacity) -> PagerResult<()> {
        let space_size = page_size - base;
        
        if space_size < cell_size * capacity {
            return Err(PagerError::new(PagerErrorKind::CellPageOverflow))
        }

        Ok(())
    }

    /// Définit le précédent d'une cellule.
    fn set_previous_sibling(&mut self, cid: &CellId, previous: &CellId) {
        if let Some(before) = &self[cid].prev_sibling() {
            self[before].set_next_sibling(Some(*previous));
        } else {
            self.set_head(Some(*previous));
        }

        self[cid].set_previous_sibling(Some(*previous));
        self[previous].set_next_sibling(Some(*cid));
    }  

    /// Définit le suivant d'une cellule.
    ///  
    /// [cid] -> next (-> after) ?
    fn set_next_sibling(&mut self, cid: &CellId, next: &CellId) {
        // la cellule actuelle est la queue de la liste.
        if Some(*cid) == self.tail() {
            self.set_tail(Some(*next));
        }

        if let Some(after) = &self.next_sibling(cid) {
            self[after].set_previous_sibling(Some(*next));
            self[next].set_next_sibling(Some(*after));
        }

        self[cid].set_next_sibling(Some(*next));
        self[next].set_previous_sibling(Some(*cid));
    }  

    fn free_cell(&mut self, cid: &CellId) {
        self.detach_cell(cid);
        self.push_free_cell(cid);
    }

    /// Insère une nouvelle cellule dans la liste des cellules libres.
    fn push_free_cell(&mut self, cid: &CellId) {
        self.as_header().get_free_head().inspect(|head| {
            self[head].set_previous_sibling(Some(*cid));
            self[cid].set_next_sibling(Some(*head));
        });
        
        self.as_mut_header().set_free_head(Some(*cid));
        self.as_mut_header().inc_free_len();
    }

    /// Retire une cellule de la liste des cellules libres.
    fn pop_free_cell(&mut self) -> Option<CellId> {
        if let Some(head) = self.as_header().get_free_head() {
            let maybe_next = self.next_sibling(&head);
            
            maybe_next.inspect(|next| {
                self[next].set_next_sibling(None);
            });
            
            self.as_mut_header().set_free_head(maybe_next);
            self.as_mut_header().dec_free_len();

            return Some(head)
        }

        return None
    }

    /// Retire la cellule de la liste chaînée
    fn detach_cell(&mut self, cid: &CellId) {
        let maybe_prev = self.previous_sibling(cid);
        let maybe_next = self.next_sibling(cid);

        if Some(*cid) == self.head() {
            self.set_head(maybe_next);
        }

        if Some(*cid) == self.tail() {
            self.set_tail(maybe_prev);
        }

        self[cid].detach();

        maybe_prev.inspect(|prev| self[prev].set_next_sibling(maybe_next));
        maybe_next.inspect(|next| self[next].set_previous_sibling(maybe_prev));
    }  

    fn set_head(&mut self, head: Option<CellId>) {
        self.as_mut_header().set_head(head);
    }

    fn set_tail(&mut self, tail: Option<CellId>) {
        self.as_mut_header().set_tail(tail);
    }

    /// Récupère une référence mutable sur les propriétés de la page cellulaire
    fn as_mut_header(&mut self) -> &mut CellPageHeader {
        self.as_mut()
    }

    fn as_mut_uninit_header(&mut self) -> &mut MaybeUninit<CellPageHeader> {
        unsafe {
            std::mem::transmute(self.as_mut_header())
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug)]
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
    /// Tête de la liste des cellules allouées
    head_cell: OptionalCellId,
    /// Queue de la liste des cellules allouées
    tail_cell: OptionalCellId,
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
            tail_cell: None.into(),
            cell_base: base
        }
    }

    pub fn get_cell_location(&self, cid: &CellId) -> PageSize {
        let rank: u16 = ((*cid) - 1).into();
        self.cell_size * rank + self.cell_base
    }

    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        if (cid.0 - 1) >= self.capacity.0 {
            return None
        }

        let loc: usize = self.get_cell_location(cid).into();
        let size: usize = self.cell_size.into();

        Some(loc..(loc + size))
    }

    pub fn get_head(&self) -> Option<CellId> {
        self.head_cell.into()
    }

    pub fn get_tail(&self) -> Option<CellId> {
        self.tail_cell.into()
    }

    pub fn set_head(&mut self, head: Option<CellId>) {
        self.head_cell = head.into();
    }

    pub fn set_tail(&mut self, tail: Option<CellId>) {
        self.tail_cell = tail.into();
    }

    fn inc_len(&mut self) -> CellCapacity {
        self.len += 1;
        self.len
    }

    fn dec_free_len(&mut self) {
        self.free_len -= 1;
    }

    fn inc_free_len(&mut self) {
        self.free_len += 1;
    }

    /// Définit la nouvelle tête de la liste chaînée des cellules libres.
    fn set_free_head(&mut self, head: Option<CellId>) {
        self.free_head_cell = head.into();
    }

    /// Récupère la tête de la liste chaînée des cellules libres.
    fn get_free_head(&self) -> Option<CellId> {
        self.free_head_cell.into()
    }

    pub fn capacity(&self) -> CellCapacity {
        self.capacity
    }

    pub fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    pub fn len(&self) -> CellCapacity {
        self.len - self.free_len
    }
}


/// Une référence vers une cellule de page.
pub struct Cell<Slice>(Slice) where Slice: AsRefPageSlice + ?Sized;

impl<Slice> Cell<Slice> where Slice: AsRefPageSlice + IntoRefPageSlice {
    pub fn into_content_slice(self) -> Slice::RefPageSlice {
        let idx = size_of::<CellHeader>()..;
        self.0.into_page_slice(idx)
    }
}

impl<Slice, Idx> Index<Idx> for Cell<Slice> where Slice: AsRefPageSlice + ?Sized, Idx: std::slice::SliceIndex<[u8], Output = [u8]> {
    type Output = PageSlice;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.as_content_slice()[index]
    }
}

impl<Slice, Idx> IndexMut<Idx> for Cell<Slice> where Slice: AsMutPageSlice + ?Sized, Idx: std::slice::SliceIndex<[u8], Output = [u8]> {

    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        &mut self.as_mut_content_slice()[index]
    }
}



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
    fn as_slice(&self) -> &PageSlice {
        self.0.as_ref()
    }
}

impl<Slice> Cell<Slice> where Slice: AsMutPageSlice + ?Sized {
    fn as_mut_slice(&mut self) -> &mut PageSlice {
        self.0.as_mut()
    }
}

impl<Slice> Cell<Slice> where Slice: AsRefPageSlice + ?Sized {
    /// Retourne l'identifiant de la cellule.
    pub fn id(&self) -> CellId {
        self.as_header().id
    }

    pub fn as_content_slice(&self) -> &PageSlice {
        &self.as_slice()[size_of::<CellHeader>()..]
    }

    /// Copie le contenu de la cellule dans une autre cellule.
    pub fn copy_into<S2>(&self, dest: &mut Cell<S2>) where S2: AsMutPageSlice + ?Sized {
        dest.as_mut_content_slice().copy_from_slice(self.as_content_slice());
    }

    pub fn next_sibling(&self) -> Option<CellId> {
        self.as_header().next.into()
    }

    pub fn prev_sibling(&self) -> Option<CellId> {
        self.as_header().prev.into()
    }

    fn as_header(&self) -> &CellHeader {
        self.as_ref()
    }
}

impl<Slice> Cell<Slice> where Slice: AsMutPageSlice + ?Sized {
    /// Détache la cellule de sa liste chaînée.
    fn detach(&mut self) {
        let header = self.as_mut_header();
        header.set_next_sibling(None);
        header.set_previous_sibling(None);
    }

    /// Définit le prochain voisin de la cellule.
    fn set_next_sibling(&mut self, next: Option<CellId>) {
        self.as_mut_header().set_next_sibling(next);
    }

    /// Définit le voisin précédent de la cellule.
    fn set_previous_sibling(&mut self, prev: Option<CellId>) {
        self.as_mut_header().set_previous_sibling(prev);     
    }

    pub fn as_mut_content_slice(&mut self) -> &mut PageSlice {
        &mut self.as_mut_slice()[size_of::<CellHeader>()..]
    }

    fn as_mut_header(&mut self) -> &mut CellHeader {
        self.as_mut()
    }

    fn as_mut_uninit_header(&mut self) -> &mut MaybeUninit<CellHeader> {
        unsafe {
            std::mem::transmute(self.as_mut_header())
        }
    }
}

pub struct OwnedRefPageCellCursor<Page> where Page: IntoRefPageSlice + Clone + AsRefPageSlice
{
    cells: CellPage<Page>,
    current: Option<CellId>
}

impl<Page> Iterator for OwnedRefPageCellCursor<Page> where Page: IntoRefPageSlice + Clone + AsRefPageSlice
{
    type Item = Cell<Page::RefPageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.cells[&cid].next_sibling();
                let idx = self.cells.get_cell_range(&cid).unwrap();
                let slice = self.cells.0.clone().into_page_slice(idx);
                Some(Cell(slice))
            },
            None => None
        }
    }
}

pub struct RefPageCellCursor<'a, Page>
where Page: AsRefPageSlice
{
    cells: &'a CellPage<Page>,
    current: Option<CellId>
}

impl<'a, Page> Iterator for RefPageCellCursor<'a, Page> 
where Page: AsRefPageSlice
{
    type Item = &'a Cell<PageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.cells[&cid].next_sibling();
                Some(&self.cells[&cid])
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

impl CellHeader {
    pub fn get_previous_sibling(&self) -> Option<CellId> {
        self.prev.into()
    }
    
    pub fn get_next_sibling(&self) -> Option<CellId> {
        self.next.into()
    }

    pub fn set_previous_sibling(&mut self, prev: Option<CellId>) {
        self.prev = prev.into();
    }

    pub fn set_next_sibling(&mut self, next: Option<CellId>) {
        self.next = next.into()
    }
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

impl Display for CellId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq<CellCapacity> for CellId {
    fn eq(&self, other: &CellCapacity) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd<CellCapacity> for CellId {
    fn partial_cmp(&self, other: &CellCapacity) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Sub<u8> for CellId {
    type Output = u8;

    fn sub(self, rhs: u8) -> Self::Output {
        self.0 - rhs
    }
}

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

impl Display for CellCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{0}", self.0)
    }
}

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

impl Sub<CellCapacity> for CellCapacity {
    type Output = CellCapacity;

    fn sub(self, rhs: CellCapacity) -> Self {
        Self(self.0 - rhs.0)
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


#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct OptionalCellId(Option<NonZeroU8>);

impl Into<Option<CellId>> for OptionalCellId {
    fn into(self) -> Option<CellId> {
        self.0.map(|v| CellId(v.get()))
    }
}

impl From<Option<CellId>> for OptionalCellId {
    fn from(value: Option<CellId>) -> Self {
        match value {
            Some(cid) => {
                if cid == CellId(0) {
                    Self(None)
                } else {
                    Self(Some(cid.0.try_into().unwrap()))
                }
            },
            None => Self(None),
        }
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

    use itertools::Itertools;

    use crate::{pager::{cell::{CellCapacity, CellId, CellPage}, fixtures::fixture_new_pager, page::PageSize}, value::{IntoValueBuf, Value, GetValueKind}};
    use super::CellHeader;

    #[test]
    fn test_set_next_sibling() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();

        let mut cells = CellPage::new(
            pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?, 
            PageSize::from(10u16), 
            4_u8.into(), 
            0_usize.into()
        )?;

        let c1 = cells.alloc_cell()?;
        let c2 = cells.alloc_cell()?;

        assert!(cells[&c1].next_sibling().clone() == None);
        assert!(cells[&c1].prev_sibling().clone() == None);

        cells[&c1].set_next_sibling(Some(c2));
        assert_eq!(cells[&c1].next_sibling().clone(), Some(c2));

        Ok(())      
    }

    #[test]
    fn test_push() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();

        let mut cells = CellPage::new(
            pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?, 
            PageSize::from(10u16), 
            4_u8.into(), 
            0_usize.into()
        )?;

        let c1 = cells.push()?;
        let c2 = cells.push()?;
        let c3 = cells.push()?;
        let c4 = cells.push()?;
        
        assert_eq!(cells[&c1].next_sibling(), Some(c2));
        assert_eq!(cells[&c2].next_sibling(), Some(c3));
        assert_eq!(cells[&c3].next_sibling(), Some(c4));
        assert_eq!(cells[&c4].next_sibling(), None);

        assert_eq!(cells.iter().map(|cell| cell.id()).collect::<Vec<_>>(), vec![c1, c2, c3, c4]);

        Ok(())
    }

    #[test]
    fn test_free_cell() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();

        let mut cells = CellPage::new(
            pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?, 
            PageSize::from(10u16), 
            4_u8.into(), 
            0_usize.into()
        )?;


        let c1 = cells.push()?;
        let c2 = cells.push()?;
        let c3 = cells.push()?;
        assert_eq!(cells.iter().map(|cell| cell.id()).collect::<Vec<_>>(), vec![c1, c2, c3]);

        cells.free_cell(&c2);

        assert_eq!(cells[&c1].next_sibling(), Some(c3));
        assert_eq!(cells.iter().map(|cell| cell.id()).collect::<Vec<_>>(), vec![c1, c3]);
        assert_eq!(cells.len(), CellCapacity(2));

        Ok(())
    }

    #[test]
    fn test_content_size() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();
        let content_size = PageSize::from(u64::get_value_kind().full_size().unwrap());

        let mut src = CellPage::new(
            pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?, 
            content_size, 
            5_u8.into(), 
            0_usize.into()
        )?;
        
        let cid = src.push()?;
        
        assert_eq!(
            src.borrow_cell(&cid).unwrap().as_content_slice().len(), 
            content_size, 
            "la taille du contenu d'une cellule doit être celle définit initialement"
        );

        Ok(())
    }

    #[test]
    fn test_split_at() -> Result<(), Box<dyn Error>> {
        let pager = fixture_new_pager();
        let content_size = PageSize::from(u64::get_value_kind().full_size().unwrap());

        let mut src = CellPage::new(
            pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?, 
            content_size, 
            5_u8.into(), 
            0_usize.into()
        )?;

        let mut dest = CellPage::new(
            pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?, 
            content_size, 
            5_u8.into(), 
            0_usize.into()
        )?;
        
        for i in 0..5u64 {
            let cid = src.push().unwrap();
            src[&cid].as_mut_content_slice().clone_from_slice(i.into_value_buf().as_ref());
        }

        src.split_at_into(&mut dest, 3)?;
        
        let src_values = src.iter()
            .map::<&Value, _>(|cell| cell.as_content_slice().into())
            .map(|value| value.cast::<u64>().to_owned())
            .collect_vec();
        
        let dest_values = dest.iter()
            .map::<&Value, _>(|cell| cell.as_content_slice().into())
            .map(|value| value.cast::<u64>().to_owned())
            .collect_vec();
        
        assert_eq!(dest_values, vec![3u64, 4u64]);
        assert_eq!(dest.len(), CellCapacity(2));
        assert_eq!(src_values, vec![0u64, 1u64, 2u64]);
        assert_eq!(src.len(), CellCapacity(3));
        Ok(())
    }

    
    #[test]
    fn test_none_on_index_overflow() -> Result<(), Box<dyn Error>>  {
        let pager = fixture_new_pager();
        let page = pager.new_page().and_then(|pid| pager.borrow_mut_page(&pid))?;

        let mut cells = CellPage::new(page,             
            10_u16.into(), 
            4_u8.into(), 
            0_usize.into()
        )?;

        assert!(cells.borrow_cell(&CellId(3)).is_none());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&CellId(1)).is_some());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&CellId(2)).is_some());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&CellId(3)).is_some());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&CellId(4)).is_some());
        assert!(cells.borrow_cell(&CellId(10)).is_none());

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
            4_u8.into(), 
            0_usize.into()
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
            4_u8.into(), 
            0_usize.into()
        )?;

        for _ in 0..4 {
            cells.push().expect("on devrait pouvoir encore ajouter une cellule");
        }

        let result = cells.push();
        assert!(result.is_err(), "on ne devrait pas pouvoir ajouter une nouvelle cellule s'il n'y a plus d'espace.");

        Ok(())
    }
}