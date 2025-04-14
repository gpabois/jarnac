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
use std::{
    fmt::Debug,
    marker::PhantomData,
    mem::MaybeUninit,
    num::NonZeroU8,
    ops::{Index, IndexMut, Range},
};

use zerocopy::FromBytes;
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::page::{
    AsMutPageSlice, AsRefPageSlice, InPage, IntoRefPageSlice, MutPage, PageSize, PageSlice, RefPage,
};
use crate::prelude::*;
use crate::{
    error::{Error, ErrorKind},
    page::AsRefPage,
    result::Result,
    tag::{DataArea, JarTag},
};

pub type CellId = u8;
pub type CellCapacity = u8;

pub struct Cells;

impl Cells {
    /// Calcule la taille de la cellule (metadonnées inclus)
    pub fn compute_cell_size(content_size: u16) -> u16 {
        u16::try_from(size_of::<CellHeader>()).unwrap() + content_size
    }

    /// Calcule la taille allouée au contenu de la cellule (métadonnées exclus)
    pub fn compute_cell_content_size(cell_size: PageSize) -> PageSize {
        cell_size - u16::try_from(size_of::<CellHeader>()).unwrap()
    }

    /// Calcule la taille maximale des cellules compte tenu de l'espace dédiée et du nombre de cellules souhaité.
    pub fn compute_available_cell_content_size(
        size: PageSize,
        reserved: PageSize,
        k: CellCapacity,
    ) -> PageSize {
        let cell_size =
            Self::compute_available_cell_space_size(size, reserved).div_ceil(u16::from(k));
        Self::compute_cell_content_size(cell_size)
    }

    /// Vérifie que l'espace alloué aux cellules puisse contenir l'ensemble des cellules à la taille souhaitée
    pub fn within_available_cell_space_size(
        size: PageSize,
        reserved: PageSize,
        content_size: PageSize,
        k: CellCapacity,
    ) -> bool {
        let available = Self::compute_available_cell_space_size(size, reserved);
        Self::compute_available_cell_space_size(content_size, reserved) * u16::from(k) <= available
    }

    pub fn compute_cell_space_size(content_size: PageSize, k: CellCapacity) -> u16 {
        Self::compute_cell_size(content_size) * u16::from(k)
    }
    /// Calcule la taille maximale de l'espace dédiée aux cellules.
    ///
    /// reserved: espace réservée avant l'espace dédiée aux cellules.
    /// size: taille de la page
    pub fn compute_available_cell_space_size(size: PageSize, reserved: PageSize) -> PageSize {
        size - Self::compute_base(reserved)
    }

    /// Calcule la base de l'espace dédiée aux cellules
    ///
    /// reserved: espace réservée avant l'espace dédiée aux cellules.
    pub fn compute_base(reserved: PageSize) -> PageSize {
        u16::try_from(CellsMeta::INTEGRATED_AREA.end).unwrap() + reserved
    }
}

pub struct WithCells<T>(PhantomData<T>);

impl<T> DataArea for WithCells<T>
where
    T: DataArea,
{
    const AREA: Range<usize> =
        CellsMeta::AREA.end..(CellsMeta::INTEGRATED_AREA.end + size_of::<T>());
}

impl DataArea for CellsMeta {
    const AREA: Range<usize> = InPage::<Self>::AREA;
}

/// Sous-sytème permettant de découper une page en cellules de tailles égales
pub struct CellPage<Page>(Page);

impl<Page> AsRefPage for CellPage<Page>
where
    Page: AsRefPage,
{
    fn tag(&self) -> &JarTag {
        self.0.tag()
    }
}

impl<Page> AsRef<PageSlice> for CellPage<Page>
where
    Page: AsRefPageSlice,
{
    fn as_ref(&self) -> &PageSlice {
        self.0.as_ref()
    }
}

impl<Page> AsMut<PageSlice> for CellPage<Page>
where
    Page: AsMutPageSlice,
{
    fn as_mut(&mut self) -> &mut PageSlice {
        self.0.as_mut()
    }
}

pub const HEADER_SLICE_RANGE: Range<usize> = 1..(size_of::<CellsMeta>() + 1);

impl<'pager> CellPage<MutPage<'pager>> {
    pub fn tag(&self) -> &JarTag {
        self.0.tag()
    }

    pub fn into_ref(self) -> CellPage<RefPage<'pager>> {
        CellPage(self.0.into_ref())
    }
}

impl<Page> Clone for CellPage<Page>
where
    Page: AsRefPageSlice + Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Page> From<Page> for CellPage<Page>
where
    Page: AsRefPageSlice,
{
    fn from(value: Page) -> Self {
        Self(value)
    }
}
impl<Page> AsRef<Page> for CellPage<Page>
where
    Page: AsRefPageSlice,
{
    fn as_ref(&self) -> &Page {
        &self.0
    }
}

impl<Page> AsMut<Page> for CellPage<Page>
where
    Page: AsMutPageSlice,
{
    fn as_mut(&mut self) -> &mut Page {
        &mut self.0
    }
}

impl<Page> AsRef<CellsMeta> for CellPage<Page>
where
    Page: AsRefPageSlice,
{
    fn as_ref(&self) -> &CellsMeta {
        CellsMeta::ref_from_bytes(&self.0.as_ref()[HEADER_SLICE_RANGE]).unwrap()
    }
}

impl<Page> AsMut<CellsMeta> for CellPage<Page>
where
    Page: AsMutPageSlice,
{
    fn as_mut(&mut self) -> &mut CellsMeta {
        CellsMeta::mut_from_bytes(&mut self.0.as_mut()[HEADER_SLICE_RANGE]).unwrap()
    }
}

impl<Page> CellPage<Page>
where
    Page: IntoRefPageSlice + AsRefPageSlice,
{
    pub fn into_cell(self, cid: &CellId) -> Option<Cell<Page::RefPageSlice>> {
        let idx = self.get_cell_range(cid)?;
        let slice = self.0.into_page_slice(idx);
        let cell = Cell(slice);
        Some(cell)
    }
}

impl<Page> IntoIterator for CellPage<Page>
where
    Page: IntoRefPageSlice + Clone + AsRefPageSlice,
{
    type Item = Cell<Page::RefPageSlice>;

    type IntoIter = OwnedRefPageCellCursor<Page>;

    fn into_iter(self) -> Self::IntoIter {
        let current = self.head();
        let cells = self;
        OwnedRefPageCellCursor { cells, current }
    }
}

impl<Page> CellPage<Page>
where
    Page: AsRefPageSlice,
{
    /// Récupère la page après coups.
    pub fn into_inner(self) -> Page {
        self.0
    }

    /// Itère sur les références des cellules de la page
    pub fn iter(&self) -> RefPageCellCursor<'_, Page> {
        RefPageCellCursor {
            cells: self,
            current: self.head(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn iter_free(&self) -> RefPageCellCursor<'_, Page> {
        RefPageCellCursor {
            cells: self,
            current: self.free_head(),
        }
    }

    /// Emprunte une cellule en lecture seule
    pub fn borrow_cell<'a>(&'a self, cid: &CellId) -> Option<&'a Cell<PageSlice>> {
        let idx = self.get_cell_range(cid)?;
        let slice = self.0.borrow_page_slice(idx);

        unsafe { Some(std::mem::transmute::<&PageSlice, &Cell<PageSlice>>(slice)) }
    }

    /// Récupère un intervalle permettant de cibler une cellule.
    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        self.as_meta().get_cell_range(cid)
    }

    /// Retourne la prochaine cellule
    pub fn next_sibling(&self, cid: &CellId) -> Option<CellId> {
        self[cid].next_sibling()
    }

    /// Retourne la cellule précédente
    pub fn previous_sibling(&self, cid: &CellId) -> Option<CellId> {
        self[cid].prev_sibling()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> CellCapacity {
        self.as_meta().len()
    }

    pub fn capacity(&self) -> CellCapacity {
        self.as_meta().capacity()
    }

    pub fn is_full(&self) -> bool {
        self.as_meta().is_full()
    }

    fn head(&self) -> Option<CellId> {
        self.as_meta().get_head()
    }

    #[allow(dead_code)]
    fn free_head(&self) -> Option<CellId> {
        self.as_meta().get_free_head()
    }

    #[allow(dead_code)]
    pub(crate) fn free_len(&self) -> CellCapacity {
        self.as_meta().free_len()
    }

    fn tail(&self) -> Option<CellId> {
        self.as_meta().get_tail()
    }

    fn as_meta(&self) -> &CellsMeta {
        CellsMeta::ref_from_bytes(&self.0.as_ref()[CellsMeta::AREA]).unwrap()
    }
}

impl<Page> Index<&CellId> for CellPage<Page>
where
    Page: AsRefPageSlice,
{
    type Output = Cell<PageSlice>;

    fn index(&self, index: &CellId) -> &Self::Output {
        self.borrow_cell(index)
            .unwrap_or_else(|| panic!("the cell {index} does not exist"))
    }
}

impl<Page> IndexMut<&CellId> for CellPage<Page>
where
    Page: AsMutPageSlice,
{
    fn index_mut(&mut self, index: &CellId) -> &mut Self::Output {
        self.borrow_mut_cell(index).unwrap()
    }
}

impl<Page> CellPage<Page>
where
    Page: AsMutPageSlice,
{
    /// Initialise les éléments nécessaires pour découper la page en cellules.
    ///
    /// La fonction échoue si l'espace nécessaire pour stocker toutes les cellules excèdent la taille de l'espace libre
    /// allouée aux cellules.
    ///
    /// - content_size: The size of the cell content, it is used to compute the cell size (= size_of::<CellHeader> + content_size)
    /// - capacity: The maximum number of cells the page can hold
    /// - reserved: The number of reserved bytes in the header, it is used to compute the cell space base (= reserved + size_of::<CellPageHeader>())
    pub fn new(
        page: Page,
        content_size: PageSize,
        capacity: CellCapacity,
        reserved: PageSize,
    ) -> Result<Self> {
        let cell_size = Cells::compute_cell_size(content_size);
        let base = Cells::compute_base(reserved);

        Self::assert_no_overflow(page.as_ref().len(), cell_size, base, capacity)?;

        let mut cells = Self::from(page);

        cells
            .as_mut_uninit_header()
            .write(CellsMeta::new(cell_size, capacity, base));

        Ok(cells)
    }

    /// Itère sur les références des cellules de la page
    pub fn iter_mut(&mut self) -> MutPageCellCursor<'_, Page> {
        let current = self.as_meta().get_head();
        MutPageCellCursor {
            page: self,
            current,
        }
    }

    /// Emprunte une cellule en écriture
    pub fn borrow_mut_cell<'a>(&'a mut self, cid: &CellId) -> Option<&'a mut Cell<PageSlice>> {
        let idx = self.get_cell_range(cid)?;

        unsafe { std::mem::transmute(self.0.borrow_mut_page_slice(idx)) }
    }

    /// Divise les cellules à l'endroit choisi.
    pub fn split_at_into<P2>(&mut self, dest: &mut CellPage<P2>, at: u8) -> Result<()>
    where
        P2: AsMutPageSlice,
    {
        let mut to_free: Vec<CellId> = vec![];

        self.iter()
            .skip(usize::from(at))
            .try_for_each::<_, Result<()>>(|src_cell| {
                let cid = dest.push()?;
                src_cell.copy_into(&mut dest[&cid]);
                to_free.push(src_cell.id());
                Ok(())
            })?;

        to_free.into_iter().for_each(|cid| {
            self.free_cell(&cid);
        });

        Ok(())
    }

    /// Insère une nouvelle cellule à la fin de la liste chaînée.
    pub fn push(&mut self) -> Result<CellId> {
        let cid = self.alloc_cell()?;

        if let Some(tail) = &self.tail() {
            self.set_next_sibling(tail, &cid);
        } else {
            self.set_head(Some(cid));
            self.set_tail(Some(cid));
        }

        Ok(cid)
    }

    /// Insère une nouvelle cellule après une autre.
    pub fn insert_after(&mut self, after: &CellId) -> Result<CellId> {
        let cid = self.alloc_cell()?;
        self.set_next_sibling(after, &cid);
        Ok(cid)
    }

    /// Insère une nouvelle cellule avant une autre.
    pub fn insert_before(&mut self, before: &CellId) -> Result<CellId> {
        let cid = self.alloc_cell()?;
        self.set_previous_sibling(before, &cid);
        Ok(cid)
    }

    /// Alloue une nouvelle cellule au sein de la page, si on en a assez.
    fn alloc_cell(&mut self) -> Result<CellId>
    where
        Page: AsMutPageSlice,
    {
        if self.is_full() {
            return Err(Error::new(ErrorKind::CellPageFull));
        }

        let cid = self.pop_free_cell().unwrap_or_else(|| {
            let cid: CellId = self.as_mut_meta().inc_len();

            assert!(
                cid < self.capacity(),
                "allocated cell {cid} overflows capacity {0} ({1:?})",
                self.capacity(),
                self.as_meta()
            );
            cid
        });

        let cell = self
            .borrow_mut_cell(&cid)
            .unwrap_or_else(|| panic!("missing cell {cid}"));

        cell.as_mut_uninit_header().write(CellHeader {
            id: cid,
            next: None.into(),
            prev: None.into(),
        });

        Ok(cid)
    }

    /// Vérifie que la taille allouée aux cellules est contenue au sein de la page.
    fn assert_no_overflow(
        page_size: PageSize,
        cell_size: PageSize,
        base: PageSize,
        capacity: CellCapacity,
    ) -> Result<()> {
        let space_size = page_size - base;

        if space_size < cell_size * u16::from(capacity) {
            return Err(Error::new(ErrorKind::CellPageOverflow));
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

    pub fn free_cell(&mut self, cid: &CellId) {
        if self.iter().any(|cell| &cell.id() == cid) {
            self.detach_cell(cid);
            self.push_free_cell(cid);
        } else {
            panic!("trying to free cell {cid} which has not been allocated");
        }
    }

    /// Insère une nouvelle cellule dans la liste des cellules libres.
    fn push_free_cell(&mut self, cid: &CellId) {
        self.as_meta().get_free_head().inspect(|head| {
            self[head].set_previous_sibling(Some(*cid));
            self[cid].set_next_sibling(Some(*head));
        });

        self.as_mut_meta().set_free_head(Some(*cid));
        self.as_mut_meta().inc_free_len();
    }

    /// Retire une cellule de la liste des cellules libres.
    fn pop_free_cell(&mut self) -> Option<CellId> {
        if let Some(head) = self.as_meta().get_free_head() {
            let maybe_next = self.next_sibling(&head);

            maybe_next.inspect(|next| {
                self[next].set_previous_sibling(None);
            });

            self.as_mut_meta().set_free_head(maybe_next);
            self.as_mut_meta().dec_free_len();

            return Some(head);
        }

        None
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
        self.as_mut_meta().set_head(head);
    }

    fn set_tail(&mut self, tail: Option<CellId>) {
        self.as_mut_meta().set_tail(tail);
    }

    /// Récupère une référence mutable sur les propriétés de la page cellulaire
    fn as_mut_meta(&mut self) -> &mut CellsMeta {
        CellsMeta::mut_from_bytes(&mut self.0.as_mut()[CellsMeta::AREA]).unwrap()
    }

    fn as_mut_uninit_header(&mut self) -> &mut MaybeUninit<CellsMeta> {
        unsafe { std::mem::transmute(self.as_mut_meta()) }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête de la page contenant les informations relatives aux cellules qui y sont stockées.
pub struct CellsMeta {
    /// Nombre maximal de cellules stockables.
    capacity: CellCapacity,
    /// Taille d'une cellule
    cell_size: PageSize,
    /// Nombre de cellules alloués
    len: CellCapacity,
    /// Nombre de cellules libres
    free_len: CellCapacity,
    /// Tête de la liste des cellules allouées
    head_cell: OptionalCellId,
    /// Tête de la liste des cellules libérées
    free_head_cell: OptionalCellId,
    /// Queue de la liste des cellules allouées
    tail_cell: OptionalCellId,
    /// Localisation de la base des cellules
    cell_base: PageSize,
}

impl Debug for CellsMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cell_size = self.cell_size;
        let cell_base = self.cell_base;
        f.debug_struct("CellPageHeader")
            .field("capacity", &self.capacity)
            .field("cell_size", &cell_size)
            .field("len", &self.len)
            .field("free_len", &self.free_len)
            .field("free_head_cell", &self.free_head_cell)
            .field("head_cell", &self.head_cell)
            .field("tail_cell", &self.tail_cell)
            .field("cell_base", &cell_base)
            .finish()
    }
}

impl CellsMeta {
    pub fn new(cell_size: PageSize, capacity: CellCapacity, base: PageSize) -> Self {
        Self {
            cell_size,
            capacity,
            len: 0,
            free_len: 0,
            free_head_cell: None.into(),
            head_cell: None.into(),
            tail_cell: None.into(),
            cell_base: base,
        }
    }

    pub fn get_cell_location(&self, cid: &CellId) -> PageSize {
        self.cell_size * u16::from(*cid - 1) + self.cell_base
    }

    pub fn get_cell_range(&self, cid: &CellId) -> Option<Range<usize>> {
        if *cid <= self.capacity {
            let loc: usize = self.get_cell_location(cid).into();
            let size: usize = self.cell_size.into();

            return Some(loc..(loc + size));
        }

        None
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> CellCapacity {
        self.len - self.free_len
    }

    pub fn free_len(&self) -> CellCapacity {
        self.free_len
    }
}

/// Une référence vers une cellule de page.
pub struct Cell<Slice>(Slice)
where
    Slice: AsRefPageSlice + ?Sized;

impl<Slice> Cell<Slice>
where
    Slice: AsRefPageSlice + IntoRefPageSlice,
{
    pub fn into_content_slice(self) -> Slice::RefPageSlice {
        let idx = (size_of::<CellHeader>())..;
        self.0.into_page_slice(idx)
    }
}

impl<Slice, Idx> Index<Idx> for Cell<Slice>
where
    Slice: AsRefPageSlice + ?Sized,
    Idx: std::slice::SliceIndex<[u8], Output = [u8]>,
{
    type Output = PageSlice;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.as_content_slice()[index]
    }
}

impl<Slice, Idx> IndexMut<Idx> for Cell<Slice>
where
    Slice: AsMutPageSlice + ?Sized,
    Idx: std::slice::SliceIndex<[u8], Output = [u8]>,
{
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        &mut self.as_mut_content_slice()[index]
    }
}

impl<Slice> AsRef<CellHeader> for Cell<Slice>
where
    Slice: AsRefPageSlice + ?Sized,
{
    fn as_ref(&self) -> &CellHeader {
        CellHeader::ref_from_bytes(&self.0.as_ref()[0..size_of::<CellHeader>()]).unwrap()
    }
}

impl<Slice> AsMut<CellHeader> for Cell<Slice>
where
    Slice: AsMutPageSlice + ?Sized,
{
    fn as_mut(&mut self) -> &mut CellHeader {
        CellHeader::mut_from_bytes(&mut self.0.as_mut()[0..size_of::<CellHeader>()]).unwrap()
    }
}

impl<Slice> Cell<Slice>
where
    Slice: AsRefPageSlice + ?Sized,
{
    fn as_slice(&self) -> &PageSlice {
        self.0.as_ref()
    }
}

impl<Slice> Cell<Slice>
where
    Slice: AsMutPageSlice + ?Sized,
{
    fn as_mut_slice(&mut self) -> &mut PageSlice {
        self.0.as_mut()
    }
}

impl<Slice> Cell<Slice>
where
    Slice: AsRefPageSlice + ?Sized,
{
    /// Retourne l'identifiant de la cellule.
    pub fn id(&self) -> CellId {
        self.as_header().id
    }

    pub fn as_content_slice(&self) -> &PageSlice {
        &self.as_slice()[size_of::<CellHeader>()..]
    }

    /// Copie le contenu de la cellule dans une autre cellule.
    pub fn copy_into<S2>(&self, dest: &mut Cell<S2>)
    where
        S2: AsMutPageSlice + ?Sized,
    {
        dest.as_mut_content_slice()
            .copy_from_slice(self.as_content_slice());
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

impl<Slice> Cell<Slice>
where
    Slice: AsMutPageSlice + ?Sized,
{
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
        unsafe { std::mem::transmute(self.as_mut_header()) }
    }
}

pub struct OwnedRefPageCellCursor<Page>
where
    Page: IntoRefPageSlice + Clone + AsRefPageSlice,
{
    cells: CellPage<Page>,
    current: Option<CellId>,
}

impl<Page> Iterator for OwnedRefPageCellCursor<Page>
where
    Page: IntoRefPageSlice + Clone + AsRefPageSlice,
{
    type Item = Cell<Page::RefPageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.cells[&cid].next_sibling();
                let idx = self.cells.get_cell_range(&cid).unwrap();
                let slice = self.cells.0.clone().into_page_slice(idx);
                Some(Cell(slice))
            }
            None => None,
        }
    }
}

pub struct RefPageCellCursor<'a, Page>
where
    Page: AsRefPageSlice,
{
    cells: &'a CellPage<Page>,
    current: Option<CellId>,
}

impl<'a, Page> Iterator for RefPageCellCursor<'a, Page>
where
    Page: AsRefPageSlice,
{
    type Item = &'a Cell<PageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.cells[&cid].next_sibling();
                Some(&self.cells[&cid])
            }
            None => None,
        }
    }
}

pub struct MutPageCellCursor<'a, Page>
where
    Page: AsMutPageSlice,
{
    page: &'a mut CellPage<Page>,
    current: Option<CellId>,
}

impl<'a, Page> Iterator for MutPageCellCursor<'a, Page>
where
    Page: AsMutPageSlice,
{
    type Item = Cell<&'a mut PageSlice>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.page.next_sibling(&cid);
                unsafe {
                    std::mem::transmute::<
                        std::option::Option<&Cell<PageSlice>>,
                        std::option::Option<Cell<&mut PageSlice>>,
                    >(self.page.borrow_cell(&cid))
                }
            }
            None => None,
        }
    }
}

/// Un curseur sur les cellules d'une page.
pub struct PageCellIdCursor<'a, Page>
where
    Page: AsRefPageSlice,
{
    page: &'a CellPage<Page>,
    current: Option<CellId>,
}

impl<Page> Iterator for PageCellIdCursor<'_, Page>
where
    Page: AsRefPageSlice,
{
    type Item = CellId;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current {
            Some(cid) => {
                self.current = self.page.next_sibling(&cid);
                Some(cid)
            }
            None => None,
        }
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C, packed)]
/// En-tête d'une cellule.
pub struct CellHeader {
    id: CellId,
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

#[derive(FromBytes, IntoBytes, Immutable, KnownLayout, Clone, Copy, Default, PartialEq, Eq)]
pub struct OptionalCellId(Option<NonZeroU8>);

impl Debug for OptionalCellId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl From<OptionalCellId> for Option<CellId> {
    fn from(val: OptionalCellId) -> Self {
        val.0.map(|v| v.get())
    }
}

impl From<Option<CellId>> for OptionalCellId {
    fn from(value: Option<CellId>) -> Self {
        match value {
            Some(cid) => {
                if cid == 0 {
                    Self(None)
                } else {
                    Self(Some(cid.try_into().unwrap()))
                }
            }
            None => Self(None),
        }
    }
}

impl AsRef<Option<CellId>> for OptionalCellId {
    fn as_ref(&self) -> &Option<CellId> {
        unsafe { std::mem::transmute(self) }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use itertools::Itertools;

    use super::CellHeader;
    use crate::knack::kind::GetKnackKind;
    use crate::knack::Knack;
    use crate::{
        arena::IArena, cell::CellPage, page::PageSize, pager::stub::new_stub_pager,
        prelude::IntoKnackBuf,
    };

    #[test]
    fn test_set_next_sibling() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4_096>();

        let mut cells = CellPage::new(pager.new_element()?, 10, 4, 0)?;

        let c1 = cells.alloc_cell()?;
        let c2 = cells.alloc_cell()?;

        assert!(cells[&c1].next_sibling().is_none());
        assert!(cells[&c1].prev_sibling().is_none());

        cells[&c1].set_next_sibling(Some(c2));
        assert_eq!(cells[&c1].next_sibling().clone(), Some(c2));

        Ok(())
    }

    #[test]
    fn test_push() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4096>();

        let mut cells = CellPage::new(pager.new_element()?, 10, 4, 0)?;

        let c1 = cells.push()?;
        let c2 = cells.push()?;
        let c3 = cells.push()?;
        let c4 = cells.push()?;

        assert_eq!(cells[&c1].next_sibling(), Some(c2));
        assert_eq!(cells[&c2].next_sibling(), Some(c3));
        assert_eq!(cells[&c3].next_sibling(), Some(c4));
        assert_eq!(cells[&c4].next_sibling(), None);

        assert_eq!(
            cells.iter().map(|cell| cell.id()).collect::<Vec<_>>(),
            vec![c1, c2, c3, c4]
        );

        Ok(())
    }

    #[test]
    fn test_free_cell() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4096>();

        let mut cells = CellPage::new(pager.new_element()?, 10, 4_u8, 0)?;

        let c1 = cells.push()?;
        let c2 = cells.push()?;
        let c3 = cells.push()?;
        assert_eq!(
            cells.iter().map(|cell| cell.id()).collect::<Vec<_>>(),
            vec![c1, c2, c3]
        );

        cells.free_cell(&c2);

        assert_eq!(cells[&c1].next_sibling(), Some(c3));
        assert_eq!(
            cells.iter().map(|cell| cell.id()).collect::<Vec<_>>(),
            vec![c1, c3]
        );
        assert_eq!(cells.len(), 2);

        cells.free_cell(&c3);

        let c4 = cells.alloc_cell()?;
        assert_eq!(c4, c3);

        let c5 = cells.alloc_cell()?;
        assert_eq!(c5, c2);

        Ok(())
    }

    #[test]
    fn test_content_size() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4096>();
        let content_size = PageSize::try_from(u64::kind().outer_size()).unwrap();

        let mut src = CellPage::new(pager.new_element()?, content_size, 5, 0)?;

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
        let pager = new_stub_pager::<4096>();
        let content_size = PageSize::try_from(u64::kind().outer_size()).unwrap();

        let mut src = CellPage::new(pager.new_element()?, content_size, 5, 0)?;

        let mut dest = CellPage::new(pager.new_element()?, content_size, 5, 0)?;

        for i in 0..5u64 {
            let cid = src.push().unwrap();
            src[&cid]
                .as_mut_content_slice()
                .clone_from_slice(i.into_knack_buf().as_ref());
        }

        src.split_at_into(&mut dest, 3)?;

        let src_values = src
            .iter()
            .map::<&Knack, _>(|cell| cell.as_content_slice().into())
            .map(|value| value.cast::<u64>().to_owned())
            .collect_vec();

        let dest_values = dest
            .iter()
            .map::<&Knack, _>(|cell| cell.as_content_slice().into())
            .map(|value| value.cast::<u64>().to_owned())
            .collect_vec();

        assert_eq!(dest_values, vec![3u64, 4u64]);
        assert_eq!(dest.len(), 2);
        assert_eq!(src_values, vec![0u64, 1u64, 2u64]);
        assert_eq!(src.len(), 3);

        let got_free_len: u8 = src.iter_free().count().try_into().unwrap();
        assert_eq!(got_free_len, 2);
        assert_eq!(got_free_len, src.free_len());

        Ok(())
    }

    #[test]
    fn test_none_on_index_overflow() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4096>();

        let mut cells = CellPage::new(pager.new_element()?, 10_u16, 4, 0)?;

        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&1).is_some());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&2).is_some());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&3).is_some());
        cells.alloc_cell()?;
        assert!(cells.borrow_cell(&4).is_some());
        assert!(cells.borrow_cell(&10).is_none());

        Ok(())
    }

    #[test]
    fn test_fails_when_overflow() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4096>();
        let page = pager.new_element()?;

        let page_size = PageSize::try_from(pager.size_of()).unwrap();
        let base = PageSize::try_from(size_of::<CellHeader>()).unwrap();
        let available_cells_space_size = page_size - base;

        let result = CellPage::new(page, available_cells_space_size, 4, 0);

        assert!(result.is_err(), "on ne devrait pas pouvoir créer une page cellulaire dont l'espace requis excède l'espace disponible");

        Ok(())
    }

    #[test]
    fn test_fails_when_full() -> Result<(), Box<dyn Error>> {
        let pager = new_stub_pager::<4096>();

        let mut cells = CellPage::new(pager.new_element()?, PageSize::from(10u16), 4, 0)?;

        for _ in 0..4 {
            cells
                .push()
                .expect("on devrait pouvoir encore ajouter une cellule");
        }

        let result = cells.push();
        assert!(
            result.is_err(),
            "on ne devrait pas pouvoir ajouter une nouvelle cellule s'il n'y a plus d'espace."
        );

        Ok(())
    }
}

